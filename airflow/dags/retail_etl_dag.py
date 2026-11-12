
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ============================================================
# RETAIL ETL PIPELINE
#
# 1. Transform the original retail dataset
# 2. Split the data into customers, products and orders
# 3. Upload the three CSV files to Amazon S3
# 4. Verify the Airflow-to-Snowflake connection
# 5. Load customers into a Snowflake staging table
# 6. Load products into a Snowflake staging table
# 7. Load orders into a Snowflake staging table
#
# Runs manually during development and testing.
# ============================================================

with DAG(
    dag_id="retail_etl_pipeline",
    description="Transform retail data, upload to S3, and stage data in Snowflake",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "etl", "snowflake"],
) as dag:

    # --------------------------------------------------------
    # TASK 1: TRANSFORM DATA
    #
    # Read the original retail CSV, standardize columns,
    # convert data types, and generate data-quality flags.
    # --------------------------------------------------------
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=(
            "python /opt/airflow/python_scripts/"
            "transform_data_script.py"
        ),
        cwd="/opt/airflow",
    )

    # --------------------------------------------------------
    # TASK 2: SPLIT DATA INTO TABLES
    #
    # Create three CSV files:
    # customers.csv, products.csv and orders.csv.
    # --------------------------------------------------------
    split_into_tables = BashOperator(
        task_id="split_into_tables",
        bash_command=(
            "python /opt/airflow/python_scripts/"
            "split_data_into_tables.py"
        ),
        cwd="/opt/airflow",
    )

    # --------------------------------------------------------
    # TASK 3: UPLOAD FILES TO AMAZON S3
    #
    # Upload the three CSV files to the retail/ folder
    # in the configured S3 bucket.
    # --------------------------------------------------------
    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=(
            "python /opt/airflow/python_scripts/"
            "upload_data_to_s3_bucket.py"
        ),
        cwd="/opt/airflow",
    )

    # --------------------------------------------------------
    # TASK 4: VERIFY SNOWFLAKE CONNECTION
    #
    # Run a read-only SQL query to confirm that Airflow
    # can connect using the snowflake_default connection.
    # --------------------------------------------------------
    verify_snowflake = SQLExecuteQueryOperator(
        task_id="verify_snowflake",
        conn_id="snowflake_default",
        sql="""
            SELECT
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE();
        """,
    )

    # --------------------------------------------------------
    # TASK 5: STAGE CUSTOMERS
    #
    # Recreate a transient staging table matching CUSTOMERS.
    # Load customers.csv from the external S3 stage.
    # The final CUSTOMERS table is not modified.
    # --------------------------------------------------------
    stage_customers = SQLExecuteQueryOperator(
        task_id="stage_customers",
        conn_id="snowflake_default",
        sql=[
            """
            CREATE OR REPLACE TRANSIENT TABLE
                RETAIL_ETL_DB.RETAIL.CUSTOMERS_STAGING
            LIKE RETAIL_ETL_DB.RETAIL.CUSTOMERS
            """,
            """
            COPY INTO RETAIL_ETL_DB.RETAIL.CUSTOMERS_STAGING
            FROM @RETAIL_ETL_DB.RETAIL.RETAIL_S3_STAGE
            FILES = ('customers.csv')
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('')
                EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = 'ABORT_STATEMENT'
            """,
        ],
    )

    # --------------------------------------------------------
    # TASK 6: STAGE PRODUCTS
    #
    # Recreate a transient staging table matching PRODUCTS.
    # Load products.csv from the external S3 stage.
    # The final PRODUCTS table is not modified.
    # --------------------------------------------------------
    stage_products = SQLExecuteQueryOperator(
        task_id="stage_products",
        conn_id="snowflake_default",
        sql=[
            """
            CREATE OR REPLACE TRANSIENT TABLE
                RETAIL_ETL_DB.RETAIL.PRODUCTS_STAGING
            LIKE RETAIL_ETL_DB.RETAIL.PRODUCTS
            """,
            """
            COPY INTO RETAIL_ETL_DB.RETAIL.PRODUCTS_STAGING
            FROM @RETAIL_ETL_DB.RETAIL.RETAIL_S3_STAGE
            FILES = ('products.csv')
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('')
                EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = 'ABORT_STATEMENT'
            """,
        ],
    )

    # --------------------------------------------------------
    # TASK 7: STAGE ORDERS
    #
    # Recreate a transient staging table matching ORDERS.
    # Load orders.csv from the external S3 stage.
    # The final ORDERS table is not modified.
    # --------------------------------------------------------
    stage_orders = SQLExecuteQueryOperator(
        task_id="stage_orders",
        conn_id="snowflake_default",
        sql=[
            """
            CREATE OR REPLACE TRANSIENT TABLE
                RETAIL_ETL_DB.RETAIL.ORDERS_STAGING
            LIKE RETAIL_ETL_DB.RETAIL.ORDERS
            """,
            """
            COPY INTO RETAIL_ETL_DB.RETAIL.ORDERS_STAGING
            FROM @RETAIL_ETL_DB.RETAIL.RETAIL_S3_STAGE
            FILES = ('orders.csv')
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('')
                EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = 'ABORT_STATEMENT'
            """,
        ],
    )

    # --------------------------------------------------------
    # TASK DEPENDENCIES
    #
    # Execute each task only after its predecessor succeeds.
    # --------------------------------------------------------
    (
        transform_data
        >> split_into_tables
        >> upload_to_s3
        >> verify_snowflake
        >> stage_customers
        >> stage_products
        >> stage_orders
    )
