
from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLCheckOperator,
)


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
# 8. Validate row counts, unique keys and non-null keys
#
# The DAG runs manually during development and testing.
# Final Snowflake tables are not modified by this version.
# ============================================================


with DAG(
    dag_id="retail_etl_pipeline",
    description="Transform retail data, upload to S3, stage and validate in Snowflake",
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
    # Read the original retail CSV, standardize column names,
    # convert data types and generate data-quality flags.
    #
    # cwd ensures relative dataset paths work inside Docker.
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
    # - customers.csv
    # - products.csv
    # - orders.csv
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
    # Upload all three CSV files to the retail/ prefix
    # in the project's S3 bucket.
    #
    # Boto3 uses the AWS credentials available to the worker.
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
    # Execute a read-only SQL query to confirm that Airflow
    # can connect to Snowflake using snowflake_default.
    # --------------------------------------------------------
    verify_snowflake = SQLExecuteQueryOperator(
        task_id="verify_snowflake",
        conn_id="snowflake_default",
        sql="""
            SELECT
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE()
        """,
    )

    # --------------------------------------------------------
    # TASK 5: STAGE CUSTOMERS
    #
    # Recreate a transient staging table matching CUSTOMERS.
    # Load customers.csv from the external S3 stage.
    #
    # The final CUSTOMERS table remains unchanged.
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
    #
    # The final PRODUCTS table remains unchanged.
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
    #
    # The final ORDERS table remains unchanged.
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
    # TASK 8: VALIDATE STAGING DATA
    #
    # Check all three staging tables for:
    # - Expected row counts
    # - Unique primary identifiers
    # - No missing primary identifiers
    #
    # SQLCheckOperator fails if any returned value is false.
    #
    # Expected counts are fixed for the current source CSV.
    # We will make these checks dynamic for incremental loads.
    # --------------------------------------------------------
    validate_staging = SQLCheckOperator(
        task_id="validate_staging",
        conn_id="snowflake_default",
        sql="""
            SELECT
                (
                    SELECT
                        COUNT(*) = 4372
                        AND COUNT(*) = COUNT(DISTINCT CUSTOMER_ID)
                        AND COUNT(*) = COUNT(CUSTOMER_ID)
                    FROM RETAIL_ETL_DB.RETAIL.CUSTOMERS_STAGING
                ) AS CUSTOMERS_VALID,

                (
                    SELECT
                        COUNT(*) = 4070
                        AND COUNT(*) = COUNT(DISTINCT STOCK_CODE)
                        AND COUNT(*) = COUNT(STOCK_CODE)
                    FROM RETAIL_ETL_DB.RETAIL.PRODUCTS_STAGING
                ) AS PRODUCTS_VALID,

                (
                    SELECT
                        COUNT(*) = 541909
                        AND COUNT(*) = COUNT(DISTINCT ORDER_LINE_ID)
                        AND COUNT(*) = COUNT(ORDER_LINE_ID)
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_STAGING
                ) AS ORDERS_VALID
        """,
    )

    # --------------------------------------------------------
    # TASK DEPENDENCIES
    #
    # Each task executes only after the previous task
    # completes successfully.
    #
    # A failed validation stops the DAG before publishing.
    # --------------------------------------------------------
    (
        transform_data
        >> split_into_tables
        >> upload_to_s3
        >> verify_snowflake
        >> stage_customers
        >> stage_products
        >> stage_orders
        >> validate_staging
    )
