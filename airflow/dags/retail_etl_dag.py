
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ============================================================
# RETAIL ETL PIPELINE
#
# 1. Transform the original retail dataset
# 2. Split the transformed data into three CSV files
# 3. Upload the CSV files to Amazon S3
# 4. Verify the Airflow-to-Snowflake connection
# 5. Load customer data into a Snowflake staging table
#
# The DAG runs manually while we develop and test it.
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
    # Read the original CSV, clean column names and data
    # types, and generate data-quality flags.
    #
    # cwd ensures relative file paths resolve correctly.
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
    # TASK 2: SPLIT INTO TABLES
    #
    # Generate customers.csv, products.csv and orders.csv
    # from the transformed dataset.
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
    # TASK 3: UPLOAD TO S3
    #
    # Upload the three CSV files to the retail/ prefix in
    # the project's S3 bucket.
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
                CURRENT_WAREHOUSE();
        """,
    )

    # --------------------------------------------------------
    # TASK 5: STAGE CUSTOMER DATA
    #
    # Create a fresh transient staging table with the same
    # structure as the final CUSTOMERS table.
    #
    # Load customers.csv from the Snowflake external S3 stage.
    #
    # The existing CUSTOMERS table remains unchanged.
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
    # TASK DEPENDENCIES
    #
    # Each task runs only after the previous one succeeds.
    # --------------------------------------------------------
    (
        transform_data
        >> split_into_tables
        >> upload_to_s3
        >> verify_snowflake
        >> stage_customers
    )
