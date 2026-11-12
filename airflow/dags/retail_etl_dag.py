
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
# 8. Validate staging data quality
# 9. Publish validated data to the final Snowflake tables
# 10. Validate the published data
#
# Loading strategy: Full refresh
# Schedule: Manual during development
#
# The staging validation must pass before publishing.
# INSERT OVERWRITE prevents duplicate rows on repeated runs.
# ============================================================


with DAG(
    dag_id="retail_etl_pipeline",
    description="Transform retail data, load Snowflake and validate results",
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
    # Read the original CSV, standardize columns, convert
    # data types and generate data-quality flags.
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
    # Generate customers.csv, products.csv and orders.csv.
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
    # TASK 3: UPLOAD TO AMAZON S3
    #
    # Upload the three CSV files to the retail/ S3 prefix.
    # Credentials are supplied through the worker environment.
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
    # TASK 4: VERIFY SNOWFLAKE
    #
    # Confirm that Airflow can execute SQL in Snowflake.
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
    # Recreate an empty staging table matching CUSTOMERS.
    # Load customers.csv without changing the final table.
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
    # Recreate an empty staging table matching PRODUCTS.
    # Load products.csv without changing the final table.
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
    # Recreate an empty staging table matching ORDERS.
    # Load orders.csv without changing the final table.
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
    # Verify expected row counts and unique, non-null keys.
    # A failed check prevents the publishing task from running.
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
    # TASK 9: PUBLISH VALIDATED DATA
    #
    # Replace the rows in all three final tables.
    #
    # INSERT OVERWRITE preserves the existing table objects,
    # schemas and grants.
    #
    # BEGIN/COMMIT group the three changes in one transaction.
    # If an operation fails before COMMIT, the transaction
    # should be rolled back rather than partially published.
    # --------------------------------------------------------
    publish_to_snowflake = SQLExecuteQueryOperator(
        task_id="publish_to_snowflake",
        conn_id="snowflake_default",
        autocommit=False,
        sql=[
            """
            BEGIN TRANSACTION
            """,
            """
            INSERT OVERWRITE INTO RETAIL_ETL_DB.RETAIL.CUSTOMERS
                (CUSTOMER_ID, PRIMARY_COUNTRY)
            SELECT
                CUSTOMER_ID,
                PRIMARY_COUNTRY
            FROM RETAIL_ETL_DB.RETAIL.CUSTOMERS_STAGING
            """,
            """
            INSERT OVERWRITE INTO RETAIL_ETL_DB.RETAIL.PRODUCTS
                (STOCK_CODE, DESCRIPTION)
            SELECT
                STOCK_CODE,
                DESCRIPTION
            FROM RETAIL_ETL_DB.RETAIL.PRODUCTS_STAGING
            """,
            """
            INSERT OVERWRITE INTO RETAIL_ETL_DB.RETAIL.ORDERS
                (
                    ORDER_LINE_ID,
                    INVOICE_NUMBER,
                    STOCK_CODE,
                    CUSTOMER_ID,
                    INVOICE_DATE,
                    QUANTITY,
                    UNIT_PRICE,
                    IS_CANCELLATION,
                    IS_OTHER_NEGATIVE_ADJUSTMENT,
                    IS_NONPOSITIVE_PRICE,
                    IS_REGULAR_SALE,
                    IS_MISSING_CUSTOMER
                )
            SELECT
                ORDER_LINE_ID,
                INVOICE_NUMBER,
                STOCK_CODE,
                CUSTOMER_ID,
                INVOICE_DATE,
                QUANTITY,
                UNIT_PRICE,
                IS_CANCELLATION,
                IS_OTHER_NEGATIVE_ADJUSTMENT,
                IS_NONPOSITIVE_PRICE,
                IS_REGULAR_SALE,
                IS_MISSING_CUSTOMER
            FROM RETAIL_ETL_DB.RETAIL.ORDERS_STAGING
            """,
            """
            COMMIT
            """,
        ],
    )

    # --------------------------------------------------------
    # TASK 10: VALIDATE PUBLISHED DATA
    #
    # Verify that the final tables contain the expected
    # rows with unique, non-null primary identifiers.
    #
    # This catches publishing errors after the transaction.
    # --------------------------------------------------------
    validate_published_data = SQLCheckOperator(
        task_id="validate_published_data",
        conn_id="snowflake_default",
        sql="""
            SELECT
                (
                    SELECT
                        COUNT(*) = 4372
                        AND COUNT(*) = COUNT(DISTINCT CUSTOMER_ID)
                        AND COUNT(*) = COUNT(CUSTOMER_ID)
                    FROM RETAIL_ETL_DB.RETAIL.CUSTOMERS
                ) AS CUSTOMERS_VALID,

                (
                    SELECT
                        COUNT(*) = 4070
                        AND COUNT(*) = COUNT(DISTINCT STOCK_CODE)
                        AND COUNT(*) = COUNT(STOCK_CODE)
                    FROM RETAIL_ETL_DB.RETAIL.PRODUCTS
                ) AS PRODUCTS_VALID,

                (
                    SELECT
                        COUNT(*) = 541909
                        AND COUNT(*) = COUNT(DISTINCT ORDER_LINE_ID)
                        AND COUNT(*) = COUNT(ORDER_LINE_ID)
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS
                ) AS ORDERS_VALID
        """,
    )

    # --------------------------------------------------------
    # TASK DEPENDENCIES
    #
    # Tasks execute in order. Publishing happens only after
    # all three staging tables pass validation.
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
        >> publish_to_snowflake
        >> validate_published_data
    )
