
from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLCheckOperator,
)


# ============================================================
# SNOWFLAKE RETAIL ETL PIPELINE
#
#  1. Transform the original retail dataset
#  2. Split data into customers, products and orders
#  3. Upload the three CSV files to Amazon S3
#  4. Verify the Airflow-to-Snowflake connection
#  5. Load customers into a Snowflake staging table
#  6. Load products into a Snowflake staging table
#  7. Load orders into a Snowflake staging table
#  8. Validate staging data quality
#  9. Publish validated data to the final tables
# 10. Validate published row counts and primary keys
# 11. Verify final tables match staging in both directions
#
# Loading strategy: Full refresh
# Schedule: Manual during development
#
# Data is validated before and after publishing.
# ============================================================


with DAG(
    dag_id="retail_etl_pipeline",
    description="End-to-end retail ETL with Snowflake validation",
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
    #
    # The working directory makes relative file paths work.
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
    # Generate:
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
    # TASK 3: UPLOAD TO AMAZON S3
    #
    # Upload the CSV files to the retail/ S3 prefix.
    # AWS authentication comes from the worker environment.
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
    # Confirm that Airflow can execute a read-only query
    # through the snowflake_default connection.
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
    # Recreate a transient table matching CUSTOMERS.
    # Load customers.csv from the external S3 stage.
    #
    # The final CUSTOMERS table is not modified here.
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
    # Recreate a transient table matching PRODUCTS.
    # Load products.csv from the external S3 stage.
    #
    # The final PRODUCTS table is not modified here.
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
    # Recreate a transient table matching ORDERS.
    # Load orders.csv from the external S3 stage.
    #
    # The final ORDERS table is not modified here.
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
    # Verify:
    # - Expected row counts
    # - Unique identifiers
    # - Non-null identifiers
    #
    # If any check fails, publishing will not run.
    #
    # Counts are fixed for the current retail dataset.
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
    # Use INSERT OVERWRITE to replace final-table rows
    # instead of appending potentially duplicate records.
    #
    # The three updates are grouped in one transaction.
    # No DDL statements are used within this transaction.
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
    # Verify the final tables have:
    # - Expected row counts
    # - Unique identifiers
    # - Non-null identifiers
    #
    # This validates the published dataset's basic quality.
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
    # TASK 11: VALIDATE FINAL TABLE CONSISTENCY
    #
    # Compare each final table with its staging table in
    # both directions using EXCEPT.
    #
    # Final-only:
    # Records in final that do not exist in staging.
    #
    # Staging-only:
    # Records in staging that do not exist in final.
    #
    # NOT EXISTS returns TRUE when no differences exist.
    #
    # SQLCheckOperator fails if any of the six checks is
    # FALSE, making data consistency part of every DAG run.
    # --------------------------------------------------------
    validate_final_consistency = SQLCheckOperator(
        task_id="validate_final_consistency",
        conn_id="snowflake_default",
        sql="""
            SELECT
                -- CUSTOMERS: final minus staging
                NOT EXISTS (
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.CUSTOMERS
                    EXCEPT
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.CUSTOMERS_STAGING
                ) AS CUSTOMERS_FINAL_MATCH,

                -- CUSTOMERS: staging minus final
                NOT EXISTS (
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.CUSTOMERS_STAGING
                    EXCEPT
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.CUSTOMERS
                ) AS CUSTOMERS_STAGING_MATCH,

                -- PRODUCTS: final minus staging
                NOT EXISTS (
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.PRODUCTS
                    EXCEPT
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.PRODUCTS_STAGING
                ) AS PRODUCTS_FINAL_MATCH,

                -- PRODUCTS: staging minus final
                NOT EXISTS (
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.PRODUCTS_STAGING
                    EXCEPT
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.PRODUCTS
                ) AS PRODUCTS_STAGING_MATCH,

                -- ORDERS: final minus staging
                NOT EXISTS (
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS
                    EXCEPT
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_STAGING
                ) AS ORDERS_FINAL_MATCH,

                -- ORDERS: staging minus final
                NOT EXISTS (
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_STAGING
                    EXCEPT
                    SELECT *
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS
                ) AS ORDERS_STAGING_MATCH
        """,
    )

    # --------------------------------------------------------
    # TASK DEPENDENCIES
    #
    # Each task starts only after its predecessor succeeds.
    #
    # The final consistency check runs after publishing
    # and after the published data-quality validation.
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
        >> validate_final_consistency
    )
