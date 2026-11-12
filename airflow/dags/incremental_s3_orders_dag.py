
from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)


# ============================================================
# INCREMENTAL S3 ORDERS PIPELINE
#
# 1. Prepare and upload an immutable CSV batch
# 2. Create a persistent incremental Snowflake target
# 3. Validate the incoming S3 records
# 4. MERGE previously unseen records into the target
# 5. Validate the loaded records
# 6. Replay the same MERGE
# 7. Verify idempotency
#
# Record identity:
#   SOURCE_FILE + SOURCE_ROW_NUMBER
#
# Loading strategy: Insert-only incremental loading
# Schedule: Manual during development
#
# Safety:
#   - Does not modify the main ORDERS table
#   - Does not modify the full-refresh DAG
#   - Does not recreate the incremental target each run
# ============================================================


# ------------------------------------------------------------
# SHARED SNOWFLAKE SQL
#
# Reuse exactly the same MERGE statement for the initial
# load and the idempotency test.
# ------------------------------------------------------------

MERGE_INCREMENTAL_SQL = """
    MERGE INTO
        RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_S3_AIRFLOW
        AS target

    USING (
        SELECT
            METADATA$FILENAME AS SOURCE_FILE,
            METADATA$FILE_ROW_NUMBER AS SOURCE_ROW_NUMBER,

            t.$1::VARCHAR AS INVOICE_NUMBER,
            t.$2::VARCHAR AS STOCK_CODE,
            TRY_TO_NUMBER(t.$3) AS CUSTOMER_ID,
            t.$4::TIMESTAMP_NTZ AS INVOICE_DATE,
            t.$5::NUMBER(38, 0) AS QUANTITY,
            t.$6::NUMBER(18, 4) AS UNIT_PRICE,
            t.$7::BOOLEAN AS IS_CANCELLATION,
            t.$8::BOOLEAN AS IS_OTHER_NEGATIVE_ADJUSTMENT,
            t.$9::BOOLEAN AS IS_NONPOSITIVE_PRICE,
            t.$10::BOOLEAN AS IS_REGULAR_SALE,
            t.$11::BOOLEAN AS IS_MISSING_CUSTOMER

        FROM
            @RETAIL_ETL_DB.RETAIL.RETAIL_S3_STAGE/incremental/orders_batch_001.csv t

        WHERE t.$1 <> 'invoice_number'
    ) AS source

    ON target.SOURCE_FILE = source.SOURCE_FILE
    AND target.SOURCE_ROW_NUMBER = source.SOURCE_ROW_NUMBER

    WHEN NOT MATCHED THEN
        INSERT ALL BY NAME
"""


# ------------------------------------------------------------
# SHARED VALIDATION SQL
#
# The target must contain exactly four unique source records
# from the demonstration batch.
#
# This check is reused after both MERGE executions.
# ------------------------------------------------------------

VALIDATE_TARGET_SQL = """
    SELECT
        (
            SELECT COUNT(*) = 4
            FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_S3_AIRFLOW
        ) AS EXPECTED_ROW_COUNT,

        (
            SELECT
                COUNT(*) = COUNT(
                    DISTINCT SOURCE_FILE || ':' ||
                    SOURCE_ROW_NUMBER::VARCHAR
                )
            FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_S3_AIRFLOW
        ) AS UNIQUE_SOURCE_IDENTIFIERS,

        (
            SELECT COUNT(*) = 0
            FROM (
                SELECT
                    METADATA$FILENAME AS SOURCE_FILE,
                    METADATA$FILE_ROW_NUMBER AS SOURCE_ROW_NUMBER

                FROM
                    @RETAIL_ETL_DB.RETAIL.RETAIL_S3_STAGE/incremental/orders_batch_001.csv t

                WHERE t.$1 <> 'invoice_number'

                EXCEPT

                SELECT
                    SOURCE_FILE,
                    SOURCE_ROW_NUMBER

                FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_S3_AIRFLOW
            )
        ) AS ALL_SOURCE_ROWS_LOADED
"""


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="incremental_s3_orders",
    description="Load immutable S3 order batches using Snowflake MERGE",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "incremental", "s3", "snowflake"],
) as dag:

    # --------------------------------------------------------
    # TASK 1: UPLOAD INCREMENTAL BATCH
    #
    # Run the Python script that prepares the four-row batch.
    #
    # The uploader:
    # - Preserves the original retail CSV
    # - Does not overwrite existing S3 objects
    # - Allows identical batches to be retried safely
    # --------------------------------------------------------
    upload_incremental_batch = BashOperator(
        task_id="upload_incremental_batch",
        bash_command=(
            "python /opt/airflow/python_scripts/"
            "upload_incremental_batch.py"
        ),
        cwd="/opt/airflow",
    )

    # --------------------------------------------------------
    # TASK 2: CREATE INCREMENTAL TARGET
    #
    # Create a separate transient Snowflake target table.
    #
    # IF NOT EXISTS preserves previously processed records
    # when the DAG is triggered again.
    #
    # This table is separate from the earlier manual test.
    # --------------------------------------------------------
    create_incremental_target = SQLExecuteQueryOperator(
        task_id="create_incremental_target",
        conn_id="snowflake_default",
        sql="""
            CREATE TRANSIENT TABLE IF NOT EXISTS
                RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_S3_AIRFLOW
            (
                SOURCE_FILE VARCHAR NOT NULL,
                SOURCE_ROW_NUMBER NUMBER(38, 0) NOT NULL,
                INVOICE_NUMBER VARCHAR,
                STOCK_CODE VARCHAR,
                CUSTOMER_ID NUMBER(38, 0),
                INVOICE_DATE TIMESTAMP_NTZ,
                QUANTITY NUMBER(38, 0),
                UNIT_PRICE NUMBER(18, 4),
                IS_CANCELLATION BOOLEAN,
                IS_OTHER_NEGATIVE_ADJUSTMENT BOOLEAN,
                IS_NONPOSITIVE_PRICE BOOLEAN,
                IS_REGULAR_SALE BOOLEAN,
                IS_MISSING_CUSTOMER BOOLEAN
            )
        """,
    )

    # --------------------------------------------------------
    # TASK 3: VALIDATE THE S3 BATCH
    #
    # Confirm that Snowflake can read the staged CSV.
    #
    # Validate:
    # - Exactly four data rows
    # - Four unique file-and-row identities
    #
    # The first CSV line is a header, not a data record.
    # --------------------------------------------------------
    validate_s3_batch = SQLCheckOperator(
        task_id="validate_s3_batch",
        conn_id="snowflake_default",
        sql="""
            SELECT
                COUNT(*) = 4 AS EXPECTED_BATCH_SIZE,

                COUNT(*) = COUNT(
                    DISTINCT METADATA$FILENAME || ':' ||
                    METADATA$FILE_ROW_NUMBER::VARCHAR
                ) AS UNIQUE_SOURCE_ROWS

            FROM
                @RETAIL_ETL_DB.RETAIL.RETAIL_S3_STAGE/incremental/orders_batch_001.csv t

            WHERE t.$1 <> 'invoice_number'
        """,
    )

    # --------------------------------------------------------
    # TASK 4: MERGE INCREMENTAL ORDERS
    #
    # Match records using:
    # - Source filename
    # - Source row number
    #
    # Insert only records that have not been loaded before.
    # --------------------------------------------------------
    merge_incremental_orders = SQLExecuteQueryOperator(
        task_id="merge_incremental_orders",
        conn_id="snowflake_default",
        sql=MERGE_INCREMENTAL_SQL,
    )

    # --------------------------------------------------------
    # TASK 5: VALIDATE INCREMENTAL TARGET
    #
    # Check the target after the first MERGE.
    #
    # Expected:
    # - Four records
    # - Four unique source identifiers
    # - No missing source records
    # --------------------------------------------------------
    validate_incremental_target = SQLCheckOperator(
        task_id="validate_incremental_target",
        conn_id="snowflake_default",
        sql=VALIDATE_TARGET_SQL,
    )

    # --------------------------------------------------------
    # TASK 6: REPLAY THE INCREMENTAL BATCH
    #
    # Re-run the identical MERGE statement.
    #
    # All four source records should already exist.
    # Therefore, no additional rows should be inserted.
    # --------------------------------------------------------
    replay_incremental_batch = SQLExecuteQueryOperator(
        task_id="replay_incremental_batch",
        conn_id="snowflake_default",
        sql=MERGE_INCREMENTAL_SQL,
    )

    # --------------------------------------------------------
    # TASK 7: VALIDATE IDEMPOTENCY
    #
    # Verify that replaying the same batch did not change
    # the record count or create duplicate source identities.
    # --------------------------------------------------------
    validate_idempotency = SQLCheckOperator(
        task_id="validate_idempotency",
        conn_id="snowflake_default",
        sql=VALIDATE_TARGET_SQL,
    )

    # --------------------------------------------------------
    # TASK DEPENDENCIES
    #
    # Execute the pipeline in a controlled sequence.
    # A failed validation stops the downstream tasks.
    # --------------------------------------------------------
    (
        upload_incremental_batch
        >> create_incremental_target
        >> validate_s3_batch
        >> merge_incremental_orders
        >> validate_incremental_target
        >> replay_incremental_batch
        >> validate_idempotency
    )
