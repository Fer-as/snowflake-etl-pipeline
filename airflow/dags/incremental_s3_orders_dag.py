
"""
Incremental S3 Orders Pipeline.

Purpose:
    Process multiple immutable CSV batches from Amazon S3
    and insert previously unseen records into Snowflake.

Workflow:
    1. Upload batch 001
    2. Upload batch 002
    3. Create the incremental target if needed
    4. Validate staged batch records
    5. Merge unseen records into Snowflake
    6. Validate the target
    7. Replay the MERGE
    8. Validate idempotency

Record identity:
    SOURCE_FILE + SOURCE_ROW_NUMBER

Safety:
    - Never overwrites the incremental target.
    - Never modifies the main ORDERS table.
    - Never modifies the full-refresh DAG.
    - Supports repeated DAG executions.
"""

from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)


# ============================================================
# CONFIGURATION
# ============================================================

SNOWFLAKE_CONNECTION_ID = "snowflake_default"

TARGET_TABLE = (
    "RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_S3_AIRFLOW"
)

# Read every CSV matching orders_batch_###.csv from the
# incremental folder, rather than hardcoding one filename.
#
# Example files:
#     orders_batch_001.csv
#     orders_batch_002.csv
#     orders_batch_003.csv
#
# The pattern filters out unrelated objects.
STAGE_SOURCE = """
    @RETAIL_ETL_DB.RETAIL.RETAIL_S3_STAGE/incremental/
    (PATTERN => '.*orders_batch_[0-9]{3}[.]csv') t
"""


# ============================================================
# REUSABLE SOURCE QUERY
#
# Snowflake supplies the filename and file row number.
# Together, they form a stable identity for records
# inside immutable source files.
#
# The CSV header is excluded from processing.
# ============================================================

STAGED_RECORDS_SQL = f"""
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

    FROM {STAGE_SOURCE}

    WHERE t.$1 <> 'invoice_number'
"""


# ============================================================
# CREATE THE INCREMENTAL TARGET
#
# The table is intentionally persistent across DAG runs.
# DO NOT use CREATE OR REPLACE here.
# ============================================================

CREATE_TARGET_SQL = f"""
    CREATE TRANSIENT TABLE IF NOT EXISTS {TARGET_TABLE}
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
"""


# ============================================================
# VALIDATE THE INCOMING S3 BATCHES
#
# For our current test:
#     Batch 001 = 4 records
#     Batch 002 = 4 records
#
# Additional batches are allowed by the general row-count
# check. The two known test batches must still be present.
# ============================================================

VALIDATE_S3_BATCHES_SQL = f"""
    WITH staged AS (
        {STAGED_RECORDS_SQL}
    )

    SELECT
        COUNT(*) >= 8
            AS MINIMUM_EXPECTED_RECORDS,

        COUNT_IF(
            SOURCE_FILE LIKE '%/orders_batch_001.csv'
        ) = 4
            AS BATCH_001_HAS_FOUR_RECORDS,

        COUNT_IF(
            SOURCE_FILE LIKE '%/orders_batch_002.csv'
        ) = 4
            AS BATCH_002_HAS_FOUR_RECORDS,

        COUNT(*) = COUNT(
            DISTINCT
                SOURCE_FILE || ':' ||
                SOURCE_ROW_NUMBER::VARCHAR
        )
            AS UNIQUE_SOURCE_IDENTITIES,

        COUNT_IF(
            SOURCE_FILE IS NULL
            OR SOURCE_ROW_NUMBER IS NULL
            OR INVOICE_NUMBER IS NULL
            OR STOCK_CODE IS NULL
        ) = 0
            AS REQUIRED_FIELDS_PRESENT

    FROM staged
"""


# ============================================================
# MERGE INTO SNOWFLAKE
#
# Existing source identities are ignored.
# New source identities are inserted.
#
# This is insert-only incremental loading.
# Updating previously inserted records is not supported.
# ============================================================

MERGE_INCREMENTAL_SQL = f"""
    MERGE INTO {TARGET_TABLE} AS target

    USING (
        {STAGED_RECORDS_SQL}
    ) AS source

    ON target.SOURCE_FILE = source.SOURCE_FILE

    AND target.SOURCE_ROW_NUMBER =
        source.SOURCE_ROW_NUMBER

    WHEN NOT MATCHED THEN
        INSERT ALL BY NAME
"""


# ============================================================
# VALIDATE THE INCREMENTAL TARGET
#
# Checks:
#     - Target count matches staged record count
#     - At least eight records are loaded
#     - Target source identities are unique
#     - Every staged identity exists in the target
#     - No unexpected source identities exist in target
#
# The checks work after the initial MERGE and its replay.
# ============================================================

VALIDATE_TARGET_SQL = f"""
    WITH staged AS (
        {STAGED_RECORDS_SQL}
    )

    SELECT
        (
            SELECT COUNT(*)
            FROM {TARGET_TABLE}
        ) = (
            SELECT COUNT(*)
            FROM staged
        )
            AS SOURCE_AND_TARGET_COUNTS_MATCH,

        (
            SELECT COUNT(*)
            FROM {TARGET_TABLE}
        ) >= 8
            AS AT_LEAST_EIGHT_RECORDS_LOADED,

        (
            SELECT COUNT(*)
            FROM {TARGET_TABLE}
        ) = (
            SELECT COUNT(
                DISTINCT
                    SOURCE_FILE || ':' ||
                    SOURCE_ROW_NUMBER::VARCHAR
            )
            FROM {TARGET_TABLE}
        )
            AS TARGET_IDENTITIES_ARE_UNIQUE,

        NOT EXISTS (
            SELECT
                SOURCE_FILE,
                SOURCE_ROW_NUMBER
            FROM staged

            EXCEPT

            SELECT
                SOURCE_FILE,
                SOURCE_ROW_NUMBER
            FROM {TARGET_TABLE}
        )
            AS NO_MISSING_SOURCE_RECORDS,

        NOT EXISTS (
            SELECT
                SOURCE_FILE,
                SOURCE_ROW_NUMBER
            FROM {TARGET_TABLE}

            EXCEPT

            SELECT
                SOURCE_FILE,
                SOURCE_ROW_NUMBER
            FROM staged
        )
            AS NO_UNEXPECTED_TARGET_RECORDS
"""


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="incremental_s3_orders",
    description=(
        "Incrementally load multiple immutable S3 order "
        "batches into Snowflake"
    ),
    start_date=pendulum.datetime(
        2026,
        11,
        1,
        tz="UTC",
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=[
        "retail",
        "incremental",
        "s3",
        "snowflake",
    ],
) as dag:

    # --------------------------------------------------------
    # TASK 1: UPLOAD BATCH 001
    #
    # This file already exists. The uploader verifies its
    # contents without overwriting the existing S3 object.
    # --------------------------------------------------------

    upload_batch_001 = BashOperator(
        task_id="upload_batch_001",
        bash_command=(
            "python /opt/airflow/python_scripts/"
            "upload_incremental_batch.py --batch-id 001"
        ),
        cwd="/opt/airflow",
    )

    # --------------------------------------------------------
    # TASK 2: UPLOAD BATCH 002
    #
    # This batch contains four different source rows.
    # It is also protected against overwriting.
    # --------------------------------------------------------

    upload_batch_002 = BashOperator(
        task_id="upload_batch_002",
        bash_command=(
            "python /opt/airflow/python_scripts/"
            "upload_incremental_batch.py --batch-id 002"
        ),
        cwd="/opt/airflow",
    )

    # --------------------------------------------------------
    # TASK 3: CREATE THE TARGET IF IT DOES NOT EXIST
    #
    # Keep previously loaded records between DAG runs.
    # --------------------------------------------------------

    create_incremental_target = SQLExecuteQueryOperator(
        task_id="create_incremental_target",
        conn_id=SNOWFLAKE_CONNECTION_ID,
        sql=CREATE_TARGET_SQL,
    )

    # --------------------------------------------------------
    # TASK 4: VALIDATE THE STAGED BATCHES
    #
    # Both known CSVs must be readable and contain
    # four valid records each.
    # --------------------------------------------------------

    validate_s3_batches = SQLCheckOperator(
        task_id="validate_s3_batches",
        conn_id=SNOWFLAKE_CONNECTION_ID,
        sql=VALIDATE_S3_BATCHES_SQL,
    )

    # --------------------------------------------------------
    # TASK 5: MERGE NEW RECORDS
    #
    # Existing batch 001 rows remain unchanged.
    # Batch 002 rows are inserted if not already present.
    # --------------------------------------------------------

    merge_incremental_orders = SQLExecuteQueryOperator(
        task_id="merge_incremental_orders",
        conn_id=SNOWFLAKE_CONNECTION_ID,
        sql=MERGE_INCREMENTAL_SQL,
    )

    # --------------------------------------------------------
    # TASK 6: VALIDATE THE TARGET
    #
    # Expect eight records when only batches 001 and 002
    # exist in the incremental S3 folder.
    # --------------------------------------------------------

    validate_incremental_target = SQLCheckOperator(
        task_id="validate_incremental_target",
        conn_id=SNOWFLAKE_CONNECTION_ID,
        sql=VALIDATE_TARGET_SQL,
    )

    # --------------------------------------------------------
    # TASK 7: REPLAY THE MERGE
    #
    # The second MERGE should insert no additional rows.
    # --------------------------------------------------------

    replay_incremental_batches = SQLExecuteQueryOperator(
        task_id="replay_incremental_batches",
        conn_id=SNOWFLAKE_CONNECTION_ID,
        sql=MERGE_INCREMENTAL_SQL,
    )

    # --------------------------------------------------------
    # TASK 8: VALIDATE IDEMPOTENCY
    #
    # Ensure the replay did not introduce duplicates
    # or change the expected target record count.
    # --------------------------------------------------------

    validate_idempotency = SQLCheckOperator(
        task_id="validate_idempotency",
        conn_id=SNOWFLAKE_CONNECTION_ID,
        sql=VALIDATE_TARGET_SQL,
    )

    # --------------------------------------------------------
    # TASK DEPENDENCIES
    # --------------------------------------------------------

    (
        upload_batch_001
        >> upload_batch_002
        >> create_incremental_target
        >> validate_s3_batches
        >> merge_incremental_orders
        >> validate_incremental_target
        >> replay_incremental_batches
        >> validate_idempotency
    )
