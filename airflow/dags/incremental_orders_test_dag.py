
from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLCheckOperator,
)


# ============================================================
# INCREMENTAL ORDERS LOADING - AUTOMATED TEST
#
# 1. Prepare isolated test tables
# 2. Validate the incoming batch
# 3. MERGE new orders into the target
# 4. Validate the first MERGE
# 5. Repeat the same MERGE
# 6. Verify that no duplicate records were introduced
#
# This is an isolated test of insert-only incremental loading.
# It does NOT modify the production-style ORDERS table.
#
# The test tables are recreated on each DAG run.
# ============================================================


with DAG(
    dag_id="incremental_orders_test",
    description="Test incremental Snowflake MERGE and idempotency",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "snowflake", "incremental", "testing"],
) as dag:

    # --------------------------------------------------------
    # TASK 1: PREPARE TEST DATA
    #
    # Create two isolated transient tables:
    #
    # ORDERS_INCREMENTAL_AIRFLOW_TEST:
    #   Contains existing order IDs 1-10.
    #
    # ORDERS_DELTA_AIRFLOW_TEST:
    #   Contains incoming order IDs 9-12.
    #
    # IDs 9 and 10 are already present.
    # IDs 11 and 12 represent new records.
    #
    # The real ORDERS table is used only as a data source.
    # --------------------------------------------------------
    prepare_test_data = SQLExecuteQueryOperator(
        task_id="prepare_test_data",
        conn_id="snowflake_default",
        sql=[
            """
            CREATE OR REPLACE TRANSIENT TABLE
                RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST
            LIKE RETAIL_ETL_DB.RETAIL.ORDERS
            """,
            """
            INSERT INTO
                RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST
            SELECT *
            FROM RETAIL_ETL_DB.RETAIL.ORDERS
            WHERE ORDER_LINE_ID BETWEEN 1 AND 10
            """,
            """
            CREATE OR REPLACE TRANSIENT TABLE
                RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST
            LIKE RETAIL_ETL_DB.RETAIL.ORDERS
            """,
            """
            INSERT INTO
                RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST
            SELECT *
            FROM RETAIL_ETL_DB.RETAIL.ORDERS
            WHERE ORDER_LINE_ID BETWEEN 9 AND 12
            """,
        ],
    )

    # --------------------------------------------------------
    # TASK 2: VALIDATE INCOMING BATCH
    #
    # Check that:
    # - The target contains 10 existing orders
    # - The incoming batch contains 4 orders
    # - Exactly 2 incoming orders are new
    # - The incoming batch has unique order IDs
    #
    # A failed check stops the incremental workflow.
    # --------------------------------------------------------
    validate_incoming_batch = SQLCheckOperator(
        task_id="validate_incoming_batch",
        conn_id="snowflake_default",
        sql="""
            SELECT
                (
                    SELECT COUNT(*) = 10
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST
                ) AS TARGET_VALID,

                (
                    SELECT COUNT(*) = 4
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST
                ) AS BATCH_SIZE_VALID,

                (
                    SELECT COUNT(*) = 2
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST AS s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST AS t
                        WHERE t.ORDER_LINE_ID = s.ORDER_LINE_ID
                    )
                ) AS NEW_ORDERS_VALID,

                (
                    SELECT
                        COUNT(*) = COUNT(DISTINCT ORDER_LINE_ID)
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST
                ) AS UNIQUE_BATCH_KEYS
        """,
    )

    # --------------------------------------------------------
    # TASK 3: FIRST INCREMENTAL MERGE
    #
    # Insert orders that do not already exist.
    #
    # The expected result is:
    # - IDs 9 and 10 skipped
    # - IDs 11 and 12 inserted
    #
    # ALL BY NAME maps columns by their names.
    # --------------------------------------------------------
    merge_new_orders = SQLExecuteQueryOperator(
        task_id="merge_new_orders",
        conn_id="snowflake_default",
        sql="""
            MERGE INTO
                RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST AS target
            USING
                RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST AS source
            ON target.ORDER_LINE_ID = source.ORDER_LINE_ID

            WHEN NOT MATCHED THEN
                INSERT ALL BY NAME
        """,
    )

    # --------------------------------------------------------
    # TASK 4: VALIDATE FIRST MERGE
    #
    # Verify that:
    # - The target now contains 12 records
    # - All order IDs are unique
    # - The highest ID is 12
    # - No incoming order is still missing
    # --------------------------------------------------------
    validate_first_merge = SQLCheckOperator(
        task_id="validate_first_merge",
        conn_id="snowflake_default",
        sql="""
            SELECT
                (
                    SELECT COUNT(*) = 12
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST
                ) AS TOTAL_COUNT_VALID,

                (
                    SELECT
                        COUNT(*) = COUNT(DISTINCT ORDER_LINE_ID)
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST
                ) AS UNIQUE_KEYS_VALID,

                (
                    SELECT MAX(ORDER_LINE_ID) = 12
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST
                ) AS MAX_ID_VALID,

                (
                    SELECT COUNT(*) = 0
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST AS s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST AS t
                        WHERE t.ORDER_LINE_ID = s.ORDER_LINE_ID
                    )
                ) AS ALL_BATCH_ROWS_PRESENT
        """,
    )

    # --------------------------------------------------------
    # TASK 5: REPEAT THE SAME MERGE
    #
    # Process the identical batch a second time.
    #
    # Because IDs 9-12 now exist, no additional records
    # should be inserted.
    # --------------------------------------------------------
    repeat_merge = SQLExecuteQueryOperator(
        task_id="repeat_merge",
        conn_id="snowflake_default",
        sql="""
            MERGE INTO
                RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST AS target
            USING
                RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST AS source
            ON target.ORDER_LINE_ID = source.ORDER_LINE_ID

            WHEN NOT MATCHED THEN
                INSERT ALL BY NAME
        """,
    )

    # --------------------------------------------------------
    # TASK 6: VALIDATE IDEMPOTENCY
    #
    # Reprocessing the same batch must not create duplicates.
    #
    # Check that:
    # - Total rows remain 12
    # - All 12 order IDs remain unique
    # - No source records remain unmatched
    # --------------------------------------------------------
    validate_idempotency = SQLCheckOperator(
        task_id="validate_idempotency",
        conn_id="snowflake_default",
        sql="""
            SELECT
                (
                    SELECT COUNT(*) = 12
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST
                ) AS TOTAL_UNCHANGED,

                (
                    SELECT
                        COUNT(DISTINCT ORDER_LINE_ID) = 12
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST
                ) AS UNIQUE_IDS_UNCHANGED,

                (
                    SELECT COUNT(*) = 0
                    FROM RETAIL_ETL_DB.RETAIL.ORDERS_DELTA_AIRFLOW_TEST AS s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM RETAIL_ETL_DB.RETAIL.ORDERS_INCREMENTAL_AIRFLOW_TEST AS t
                        WHERE t.ORDER_LINE_ID = s.ORDER_LINE_ID
                    )
                ) AS NO_UNPROCESSED_ORDERS
        """,
    )

    # --------------------------------------------------------
    # TASK DEPENDENCIES
    #
    # The workflow runs the MERGE twice and verifies that
    # processing the same batch again changes nothing.
    # --------------------------------------------------------
    (
        prepare_test_data
        >> validate_incoming_batch
        >> merge_new_orders
        >> validate_first_merge
        >> repeat_merge
        >> validate_idempotency
    )
