"""
Connection health check (read-only).

Purpose:
    Verify both Airflow connections required by Task 6 of the challenge
    before running the ETL DAGs:

        aws_default        Amazon S3 + Amazon SNS (via the Amazon provider)
        snowflake_default  Snowflake warehouse/database/schema

Tasks:
    1. check_aws_identity     STS GetCallerIdentity through aws_default
    2. check_s3_objects       The bucket and the three full-refresh CSVs exist
    3. check_sns_topic        The alert topic exists and is reachable
    4. check_snowflake        Read-only session/context query

Safety:
    Every call is read-only. No S3 object, SNS message or Snowflake table
    is created, changed or deleted, so the DAG can be run at any time.
"""

import logging
import os
from datetime import timedelta

import pendulum

from airflow.sdk import DAG, task
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

from etl_sns_alerts import send_sns_failure_alert

LOGGER = logging.getLogger(__name__)

AWS_CONN_ID = os.environ.get("AWS_ALERTS_CONN_ID", "aws_default")
SNOWFLAKE_CONN_ID = "snowflake_default"
S3_BUCKET = os.environ.get("RETAIL_S3_BUCKET", "snowflake-retail-etl-670941257756")
S3_PREFIX = "retail"
EXPECTED_KEYS = ["customers.csv", "products.csv", "orders.csv"]


with DAG(
    dag_id="connection_healthcheck",
    description="Read-only check of the aws_default and snowflake_default connections",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": send_sns_failure_alert,
    },
    tags=["monitoring", "connections", "read-only"],
) as dag:

    @task
    def check_aws_identity() -> None:
        """Confirm aws_default can obtain credentials (STS)."""
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        sts = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="sts").get_conn()
        identity = sts.get_caller_identity()
        # Log only the principal name, not the full ARN/account ID.
        principal = identity["Arn"].rsplit("/", 1)[-1]
        LOGGER.info("aws_default authenticated as principal '%s'.", principal)

    @task
    def check_s3_objects() -> None:
        """Confirm the bucket and the full-refresh CSV objects exist."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        if not hook.check_for_bucket(S3_BUCKET):
            raise RuntimeError(f"S3 bucket '{S3_BUCKET}' is not accessible.")

        missing = [
            key for key in EXPECTED_KEYS
            if not hook.check_for_key(f"{S3_PREFIX}/{key}", bucket_name=S3_BUCKET)
        ]
        if missing:
            raise RuntimeError(f"Missing S3 objects under {S3_PREFIX}/: {missing}")
        LOGGER.info("S3 bucket and %d expected objects are present.", len(EXPECTED_KEYS))

    @task
    def check_sns_topic() -> None:
        """Confirm the alert topic is reachable without publishing."""
        from airflow.providers.amazon.aws.hooks.sns import SnsHook

        topic_arn = os.environ.get("AWS_SNS_TOPIC_ARN", "").strip()
        if not topic_arn:
            raise RuntimeError("AWS_SNS_TOPIC_ARN is not set in the container environment.")

        client = SnsHook(aws_conn_id=AWS_CONN_ID).get_conn()
        attributes = client.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
        LOGGER.info(
            "SNS topic reachable. Confirmed subscriptions: %s",
            attributes.get("SubscriptionsConfirmed", "unknown"),
        )

    # Fails unless every value is TRUE (SQLCheckOperator semantics).
    check_snowflake = SQLCheckOperator(
        task_id="check_snowflake",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT
                CURRENT_WAREHOUSE() IS NOT NULL AS HAS_WAREHOUSE,
                (
                    SELECT COUNT(*)
                    FROM RETAIL_ETL_DB.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'RETAIL'
                      AND TABLE_NAME IN ('CUSTOMERS', 'PRODUCTS', 'ORDERS')
                ) = 3 AS FINAL_TABLES_VISIBLE
        """,
    )

    check_aws_identity() >> [check_s3_objects(), check_sns_topic()]
    check_snowflake
