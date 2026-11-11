
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="retail_etl_pipeline",
    description="Transform retail data, upload CSVs to S3, and verify Snowflake",
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

    # Task 1: Transform the raw retail dataset.
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/python_scripts/transform_data_script.py",
        cwd="/opt/airflow",
    )

    # Task 2: Split the transformed data into three tables.
    split_into_tables = BashOperator(
        task_id="split_into_tables",
        bash_command="python /opt/airflow/python_scripts/split_data_into_tables.py",
        cwd="/opt/airflow",
    )

    # Task 3: Upload the three CSV files to Amazon S3.
    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command="python /opt/airflow/python_scripts/upload_data_to_s3_bucket.py",
        cwd="/opt/airflow",
    )

    # Task 4: Verify that Airflow can query Snowflake.
    # This is read-only and does not load or modify data.
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

    # Execute tasks in this order.
    transform_data >> split_into_tables >> upload_to_s3 >> verify_snowflake
