
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="retail_etl_pipeline",
    description="Transform retail data, split tables, and upload CSVs to S3",
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

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/python_scripts/transform_data_script.py",
        cwd="/opt/airflow",
    )

    split_into_tables = BashOperator(
        task_id="split_into_tables",
        bash_command="python /opt/airflow/python_scripts/split_data_into_tables.py",
        cwd="/opt/airflow",
    )

    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command="python /opt/airflow/python_scripts/upload_data_to_s3_bucket.py",
        cwd="/opt/airflow",
    )

    transform_data >> split_into_tables >> upload_to_s3
