"""
Consistency tests for lineage/lineage.json.

They fail if the lineage manifest references code, DAG tasks, datasets or
Snowflake columns that do not exist, so the lineage cannot drift silently
from the implementation. No Airflow installation is required.
"""

import json
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = json.loads((PROJECT_ROOT / "lineage" / "lineage.json").read_text(encoding="utf-8"))

DAG_FILES = {
    "retail_etl_pipeline": PROJECT_ROOT / "airflow/dags/retail_etl_dag.py",
    "incremental_s3_orders": PROJECT_ROOT / "airflow/dags/incremental_s3_orders_dag.py",
}


def _task_ids(dag_file: Path) -> set[str]:
    return set(re.findall(r'task_id="([^"]+)"', dag_file.read_text(encoding="utf-8")))


def _terraform_columns(table: str) -> set[str]:
    """Column names declared for a snowflake_table resource in tables.tf."""
    text = (PROJECT_ROOT / "terraform" / "tables.tf").read_text(encoding="utf-8")
    block = re.search(rf'resource "snowflake_table" "{table}" \{{(.*?)\n\}}', text, re.S)
    assert block, f"Terraform table '{table}' not found"
    return set(re.findall(r'name\s*=\s*"([A-Z_]+)"', block.group(1))) - {table.upper()}


def test_every_job_references_known_datasets():
    datasets = set(MANIFEST["datasets"])
    for job in MANIFEST["jobs"]:
        unknown = (set(job["inputs"]) | set(job["outputs"])) - datasets
        assert not unknown, f"{job['task']} references unknown datasets {unknown}"


def test_every_job_code_file_exists():
    for job in MANIFEST["jobs"]:
        assert (PROJECT_ROOT / job["code"]).is_file(), job["code"]


@pytest.mark.parametrize("dag_id", list(DAG_FILES))
def test_lineage_covers_exactly_the_dag_tasks(dag_id):
    documented = {job["task"] for job in MANIFEST["jobs"] if job["dag"] == dag_id}
    assert documented == _task_ids(DAG_FILES[dag_id])


def test_every_dataset_is_produced_or_is_a_source():
    produced = {name for job in MANIFEST["jobs"] for name in job["outputs"]}
    sources = {"raw_retail_csv", "retail_s3_stage"}  # external inputs, not produced by a job
    assert set(MANIFEST["datasets"]) - produced - sources == set()


def test_local_dataset_folders_exist():
    for dataset in MANIFEST["datasets"].values():
        if dataset["namespace"] == "file":
            assert (PROJECT_ROOT / dataset["name"]).parent.is_dir(), dataset["name"]


@pytest.mark.parametrize("table", ["orders", "customers", "products"])
def test_column_lineage_matches_terraform_schema(table):
    assert set(MANIFEST["column_lineage"][table]) == _terraform_columns(table)
