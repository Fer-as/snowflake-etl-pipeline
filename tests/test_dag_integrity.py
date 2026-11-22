"""
Airflow DAG integrity tests.

These tests need Apache Airflow, so they are skipped on a plain Python
install and run inside the project's Airflow image:

    make test-dags
    # or
    docker run --rm --entrypoint bash -v "$PWD:/project:ro" -w /project \
        retail-etl-airflow:3.3.0 -c "pip install -q pytest && python -m pytest tests/ -v"

They protect the verified behaviour of the existing pipelines:
    - every DAG file imports without errors
    - the full-refresh DAG keeps its 11 tasks in the same linear order
    - the incremental DAG keeps its 8 tasks in the same linear order
    - every production task alerts through SNS and retries once
"""

from pathlib import Path

import pytest

# "airflow.sdk" (not "airflow") because the project's own airflow/ folder
# is importable as a namespace package when pytest runs from the repo root.
pytest.importorskip("airflow.sdk", reason="Apache Airflow is not installed (run inside Docker)")

try:  # Airflow >= 3.1
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # pragma: no cover - older Airflow 3.0 layout
    from airflow.models.dagbag import DagBag

DAG_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"

FULL_REFRESH_ORDER = [
    "transform_data",
    "split_into_tables",
    "upload_to_s3",
    "verify_snowflake",
    "stage_customers",
    "stage_products",
    "stage_orders",
    "validate_staging",
    "publish_to_snowflake",
    "validate_published_data",
    "validate_final_consistency",
]

INCREMENTAL_ORDER = [
    "upload_batch_001",
    "upload_batch_002",
    "create_incremental_target",
    "validate_s3_batches",
    "merge_incremental_orders",
    "validate_incremental_target",
    "replay_incremental_batches",
    "validate_idempotency",
]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_FOLDER))


def _callbacks(task):
    callback = task.on_failure_callback
    if callback is None:
        return []
    return list(callback) if isinstance(callback, (list, tuple)) else [callback]


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_are_present(dagbag):
    assert {
        "retail_etl_pipeline",
        "incremental_s3_orders",
        "incremental_orders_test",
        "connection_healthcheck",
    } <= set(dagbag.dag_ids)


def test_temporary_failing_dag_was_removed(dagbag):
    assert "sns_failure_alert_test" not in dagbag.dag_ids


@pytest.mark.parametrize(
    ("dag_id", "expected_order"),
    [("retail_etl_pipeline", FULL_REFRESH_ORDER), ("incremental_s3_orders", INCREMENTAL_ORDER)],
)
def test_linear_task_order_is_preserved(dagbag, dag_id, expected_order):
    dag = dagbag.dags[dag_id]  # in-memory; get_dag() queries the metadata DB

    assert sorted(dag.task_ids) == sorted(expected_order)
    for upstream, downstream in zip(expected_order, expected_order[1:]):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}
    assert dag.get_task(expected_order[-1]).downstream_task_ids == set()


@pytest.mark.parametrize("dag_id", ["retail_etl_pipeline", "incremental_s3_orders"])
def test_every_task_alerts_via_sns_after_one_retry(dagbag, dag_id):
    dag = dagbag.dags[dag_id]  # in-memory; get_dag() queries the metadata DB

    for task in dag.tasks:
        names = [getattr(cb, "__name__", "") for cb in _callbacks(task)]
        assert "send_sns_failure_alert" in names, task.task_id
        assert task.retries == 1, task.task_id


@pytest.mark.parametrize("dag_id", ["retail_etl_pipeline", "incremental_s3_orders", "connection_healthcheck"])
def test_dags_are_manual_and_single_run(dagbag, dag_id):
    dag = dagbag.dags[dag_id]  # in-memory; get_dag() queries the metadata DB

    assert dag.max_active_runs == 1
    assert dag.catchup is False
