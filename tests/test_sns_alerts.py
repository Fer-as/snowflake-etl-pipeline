"""
Unit tests for the SNS failure-alert callback.

No AWS calls are made: the SNS client is replaced with a fake object.
"""

from types import SimpleNamespace

import pytest

import etl_sns_alerts

VALID_ARN = "arn:aws:sns:eu-west-2:123456789012:retail-etl-airflow-alerts"


class FakeSnsClient:
    """Records publish calls instead of contacting AWS."""

    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def publish(self, **kwargs):
        if self.fail:
            raise ConnectionError("network down")
        self.calls.append(kwargs)
        return {"MessageId": "test-message-id"}


def make_context(exception=None, dag_id="retail_etl_pipeline", task_id="stage_orders"):
    """Build a minimal Airflow-like failure context."""
    task_instance = SimpleNamespace(
        dag_id=dag_id,
        task_id=task_id,
        try_number=2,
        max_tries=1,
        run_id="manual__2026-11-16T10:00:00+00:00",
        log_url="http://localhost:8080/dags/x/runs/y/tasks/z",
    )
    dag_run = SimpleNamespace(run_id=task_instance.run_id, logical_date=None)
    return {"task_instance": task_instance, "dag_run": dag_run, "exception": exception}


@pytest.fixture
def fake_client(monkeypatch):
    client = FakeSnsClient()
    monkeypatch.setattr(etl_sns_alerts, "_get_sns_client", lambda conn_id, region: client)
    monkeypatch.setenv("AWS_SNS_TOPIC_ARN", VALID_ARN)
    return client


def test_publishes_one_alert_with_task_details(fake_client):
    message_id = etl_sns_alerts.send_sns_failure_alert(make_context(RuntimeError("boom")))

    assert message_id == "test-message-id"
    assert len(fake_client.calls) == 1
    call = fake_client.calls[0]
    assert call["TopicArn"] == VALID_ARN
    assert call["Subject"] == "[Airflow FAILED] retail_etl_pipeline.stage_orders"
    assert "Task:         stage_orders" in call["Message"]
    assert "Error type:   RuntimeError" in call["Message"]
    assert "http://localhost:8080/dags/x/runs/y/tasks/z" in call["Message"]


def test_exception_message_is_not_leaked(fake_client):
    secret = "password=SuperSecret123"
    etl_sns_alerts.send_sns_failure_alert(make_context(ValueError(secret)))

    assert secret not in fake_client.calls[0]["Message"]


def test_missing_topic_arn_does_not_publish_or_raise(monkeypatch, fake_client):
    monkeypatch.delenv("AWS_SNS_TOPIC_ARN")

    assert etl_sns_alerts.send_sns_failure_alert(make_context()) is None
    assert fake_client.calls == []


@pytest.mark.parametrize("bad_arn", ["not-an-arn", "arn:aws:s3:::bucket", "arn:aws:sns:eu-west-2:123:x"])
def test_invalid_topic_arn_is_rejected(monkeypatch, fake_client, bad_arn):
    monkeypatch.setenv("AWS_SNS_TOPIC_ARN", bad_arn)

    assert etl_sns_alerts.send_sns_failure_alert(make_context()) is None
    assert fake_client.calls == []


def test_publish_error_is_logged_not_raised(monkeypatch):
    monkeypatch.setenv("AWS_SNS_TOPIC_ARN", VALID_ARN)
    monkeypatch.setattr(etl_sns_alerts, "_get_sns_client", lambda c, r: FakeSnsClient(fail=True))

    assert etl_sns_alerts.send_sns_failure_alert(make_context()) is None


def test_context_without_task_instance_does_not_raise(fake_client):
    assert etl_sns_alerts.send_sns_failure_alert({}) is None
    assert fake_client.calls == []


def test_subject_is_sns_compatible(fake_client):
    long_task = "t" * 150 + "é\n"
    etl_sns_alerts.send_sns_failure_alert(make_context(task_id=long_task))

    subject = fake_client.calls[0]["Subject"]
    assert len(subject) < 100
    assert subject.isascii()
    assert "\n" not in subject


def test_fifo_topic_arn_is_accepted(monkeypatch, fake_client):
    monkeypatch.setenv("AWS_SNS_TOPIC_ARN", VALID_ARN + ".fifo")

    assert etl_sns_alerts.send_sns_failure_alert(make_context()) == "test-message-id"
