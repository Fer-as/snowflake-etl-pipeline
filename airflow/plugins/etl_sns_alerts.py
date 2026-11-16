"""
Amazon SNS failure notifications for Apache Airflow.

Purpose:
    Publish an email-friendly notification to an Amazon SNS topic when
    an Airflow task reaches the FAILED state.

How it is wired:
    Both production DAGs pass ``send_sns_failure_alert`` as the
    ``on_failure_callback`` in ``default_args``. Airflow only calls
    ``on_failure_callback`` once a task has exhausted its retries.
    Intermediate attempts trigger ``on_retry_callback`` instead (not used
    here), so one failed task produces one email rather than one per
    attempt.

Where the callback runs (Airflow 3):
    - Normal task failures: inside the task runner on the Celery worker.
    - Tasks killed externally (heartbeat timeout, OOM): the DAG processor.
    Both containers therefore need AWS credentials (see
    docker-compose.yaml).

Configuration (environment variables):
    AWS_SNS_TOPIC_ARN   Required. ARN of the SNS topic.
    AWS_ALERTS_CONN_ID  Optional. Airflow AWS connection ID.
                        Default: ``aws_default``.
    AWS_DEFAULT_REGION  Optional fallback region. Default: ``eu-west-2``.
    AIRFLOW_UI_URL      Optional base URL shown in the email.
                        Default: ``http://localhost:8080``.

Security:
    - No credentials or email addresses are stored in this file.
    - Credentials come from the Airflow AWS connection. That connection
      holds no keys, so boto3 uses its default credential chain (the
      temporary AWS login session mounted into the containers).
    - Exception messages are NOT copied into the email because they may
      contain SQL, file paths or connection details. Only the exception
      class name is included; details stay in the Airflow task log.

Failure behaviour:
    The callback never raises. If the alert cannot be published, the
    reason is logged at ERROR level so the original task failure remains
    the primary error in Airflow.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Mapping

LOGGER = logging.getLogger(__name__)

DEFAULT_AWS_CONN_ID = "aws_default"
DEFAULT_REGION = "eu-west-2"
DEFAULT_UI_URL = "http://localhost:8080"

# SNS email subjects must be ASCII, contain no line breaks and be
# shorter than 100 characters.
_SNS_SUBJECT_MAX_LENGTH = 99
_TOPIC_ARN_PATTERN = re.compile(r"^arn:aws[a-zA-Z-]*:sns:[a-z0-9-]+:\d{12}:[\w-]{1,256}(\.fifo)?$")


# ============================================================
# MESSAGE CONSTRUCTION
# ============================================================


def _safe_subject(text: str) -> str:
    """Return an SNS-compatible subject line.

    Non-ASCII characters and line breaks are replaced, and the text is
    truncated to the SNS limit.
    """
    ascii_text = text.encode("ascii", errors="replace").decode("ascii")
    single_line = re.sub(r"[\r\n\t]+", " ", ascii_text).strip()
    return single_line[:_SNS_SUBJECT_MAX_LENGTH]


def build_failure_alert(context: Mapping[str, Any]) -> tuple[str, str]:
    """Build the SNS subject and message body for a failed task.

    Args:
        context: The Airflow task context passed to the callback.

    Returns:
        A ``(subject, message)`` tuple.

    Raises:
        ValueError: If the context does not contain a task instance.
    """
    task_instance = context.get("task_instance") or context.get("ti")
    if task_instance is None:
        raise ValueError("Task instance was not found in the Airflow context.")

    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    try_number = getattr(task_instance, "try_number", "unknown")
    max_tries = getattr(task_instance, "max_tries", None)
    run_id = getattr(dag_run, "run_id", None) or getattr(task_instance, "run_id", "unknown")
    logical_date = getattr(dag_run, "logical_date", None)

    # Airflow 3 exposes a direct log link on the runtime task instance.
    # Fall back to the UI home page when it is unavailable.
    ui_url = os.environ.get("AIRFLOW_UI_URL", DEFAULT_UI_URL).rstrip("/")
    log_url = getattr(task_instance, "log_url", None) or ui_url

    attempts = f"{try_number}" if max_tries is None else f"{try_number} (retries allowed: {max_tries})"
    error_type = type(exception).__name__ if exception is not None else "not provided"

    subject = _safe_subject(f"[Airflow FAILED] {dag_id}.{task_id}")

    message = (
        "Snowflake ETL Pipeline - Task Failure Alert\n"
        "===========================================\n"
        "\n"
        "An Airflow task failed after exhausting its retries.\n"
        "\n"
        f"DAG:          {dag_id}\n"
        f"Task:         {task_id}\n"
        f"Run ID:       {run_id}\n"
        f"Logical date: {logical_date if logical_date is not None else 'n/a (manual run)'}\n"
        f"Attempt:      {attempts}\n"
        f"Error type:   {error_type}\n"
        "\n"
        "Full error details are in the Airflow task log:\n"
        f"{log_url}\n"
        "\n"
        "Troubleshooting guide: docs/monitoring_and_alerting.md\n"
    )

    return subject, message


# ============================================================
# SNS CLIENT
# ============================================================


def _get_sns_client(aws_conn_id: str, region: str):
    """Return a boto3 SNS client.

    The Airflow AWS connection is preferred so that credentials and region
    are managed in one place (Task 6 of the challenge). If the Amazon
    provider or the connection is unavailable, plain boto3 with the
    default credential chain is used instead.
    """
    try:
        from airflow.providers.amazon.aws.hooks.sns import SnsHook

        return SnsHook(aws_conn_id=aws_conn_id, region_name=region).get_conn()
    except Exception as error:  # noqa: BLE001 - fall back to boto3 for any hook problem
        LOGGER.warning(
            "Could not create SnsHook for connection '%s' (%s). Falling back to boto3 default credentials.",
            aws_conn_id,
            type(error).__name__,
        )

    import boto3
    from botocore.config import Config

    return boto3.client(
        "sns",
        region_name=region,
        config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 2}),
    )


# ============================================================
# AIRFLOW CALLBACK
# ============================================================


def send_sns_failure_alert(context: Mapping[str, Any]) -> str | None:
    """Airflow ``on_failure_callback`` that publishes an SNS alert.

    Args:
        context: The Airflow task context.

    Returns:
        The SNS ``MessageId`` when published, otherwise ``None``.
        Airflow ignores the return value; it is used by the unit tests.
    """
    topic_arn = os.environ.get("AWS_SNS_TOPIC_ARN", "").strip()
    if not topic_arn:
        LOGGER.error("SNS alert not sent: AWS_SNS_TOPIC_ARN is not set.")
        return None
    if not _TOPIC_ARN_PATTERN.match(topic_arn):
        LOGGER.error("SNS alert not sent: AWS_SNS_TOPIC_ARN is not a valid SNS topic ARN.")
        return None

    aws_conn_id = os.environ.get("AWS_ALERTS_CONN_ID", DEFAULT_AWS_CONN_ID)
    region = os.environ.get("AWS_DEFAULT_REGION", DEFAULT_REGION)

    try:
        subject, message = build_failure_alert(context)
        client = _get_sns_client(aws_conn_id, region)
        response = client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except Exception:  # noqa: BLE001 - a callback must never mask the task failure
        LOGGER.exception("SNS failure alert could not be published.")
        return None

    message_id = response.get("MessageId")
    LOGGER.info("SNS failure alert published. Subject=%r MessageId=%s", subject, message_id)
    return message_id
