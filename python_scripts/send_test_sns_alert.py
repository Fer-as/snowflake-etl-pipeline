"""
Send one test failure alert through the production SNS callback.

This replaces the temporary always-failing DAG. It calls the exact
function that Airflow runs as ``on_failure_callback``, with a synthetic
context, so the SNS topic, the subscription and the container's AWS
credentials can be checked without failing a DAG or touching any data.

Run inside the Airflow worker (same credentials as real tasks):

    docker compose exec airflow-worker \
        python /opt/airflow/python_scripts/send_test_sns_alert.py

Exit code 0 = SNS accepted the message (check the inbox), 1 = not sent
(the reason is printed in the log output).
"""

import logging
import sys
from pathlib import Path
from types import SimpleNamespace

# The callback lives in airflow/plugins, which Airflow adds to sys.path.
# Add it explicitly so this script also works as a plain Python process.
PLUGINS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "plugins"
sys.path.insert(0, str(PLUGINS_DIR))

from etl_sns_alerts import send_sns_failure_alert  # noqa: E402


def main() -> int:
    """Publish the test alert and return a process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    task_instance = SimpleNamespace(
        dag_id="manual_alert_test",
        task_id="send_test_sns_alert",
        try_number=1,
        max_tries=0,
        run_id="manual_alert_test",
    )
    context = {
        "task_instance": task_instance,
        "dag_run": SimpleNamespace(run_id="manual_alert_test", logical_date=None),
        "exception": RuntimeError("synthetic test failure"),
    }

    message_id = send_sns_failure_alert(context)
    if message_id is None:
        print("Test alert was NOT sent. See the log lines above.")
        return 1
    print(f"Test alert accepted by SNS. MessageId={message_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
