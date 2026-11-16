"""
Shared pytest configuration.

Adds the project's script folders to ``sys.path`` so tests can import the
ETL scripts, the SNS alert plugin and the dashboard modules exactly as
Airflow and Streamlit do at runtime.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

for folder in ("python_scripts", "airflow/plugins", "dashboard"):
    path = str(PROJECT_ROOT / folder)
    if path not in sys.path:
        sys.path.insert(0, path)
