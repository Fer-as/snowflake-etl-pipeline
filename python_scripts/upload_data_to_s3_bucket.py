"""
Upload the three transformed table CSVs to Amazon S3.

Source:      assets/datasets/Transformed_tables/{customers,products,orders}.csv
Destination: s3://<RETAIL_S3_BUCKET>/retail/<file>.csv

The Snowflake external stage RETAIL_S3_STAGE points at the ``retail/``
prefix, so these objects are exactly what the COPY INTO statements read.
This is a full-refresh upload: existing objects are replaced each run.

Configuration (environment variables):
    RETAIL_S3_BUCKET    Target bucket (default: project bucket)
    AWS_DEFAULT_REGION  AWS region (default: eu-west-2)

Credentials:
    boto3's default credential chain. In Docker this is the temporary AWS
    login session mounted read-only from the host; no access keys are
    stored in the project.

Large files:
    ``upload_file`` uses boto3's managed transfer, which automatically
    switches to parallel multipart uploads above the threshold below
    (orders.csv is about 44 MB).
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLES_DIR = PROJECT_ROOT / "assets" / "datasets" / "Transformed_tables"

BUCKET_NAME = os.environ.get("RETAIL_S3_BUCKET", "snowflake-retail-etl-670941257756")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-west-2")
S3_PREFIX = "retail"
FILES_TO_UPLOAD = ["customers.csv", "products.csv", "orders.csv"]

TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=16 * 1024 * 1024,
    multipart_chunksize=16 * 1024 * 1024,
    max_concurrency=4,
)


def upload_tables(s3_client=None, tables_dir: Path = TABLES_DIR) -> list[str]:
    """Upload every table CSV and return the S3 keys written.

    Raises:
        FileNotFoundError: If any CSV is missing (checked before uploading
            anything, so a partial upload cannot happen for that reason).
    """
    paths = [Path(tables_dir) / name for name in FILES_TO_UPLOAD]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Files not found: {missing}")

    s3_client = s3_client or boto3.client("s3", region_name=AWS_REGION)

    uploaded = []
    for path in paths:
        key = f"{S3_PREFIX}/{path.name}"
        s3_client.upload_file(
            str(path),
            BUCKET_NAME,
            key,
            ExtraArgs={"ContentType": "text/csv"},
            Config=TRANSFER_CONFIG,
        )
        print(f"Uploaded {path.name} ({path.stat().st_size:,} bytes) to s3://{BUCKET_NAME}/{key}")
        uploaded.append(key)
    return uploaded


def main() -> None:
    """Entry point used by the Airflow upload_to_s3 task."""
    upload_tables()


if __name__ == "__main__":
    main()
