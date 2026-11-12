
"""
Upload immutable incremental order batches to Amazon S3.

Batch 001:
    First three source orders plus a repeated first order.
    Already uploaded and loaded into Snowflake.

Batch 002:
    Four different order lines from the same test dataset.
    Demonstrates adding records from a second incoming file.

Safety:
    - Does not modify the source dataset.
    - Does not overwrite existing local or S3 batch content.
    - Uses AWS credentials provided by the environment.
    - Preserves backward compatibility with the existing DAG.

Usage:
    python upload_incremental_batch.py --batch-id 001
    python upload_incremental_batch.py --batch-id 002

Without --batch-id, the script uses batch 001.
"""

import argparse
from hashlib import sha256
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_CSV = (
    PROJECT_ROOT
    / "assets"
    / "datasets"
    / "Transformed_tables"
    / "orders.csv"
)

OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "assets"
    / "datasets"
    / "Incremental_batches"
)

BUCKET_NAME = "snowflake-retail-etl-670941257756"
AWS_REGION = "eu-west-2"
S3_PREFIX = "retail/incremental"


# Use exactly the same column order as batch 001.
# ORDER_LINE_ID is excluded because it is position-based
# and isn't a stable identifier for incremental loading.

ORDER_COLUMNS = [
    "invoice_number",
    "stock_code",
    "customer_id",
    "invoice_date",
    "quantity",
    "unit_price",
    "is_cancellation",
    "is_other_negative_adjustment",
    "is_nonpositive_price",
    "is_regular_sale",
    "is_missing_customer",
]


# ============================================================
# BATCH DEFINITIONS
# ============================================================

BATCH_SPECS = {
    "001": {
        "filename": "orders_batch_001.csv",
        "rows_to_read": 3,
        "start_position": 0,
        "end_position": 3,
        "repeat_first_row": True,
    },
    "002": {
        "filename": "orders_batch_002.csv",
        "rows_to_read": 7,
        "start_position": 3,
        "end_position": 7,
        "repeat_first_row": False,
    },
}


# ============================================================
# COMMAND-LINE ARGUMENTS
# ============================================================

def parse_arguments():
    """Read the requested batch ID."""

    parser = argparse.ArgumentParser(
        description="Upload an immutable incremental CSV batch."
    )

    parser.add_argument(
        "--batch-id",
        choices=sorted(BATCH_SPECS.keys()),
        default="001",
        help="Batch to upload; default is 001.",
    )

    return parser.parse_args()


# ============================================================
# STEP 1: BUILD THE CSV CONTENT
# ============================================================

def create_incremental_batch(batch_id):
    """Prepare deterministic CSV bytes for a batch."""

    if not SOURCE_CSV.is_file():
        raise FileNotFoundError(
            f"Source orders CSV not found: {SOURCE_CSV}"
        )

    specification = BATCH_SPECS[batch_id]

    # Read only the source rows needed for this batch.
    # Keep key fields as strings to preserve their values.
    orders = pd.read_csv(
        SOURCE_CSV,
        nrows=specification["rows_to_read"],
        encoding="utf-8",
        dtype={
            "invoice_number": "string",
            "stock_code": "string",
            "customer_id": "Int64",
        },
    )

    missing_columns = set(ORDER_COLUMNS) - set(
        orders.columns
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    batch = orders.iloc[
        specification["start_position"]:
        specification["end_position"]
    ][ORDER_COLUMNS].copy()

    if len(batch) != 3 and batch_id == "001":
        raise ValueError(
            "Batch 001 requires three source rows."
        )

    if len(batch) != 4 and batch_id == "002":
        raise ValueError(
            "Batch 002 requires four source rows."
        )

    # Preserve the original four-record batch 001 layout.
    # The repeated record has the same business values,
    # but Snowflake can identify it by its file row number.
    if specification["repeat_first_row"]:
        repeated_row = batch.iloc[[0]].copy()

        batch = pd.concat(
            [batch, repeated_row],
            ignore_index=True,
        )

    # Use the same CSV formatting as the original uploader.
    csv_text = batch.to_csv(
        index=False,
        lineterminator="\n",
    )

    content = csv_text.encode("utf-8")

    return content, len(batch)


# ============================================================
# STEP 2: SAVE AN IMMUTABLE LOCAL FILE
# ============================================================

def save_local_batch(filename, content):
    """Create the CSV, or verify an identical existing one."""

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = OUTPUT_DIRECTORY / filename

    if output_path.exists():
        existing_content = output_path.read_bytes()

        if existing_content != content:
            raise ValueError(
                "Local batch exists with different content: "
                f"{output_path}. Use a new batch filename."
            )

        print("Local batch already exists and is identical.")

        return output_path

    output_path.write_bytes(content)

    print(f"Created local batch: {output_path}")

    return output_path


# ============================================================
# STEP 3: UPLOAD WITHOUT OVERWRITING S3 OBJECTS
# ============================================================

def upload_immutable_batch(s3_key, content):
    """Upload a new S3 object, or verify identical content."""

    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
    )

    try:
        # S3 only accepts this write if the key is absent.
        # A pre-existing object causes HTTP 412.
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=content,
            ContentType="text/csv",
            IfNoneMatch="*",
        )

        print("New incremental batch uploaded successfully.")

    except ClientError as error:
        status_code = error.response.get(
            "ResponseMetadata", {}
        ).get("HTTPStatusCode")

        if status_code != 412:
            raise

        # An existing file can be reused only if its
        # exact bytes match the expected batch content.
        existing_object = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
        )

        existing_content = existing_object["Body"].read()

        if existing_content != content:
            raise ValueError(
                "S3 batch exists with different content: "
                f"{s3_key}. Do not overwrite it."
            ) from error

        print(
            "S3 batch already exists with identical content. "
            "No upload needed."
        )


# ============================================================
# MAIN ENTRY POINT
# ============================================================

def main():
    """Create, save and upload the requested batch."""

    arguments = parse_arguments()

    batch_id = arguments.batch_id

    specification = BATCH_SPECS[batch_id]

    filename = specification["filename"]

    s3_key = f"{S3_PREFIX}/{filename}"

    content, record_count = create_incremental_batch(
        batch_id
    )

    save_local_batch(
        filename,
        content,
    )

    upload_immutable_batch(
        s3_key,
        content,
    )

    checksum = sha256(content).hexdigest()

    print("")
    print("Incremental batch summary")
    print("-------------------------")
    print(f"Batch ID: {batch_id}")
    print(f"Filename: {filename}")
    print(f"Records: {record_count}")
    print(f"SHA-256: {checksum}")
    print(f"S3 location: s3://{BUCKET_NAME}/{s3_key}")


if __name__ == "__main__":
    main()
