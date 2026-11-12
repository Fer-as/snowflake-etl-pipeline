
"""
Prepare and upload an immutable incremental orders batch.

Purpose:
    Demonstrate incremental ingestion using a new S3 object.

Safety:
    - Does not modify the original dataset.
    - Does not modify the full-refresh ETL pipeline.
    - Does not overwrite an existing S3 batch.
    - Does not contain AWS credentials.

Record identity:
    Snowflake will use the S3 filename and file row number.
    The generated ORDER_LINE_ID is deliberately excluded.
"""

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

BATCH_FILENAME = "orders_batch_001.csv"

BUCKET_NAME = "snowflake-retail-etl-670941257756"
AWS_REGION = "eu-west-2"

S3_KEY = f"retail/incremental/{BATCH_FILENAME}"


# Keep all order attributes, except the position-based ID.
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
# STEP 1: CREATE A SMALL INCOMING BATCH
# ============================================================

def create_incremental_batch() -> bytes:
    """Build a four-row CSV from existing test data."""

    if not SOURCE_CSV.is_file():
        raise FileNotFoundError(
            f"Source CSV not found: {SOURCE_CSV}"
        )

    # Read three existing rows for our demonstration.
    orders = pd.read_csv(
        SOURCE_CSV,
        nrows=3,
        encoding="utf-8",
        dtype={
            "invoice_number": "string",
            "stock_code": "string",
            "customer_id": "Int64",
        },
    )

    if len(orders) != 3:
        raise ValueError(
            "Expected at least three source order rows."
        )

    missing_columns = set(ORDER_COLUMNS) - set(orders.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    # Copy only the transaction attributes.
    batch = orders[ORDER_COLUMNS].copy()

    # Deliberately repeat the first transaction.
    # This demonstrates why transaction values alone
    # cannot serve as a unique record identifier.
    repeated_row = batch.iloc[[0]].copy()

    batch = pd.concat(
        [batch, repeated_row],
        ignore_index=True,
    )

    if len(batch) != 4:
        raise ValueError("The test batch must contain four rows.")

    # Generate deterministic UTF-8 CSV content.
    csv_text = batch.to_csv(
        index=False,
        lineterminator="\n",
    )

    return csv_text.encode("utf-8")


# ============================================================
# STEP 2: SAVE THE BATCH LOCALLY
# ============================================================

def save_local_batch(content: bytes) -> Path:
    """Save the batch without silently replacing its content."""

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = OUTPUT_DIRECTORY / BATCH_FILENAME

    if output_path.exists():
        existing_content = output_path.read_bytes()

        if existing_content != content:
            raise ValueError(
                "Local batch already exists with different "
                "content. Use a new batch filename."
            )

        print("Local batch already exists and is identical.")
        return output_path

    output_path.write_bytes(content)

    print(f"Created local batch: {output_path}")

    return output_path


# ============================================================
# STEP 3: UPLOAD AN IMMUTABLE OBJECT TO S3
# ============================================================

def upload_immutable_batch(content: bytes) -> None:
    """
    Upload only if the S3 key does not exist.

    A repeated run is allowed if the existing object has
    exactly the same bytes.

    Different content must use a different batch filename.
    """

    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
    )

    try:
        # IfNoneMatch prevents overwriting an existing object.
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=S3_KEY,
            Body=content,
            ContentType="text/csv",
            IfNoneMatch="*",
        )

        print("New incremental batch uploaded successfully.")

    except ClientError as error:
        status_code = error.response.get(
            "ResponseMetadata", {}
        ).get("HTTPStatusCode")

        # HTTP 412 means the object already exists.
        if status_code != 412:
            raise

        existing_object = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=S3_KEY,
        )

        existing_content = existing_object["Body"].read()

        if existing_content != content:
            raise ValueError(
                "S3 batch already exists with different "
                "content. Never overwrite an immutable batch."
            ) from error

        print(
            "S3 batch already exists with identical content. "
            "No upload needed."
        )


# ============================================================
# MAIN ENTRY POINT
# ============================================================

def main() -> None:
    """Prepare and upload the incremental test batch."""

    content = create_incremental_batch()

    save_local_batch(content)

    upload_immutable_batch(content)

    checksum = sha256(content).hexdigest()

    print(f"Batch filename: {BATCH_FILENAME}")
    print("Batch records: 4")
    print(f"SHA-256: {checksum}")
    print(f"S3 location: s3://{BUCKET_NAME}/{S3_KEY}")


if __name__ == "__main__":
    main()
