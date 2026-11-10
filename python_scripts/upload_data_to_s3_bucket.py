from pathlib import Path
import boto3

s3_client = boto3.client("s3", region_name="eu-west-2")

bucket_name = "snowflake-retail-etl-670941257756"

files_to_upload = [
    "customers.csv",
    "products.csv",
    "orders.csv"
]

for file_path in files_to_upload:
    local_file_path = Path("assets/datasets/Transformed_tables") / file_path

    if not local_file_path.exists():
        raise FileNotFoundError(f"File not found: {local_file_path}")

    try:
        s3_client.upload_file(
            str(local_file_path),
            bucket_name,
            f"retail/{file_path}"
        )
        print(f"Successfully uploaded {file_path} to s3://{bucket_name}/retail/")

    except Exception as error:
        print(f"Failed to upload {file_path}: {error}")
        raise

