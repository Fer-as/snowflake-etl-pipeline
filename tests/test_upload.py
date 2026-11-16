"""
Unit tests for the S3 upload script using a fake S3 client (no AWS calls).
"""

import pytest

import upload_data_to_s3_bucket as upload


class FakeS3:
    def __init__(self):
        self.uploads = []

    def upload_file(self, filename, bucket, key, ExtraArgs=None, Config=None):  # noqa: N803 - boto3 names
        self.uploads.append((bucket, key, ExtraArgs))


def test_uploads_all_three_tables_under_retail_prefix(tmp_path):
    for name in upload.FILES_TO_UPLOAD:
        (tmp_path / name).write_text("id\n1\n")
    client = FakeS3()

    keys = upload.upload_tables(s3_client=client, tables_dir=tmp_path)

    assert keys == ["retail/customers.csv", "retail/products.csv", "retail/orders.csv"]
    assert all(bucket == upload.BUCKET_NAME for bucket, _, _ in client.uploads)
    assert all(extra == {"ContentType": "text/csv"} for _, _, extra in client.uploads)


def test_missing_file_fails_before_any_upload(tmp_path):
    (tmp_path / "customers.csv").write_text("id\n1\n")
    client = FakeS3()

    with pytest.raises(FileNotFoundError):
        upload.upload_tables(s3_client=client, tables_dir=tmp_path)
    assert client.uploads == []
