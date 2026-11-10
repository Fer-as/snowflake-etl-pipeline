variable "retail_s3_role_arn" {
  description = "AWS IAM role ARN used by Snowflake to read retail CSVs"
  type        = string
}

resource "snowflake_storage_integration_aws" "retail_s3" {
  name             = "RETAIL_S3_INTEGRATION"
  enabled          = true
  storage_provider = "S3"

  storage_aws_role_arn = var.retail_s3_role_arn

  storage_allowed_locations = [
    "s3://snowflake-retail-etl-670941257756/retail/"
  ]

  comment = "Read-only S3 integration for the retail ETL pipeline"
}