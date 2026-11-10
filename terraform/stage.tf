resource "snowflake_stage_external_s3" "retail" {
  name     = "RETAIL_S3_STAGE"
  database = snowflake_database.retail.name
  schema   = snowflake_schema.retail.name

  url = "s3://snowflake-retail-etl-670941257756/retail/"

  storage_integration = snowflake_storage_integration_aws.retail_s3.name

  comment = "External S3 stage for retail ETL CSV files"
}