resource "snowflake_warehouse" "etl_warehouse" {
  name                = "RETAIL_ETL_WH"
  warehouse_type      = "STANDARD"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true

  comment = "Warehouse for the Snowflake retail ETL pipeline"
}