resource "snowflake_database" "retail" {
  name    = "RETAIL_ETL_DB"
  comment = "Database for the retail ETL pipeline"
}

resource "snowflake_schema" "retail" {
  name     = "RETAIL"
  database = snowflake_database.retail.name

  comment = "Schema containing retail customer, product and order tables"
}