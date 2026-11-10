provider "snowflake" {
  authenticator            = "PROGRAMMATIC_ACCESS_TOKEN"
  preview_features_enabled = ["snowflake_table_resource"]
}