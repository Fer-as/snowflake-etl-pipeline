resource "snowflake_table" "customers" {
  name     = "CUSTOMERS"
  database = snowflake_database.retail.name
  schema   = snowflake_schema.retail.name

  column {
    name     = "CUSTOMER_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }

  column {
    name = "PRIMARY_COUNTRY"
    type = "VARCHAR"
  }
}
resource "snowflake_table" "products" {
  name     = "PRODUCTS"
  database = snowflake_database.retail.name
  schema   = snowflake_schema.retail.name

  column {
    name     = "STOCK_CODE"
    type     = "VARCHAR"
    nullable = false
  }

  column {
    name     = "DESCRIPTION"
    type     = "VARCHAR"
    nullable = true
  }
}
resource "snowflake_table" "orders" {
  name     = "ORDERS"
  database = snowflake_database.retail.name
  schema   = snowflake_schema.retail.name

  column {
    name     = "ORDER_LINE_ID"
    type     = "NUMBER(38,0)"
    nullable = false
  }

  column {
    name     = "INVOICE_NUMBER"
    type     = "VARCHAR"
    nullable = true
  }

  column {
    name     = "STOCK_CODE"
    type     = "VARCHAR"
    nullable = true
  }

  column {
    name     = "CUSTOMER_ID"
    type     = "NUMBER(38,0)"
    nullable = true
  }

  column {
    name     = "INVOICE_DATE"
    type     = "TIMESTAMP_NTZ"
    nullable = true
  }

  column {
    name     = "QUANTITY"
    type     = "NUMBER(38,0)"
    nullable = true
  }

  column {
    name     = "UNIT_PRICE"
    type     = "NUMBER(18,4)"
    nullable = true
  }

  column {
    name     = "IS_CANCELLATION"
    type     = "BOOLEAN"
    nullable = true
  }

  column {
    name     = "IS_OTHER_NEGATIVE_ADJUSTMENT"
    type     = "BOOLEAN"
    nullable = true
  }

  column {
    name     = "IS_NONPOSITIVE_PRICE"
    type     = "BOOLEAN"
    nullable = true
  }

  column {
    name     = "IS_REGULAR_SALE"
    type     = "BOOLEAN"
    nullable = true
  }

  column {
    name     = "IS_MISSING_CUSTOMER"
    type     = "BOOLEAN"
    nullable = true
  }
}