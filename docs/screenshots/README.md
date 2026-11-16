# Screenshots

## Included

| File | Shows | Captured how |
|---|---|---|
| `dashboard_1_revenue_trend.png` | KPI row and monthly revenue | `dashboard/capture_screenshots.py`, **offline (local CSV) mode** |
| `dashboard_2_top_products.png` | Top 10 products (gross) | same |
| `dashboard_3_countries.png` | Revenue by country (UK hidden) | same |
| `dashboard_4_customers.png` | Customer statistics | same |
| `dashboard_5_data_quality.png` | Quality flags and revenue definitions | same |

Offline mode computes the same metrics as the Snowflake queries; the
caption under the title in each image names the data source.

## Still to capture (needs live services)

These require a running Airflow stack with valid AWS and Snowflake sessions,
so they cannot be generated from the repository alone. Save them in this
folder with the suggested names, then link them from the main README.

| Suggested file | How to capture |
|---|---|
| `airflow_dags_list.png` | Airflow UI → *Dags*: `retail_etl_pipeline`, `incremental_s3_orders`, `connection_healthcheck` |
| `airflow_full_refresh_graph.png` | `retail_etl_pipeline` → a successful run → *Graph* (11 green tasks) |
| `airflow_incremental_graph.png` | `incremental_s3_orders` → a successful run → *Graph* (8 green tasks) |
| `airflow_healthcheck_run.png` | `connection_healthcheck` successful run |
| `sns_alert_email.png` | The email from `make alert-test`. **Crop or blur your email address** |
| `snowflake_tables.png` | Snowsight → `RETAIL_ETL_DB.RETAIL`, with row counts for CUSTOMERS / PRODUCTS / ORDERS |
| `snowflake_incremental_lineage.png` | Output of the trace query in `docs/lineage.md` §9 |
| `dashboard_live_snowflake.png` | Dashboard with *Snowflake (live)* selected: `python dashboard/capture_screenshots.py` |

Before committing any screenshot, check that it shows no account
identifiers, ARNs, email addresses, tokens or browser bookmarks.
