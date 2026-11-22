# Retail Sales Dashboard

A Streamlit dashboard over the validated Snowflake tables
`RETAIL_ETL_DB.RETAIL.ORDERS`, `CUSTOMERS` and `PRODUCTS`, which are loaded by
the `retail_etl_pipeline` Airflow DAG.

| Tab | What it shows |
|---|---|
| KPI row | Revenue, sales orders, average order value, active customers, cancelled value, net of cancellations |
| Revenue trend | Monthly regular-sale revenue and orders |
| Top products | Top N products by gross revenue, or net of cancellations (toggle) |
| Countries | Revenue by the customer's primary country, UK share, and revenue that cannot be attributed |
| Customers | Repeat rate, medians, orders-per-customer distribution, top customers |
| Data quality | Line counts per quality flag, and the revenue-definition comparison |
| How the numbers work | The calculation rules (below) |

Every chart has a **Table view** expander. The SQL behind every chart is in
[`sql/`](sql/).

## Run it

```powershell
# From the repository root
python -m venv .venv
.venv\Scripts\activate            # macOS/Linux: source .venv/bin/activate
pip install -r dashboard/requirements.txt

copy dashboard\.env.example dashboard\.env   # then edit; the file is git-ignored
streamlit run dashboard/app.py               # opens http://localhost:8501
```

### No Snowflake access? Use offline mode

```powershell
$env:DASHBOARD_DATA_SOURCE = "local"   # bash: export DASHBOARD_DATA_SOURCE=local
streamlit run dashboard/app.py
```

Offline mode computes **the same metrics with pandas** from
`assets/datasets/Transformed_tables/*.csv`. Generate those files first with
the transform and split scripts. You can also switch mode in the sidebar.
Because the two implementations are independent, comparing them with
identical filters is a reconciliation check of the Snowflake load.

## Configuration

All settings come from environment variables, or from `dashboard/.env` if
`python-dotenv` is installed. No credentials are stored in code.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `SNOWFLAKE_ACCOUNT` | yes | – | `<orgname>-<accountname>`, without `.snowflakecomputing.com` |
| `SNOWFLAKE_USER` | yes | – | |
| `SNOWFLAKE_ROLE` | no | user default | Needs `USAGE` on the warehouse, database and schema, and `SELECT` on the three tables |
| `SNOWFLAKE_WAREHOUSE` | no | `RETAIL_ETL_WH` | |
| `SNOWFLAKE_DATABASE` / `SNOWFLAKE_SCHEMA` | no | `RETAIL_ETL_DB` / `RETAIL` | |
| **Auth (one of)** | yes | | Precedence is the order of this list |
| `SNOWFLAKE_PRIVATE_KEY_FILE` (+ `_PWD`) | | | Key-pair auth (recommended for services) |
| `SNOWFLAKE_TOKEN` | | | Programmatic access token (the same method Terraform uses) |
| `SNOWFLAKE_PASSWORD` | | | |
| `SNOWFLAKE_AUTHENTICATOR=externalbrowser` | | | SSO in the browser |
| `DASHBOARD_DATA_SOURCE` | no | `snowflake` | `snowflake` or `local` |

A least-privilege read-only role for the dashboard (run as an admin):

```sql
CREATE ROLE IF NOT EXISTS RETAIL_DASHBOARD_READER;
GRANT USAGE ON WAREHOUSE RETAIL_ETL_WH TO ROLE RETAIL_DASHBOARD_READER;
GRANT USAGE ON DATABASE RETAIL_ETL_DB TO ROLE RETAIL_DASHBOARD_READER;
GRANT USAGE ON SCHEMA RETAIL_ETL_DB.RETAIL TO ROLE RETAIL_DASHBOARD_READER;
GRANT SELECT ON TABLE RETAIL_ETL_DB.RETAIL.ORDERS    TO ROLE RETAIL_DASHBOARD_READER;
GRANT SELECT ON TABLE RETAIL_ETL_DB.RETAIL.CUSTOMERS TO ROLE RETAIL_DASHBOARD_READER;
GRANT SELECT ON TABLE RETAIL_ETL_DB.RETAIL.PRODUCTS  TO ROLE RETAIL_DASHBOARD_READER;
GRANT ROLE RETAIL_DASHBOARD_READER TO USER <your_user>;
```

### Error handling

| Situation | What you see |
|---|---|
| Missing variables | A *Configuration problem* message listing **every** missing setting, an example `.env`, and a hint to use offline mode |
| Wrong account, credentials, network or role | *Could not load data* with the connector's error type and message. Press **Refresh data** after fixing it |
| Missing local CSVs (offline mode) | A message naming the missing files and the scripts that create them |
| Empty date range | A warning instead of empty charts |

Query results are cached for 10 minutes. **Refresh data** clears the cache.

## How the numbers are calculated

* **Revenue** = `SUM(QUANTITY * UNIT_PRICE)` where `IS_REGULAR_SALE`, meaning
  quantity > 0, price > 0, and the invoice does not start with `C`.
* **Cancellations** are excluded from revenue and shown separately as
  *Cancelled value*. *Net of cancellations* = revenue − cancelled value. It is
  approximate, because a cancellation can reverse a sale from outside the
  selected range.
* **Other negative adjustments** (all priced 0) and **non-positive prices**
  (for example the negative *Adjust bad debt* line) are excluded.
* **Missing customers** (about 25% of lines) are **included** in revenue,
  orders and products. They are excluded from customer statistics, and the
  Countries tab shows them as *Revenue with no customer/country*.
* **Country** comes from `CUSTOMERS.PRIMARY_COUNTRY` (the customer's most
  frequent country), because `ORDERS` has no country column.
* **Top products** exclude service codes: postage, carriage, manual lines,
  fees, bad debt and gift vouchers. Gross ranking includes orders that were
  later cancelled; for example, 80,995 × 23843 was ordered and cancelled on
  9 Dec 2011. Net ranking corrects for this.
* **No double counting.** Every query aggregates `ORDERS` at line level. Joins
  use the unique keys of `CUSTOMERS` and `PRODUCTS`, which the DAG validates,
  so a join cannot multiply rows. *Orders* = distinct invoice numbers. The
  tests assert that monthly and country breakdowns sum exactly to total
  revenue.

Reference figures for the full dataset (`tests/test_dashboard.py`):
revenue **£10,666,684.54** · 19,960 sales orders · 4,338 active customers ·
cancelled value £896,812.49 across 3,836 invoices.

## Screenshots

`docs/screenshots/dashboard_*.png` were captured in **offline mode** (the
caption in each image names the data source). To capture your own, for
example in live Snowflake mode:

```powershell
pip install playwright                 # optional dev tool; uses installed Chrome
streamlit run dashboard/app.py         # terminal 1
python dashboard/capture_screenshots.py  # terminal 2 -> docs/screenshots/
```

## Tests

```powershell
python -m pytest tests/test_dashboard.py -v
```

They cover SQL bind-parameter safety and read-only checks, credential
handling, a fake Snowflake connection, reconciliation of the offline
backend, and two headless runs of the app (missing credentials, and offline
mode).
