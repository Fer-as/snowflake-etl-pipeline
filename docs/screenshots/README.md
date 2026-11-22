# Screenshots

Every image here was captured from a real run. None show account
identifiers, ARNs, email addresses or tokens.

## Airflow (live AWS + Snowflake runs, 22 Nov 2026)

| File | Shows |
|---|---|
| `airflow_dags_list.png` | The four DAGs, each with a successful latest run |
| `airflow_full_refresh_run.png` | `retail_etl_pipeline` run: all 11 task instances succeeded |
| `airflow_full_refresh_graph.png` / `_crop.png` | The same run in Graph view (11 green tasks) |
| `airflow_full_refresh_validation_log.png` | `validate_published_data` log: count and uniqueness checks for 4,372 / 4,070 / 541,909 all `True` |
| `airflow_incremental_graph.png` / `_crop.png` | `incremental_s3_orders` run in Graph view (8 green tasks) |
| `airflow_incremental_idempotency_log.png` | `validate_idempotency` log after a replay: all checks `True` |
| `airflow_healthcheck_run.png` | `connection_healthcheck`: AWS identity, S3 objects, SNS topic and Snowflake checks passed |

Captured with Playwright against the local Airflow UI (1600×1000 viewport,
2× scale).

## Dashboard

| File | Source |
|---|---|
| `dashboard_live_1_revenue_trend.png` … `dashboard_live_5_data_quality.png` | **Live Snowflake** (`RETAIL_ETL_DB.RETAIL`) |
| `dashboard_1_revenue_trend.png` … `dashboard_5_data_quality.png` | Offline (local CSV) mode |

The two sets show identical figures (revenue £10,666,684.54). The caption under
the title in each image names the data source. To regenerate them:

```powershell
python dashboard/capture_screenshots.py                                  # offline app on :8501
python dashboard/capture_screenshots.py --url http://localhost:8502 --prefix dashboard_live
```

## Not included (need a personal login)

| Item | Why it isn't here | Equivalent evidence |
|---|---|---|
| SNS alert email | Shows a personal inbox and address | `make alert-test` returns the SNS MessageId; delivery was confirmed by the recipient |
| Snowsight table views | Needs an interactive Snowflake login | The validation-log screenshots above show the same row-count and uniqueness checks |

If you add either one, crop or blur the email address and account
locator first.
