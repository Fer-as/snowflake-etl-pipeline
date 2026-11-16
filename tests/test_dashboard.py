"""
Tests for the Streamlit dashboard.

* SQL files: bind-parameter safety, schema columns, service-code list.
* Settings: credential detection and secret-free descriptions.
* SnowflakeSource: executed through a fake DB-API connection.
* LocalCsvSource: reconciled against independently verified figures.
* App: rendered headlessly with streamlit.testing (no browser, no network).
"""

import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

import data_sources as ds

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FULL_RANGE = (date(2010, 12, 1), date(2011, 12, 9))
LOCAL_DATA = (PROJECT_ROOT / "assets/datasets/Transformed_tables/orders.csv").is_file()

QUERY_PARAMS = {
    "kpis": {"start_date", "end_date"},
    "monthly_revenue": {"start_date", "end_date"},
    "top_products": {"start_date", "end_date", "limit", "rank_by_net"},
    "revenue_by_country": {"start_date", "end_date"},
    "customer_summary": {"start_date", "end_date"},
    "data_quality": {"start_date", "end_date"},
}


# ============================================================
# SQL FILES
# ============================================================


@pytest.mark.parametrize("name", ds.QUERY_NAMES)
def test_sql_uses_only_named_bind_parameters(name):
    sql = ds.load_sql(name)
    # pyformat binding treats every '%' as a placeholder, so stray '%'
    # (e.g. LIKE 'x%') would break execution.
    assert set(re.findall(r"%\((\w+)\)s", sql)) == QUERY_PARAMS[name]
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql)
    # Rendering with dummy values must succeed (what the connector does).
    sql % {key: "'x'" for key in QUERY_PARAMS[name]}


@pytest.mark.parametrize("name", ds.QUERY_NAMES)
def test_sql_is_read_only(name):
    sql = re.sub(r"--.*", "", ds.load_sql(name)).upper()
    for keyword in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "TRUNCATE", "ALTER", "GRANT"):
        assert not re.search(rf"\b{keyword}\b", sql), f"{name} contains {keyword}"


def test_service_codes_match_sql():
    sql = ds.load_sql("top_products")
    listed = re.search(r"NOT IN \(([^)]*)\)", sql).group(1)
    assert tuple(re.findall(r"'([^']*)'", listed)) == ds.SERVICE_STOCK_CODES
    assert f"STARTSWITH(o.STOCK_CODE, '{ds.SERVICE_STOCK_PREFIX}')" in sql


def test_revenue_queries_filter_on_regular_sales():
    for name in ("monthly_revenue", "revenue_by_country", "customer_summary"):
        assert "WHERE o.IS_REGULAR_SALE" in ds.load_sql(name) or "WHERE IS_REGULAR_SALE" in ds.load_sql(name)


# ============================================================
# SETTINGS
# ============================================================


def test_missing_credentials_lists_every_problem():
    with pytest.raises(ds.ConfigurationError) as error:
        ds.SnowflakeSettings.from_env({})
    message = str(error.value)
    assert "SNOWFLAKE_ACCOUNT" in message and "SNOWFLAKE_USER" in message and "SNOWFLAKE_TOKEN" in message


def test_pat_settings_and_defaults():
    settings = ds.SnowflakeSettings.from_env(
        {"SNOWFLAKE_ACCOUNT": "org-acct", "SNOWFLAKE_USER": "u", "SNOWFLAKE_TOKEN": "secret-token"}
    )
    kwargs = settings.connect_kwargs()
    assert kwargs["authenticator"] == "PROGRAMMATIC_ACCESS_TOKEN"
    assert kwargs["token"] == "secret-token"
    assert (kwargs["database"], kwargs["schema"], kwargs["warehouse"]) == ("RETAIL_ETL_DB", "RETAIL", "RETAIL_ETL_WH")
    assert "secret-token" not in settings.describe()


def test_key_pair_takes_precedence_over_password():
    settings = ds.SnowflakeSettings.from_env(
        {
            "SNOWFLAKE_ACCOUNT": "a",
            "SNOWFLAKE_USER": "u",
            "SNOWFLAKE_PASSWORD": "pw",
            "SNOWFLAKE_PRIVATE_KEY_FILE": "/keys/k.p8",
        }
    )
    kwargs = settings.connect_kwargs()
    assert kwargs["private_key_file"] == "/keys/k.p8"
    assert "password" not in kwargs


def test_unknown_mode_is_rejected():
    with pytest.raises(ds.ConfigurationError):
        ds.create_source("excel")


def test_end_before_start_is_rejected():
    with pytest.raises(ValueError):
        ds.date_params(date(2011, 2, 1), date(2011, 1, 1))


# ============================================================
# SNOWFLAKE BACKEND (fake connection)
# ============================================================


class FakeCursor:
    def __init__(self, log):
        self.log = log
        self.description = [("REVENUE",), ("ORDERS",)]

    def execute(self, sql, params):
        self.log.append((sql, params))

    def fetchall(self):
        return [(Decimal("123.45"), 3)]

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)


def test_snowflake_source_binds_parameters_and_converts_decimals():
    connection = FakeConnection()
    settings = ds.SnowflakeSettings.from_env({"SNOWFLAKE_ACCOUNT": "a", "SNOWFLAKE_USER": "u", "SNOWFLAKE_TOKEN": "t"})
    source = ds.SnowflakeSource(settings, connection=connection)

    frame = source.top_products(*FULL_RANGE, limit=5, rank_by_net=True)

    sql, params = connection.log[0]
    assert sql == ds.load_sql("top_products")
    assert params == {"start_date": "2010-12-01", "end_date": "2011-12-10", "limit": 5, "rank_by_net": True}
    assert frame["REVENUE"].dtype == float


# ============================================================
# LOCAL BACKEND RECONCILIATION
# ============================================================


@pytest.fixture(scope="module")
def local_source():
    if not LOCAL_DATA:
        pytest.skip("Local transformed CSVs not available")
    return ds.LocalCsvSource()


def test_local_kpis_match_verified_figures(local_source):
    k = local_source.kpis(*FULL_RANGE).iloc[0]
    assert k["TOTAL_LINES"] == 541_909
    assert k["REVENUE"] == pytest.approx(10_666_684.54, abs=0.01)
    assert k["CANCELLED_VALUE"] == pytest.approx(896_812.49, abs=0.01)
    assert k["REVENUE_MISSING_CUSTOMER"] == pytest.approx(1_755_276.64, abs=0.01)
    assert (k["SALES_ORDERS"], k["ACTIVE_CUSTOMERS"], k["CANCELLED_INVOICES"]) == (19_960, 4_338, 3_836)


def test_breakdowns_reconcile_to_total_revenue(local_source):
    total = local_source.kpis(*FULL_RANGE).iloc[0]["REVENUE"]
    assert local_source.monthly_revenue(*FULL_RANGE)["REVENUE"].sum() == pytest.approx(total)
    # Country breakdown (incl. 'Unidentified customer') must not double count.
    assert local_source.revenue_by_country(*FULL_RANGE)["REVENUE"].sum() == pytest.approx(total)


def test_net_ranking_demotes_cancelled_bulk_order(local_source):
    gross = local_source.top_products(*FULL_RANGE, limit=5)
    net = local_source.top_products(*FULL_RANGE, limit=5, rank_by_net=True)
    assert "23843" in gross["STOCK_CODE"].tolist()
    assert "23843" not in net["STOCK_CODE"].tolist()
    assert not set(gross["STOCK_CODE"]) & set(ds.SERVICE_STOCK_CODES)


def test_data_quality_counts(local_source):
    q = local_source.data_quality(*FULL_RANGE).iloc[0]
    assert (q["CANCELLATION"], q["OTHER_NEGATIVE_ADJUSTMENT"], q["NONPOSITIVE_PRICE"], q["MISSING_CUSTOMER"]) == (
        9_288, 1_336, 2_517, 135_080,
    )
    assert q["NAIVE_NET_VALUE"] == pytest.approx(9_747_747.93, abs=0.01)


# ============================================================
# STREAMLIT APP (headless)
# ============================================================

APP = str(PROJECT_ROOT / "dashboard" / "app.py")


def test_app_shows_configuration_error_without_credentials(monkeypatch):
    testing = pytest.importorskip("streamlit.testing.v1")
    for key in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_TOKEN", "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_PRIVATE_KEY_FILE", "SNOWFLAKE_AUTHENTICATOR"):
        monkeypatch.setenv(key, "")  # empty beats any value loaded from dashboard/.env
    monkeypatch.setenv("DASHBOARD_DATA_SOURCE", "snowflake")

    app = testing.AppTest.from_file(APP, default_timeout=60).run()

    assert not app.exception
    assert any("Configuration problem" in e.value for e in app.error)


@pytest.mark.skipif(not LOCAL_DATA, reason="Local transformed CSVs not available")
def test_app_renders_in_local_mode(monkeypatch):
    testing = pytest.importorskip("streamlit.testing.v1")
    monkeypatch.setenv("DASHBOARD_DATA_SOURCE", "local")

    app = testing.AppTest.from_file(APP, default_timeout=120).run()

    assert not app.exception
    assert not app.error
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Revenue"] == "£10.67M"
    assert metrics["Sales orders"] == "19,960"
    assert metrics["Net of cancellations"] == "£9.77M"
