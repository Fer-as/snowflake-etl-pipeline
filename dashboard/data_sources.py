"""
Data access for the retail dashboard.

Two interchangeable backends expose the same six methods and return
DataFrames with identical (upper-case) column names:

    SnowflakeSource  Runs the SQL files in dashboard/sql/ against
                     RETAIL_ETL_DB.RETAIL (the production path).
    LocalCsvSource   Computes the same metrics with pandas from
                     assets/datasets/Transformed_tables/*.csv. Lets a
                     reviewer run the dashboard without Snowflake, and acts
                     as an independent reconciliation baseline.

Configuration is read from environment variables (optionally loaded from
dashboard/.env). No credentials are stored in code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

DASHBOARD_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DASHBOARD_DIR.parent
SQL_DIR = DASHBOARD_DIR / "sql"
LOCAL_TABLES_DIR = PROJECT_ROOT / "assets" / "datasets" / "Transformed_tables"

# Stock codes that are services or accounting lines, not merchandise.
# Must match the NOT IN list in sql/top_products.sql (tested).
SERVICE_STOCK_CODES = (
    "AMAZONFEE", "B", "BANK CHARGES", "C2", "CRUK", "D", "DOT", "M", "PADS", "POST", "S", "m",
)
SERVICE_STOCK_PREFIX = "gift_"

QUERY_NAMES = ("kpis", "monthly_revenue", "top_products", "revenue_by_country", "customer_summary", "data_quality")


class ConfigurationError(RuntimeError):
    """Raised when required settings (e.g. credentials) are missing or invalid."""


class DataSourceError(RuntimeError):
    """Raised when the data source cannot be reached or a query fails."""


def load_sql(name: str) -> str:
    """Return the text of ``dashboard/sql/<name>.sql``."""
    return (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")


# ============================================================
# SNOWFLAKE CONFIGURATION
# ============================================================


@dataclass(frozen=True)
class SnowflakeSettings:
    """Connection settings read from SNOWFLAKE_* environment variables."""

    account: str
    user: str
    warehouse: str
    database: str
    schema: str
    role: str | None
    auth_method: str
    secret: str | None = None
    private_key_file: str | None = None

    @classmethod
    def from_env(cls, env: dict | None = None) -> "SnowflakeSettings":
        """Build settings and choose an authentication method.

        Precedence: key pair > programmatic access token > password >
        SNOWFLAKE_AUTHENTICATOR=externalbrowser (SSO in the browser).

        Raises:
            ConfigurationError: Listing every missing variable.
        """
        env = os.environ if env is None else env
        get = lambda key: (env.get(key) or "").strip() or None  # noqa: E731

        missing = [key for key in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER") if not get(key)]

        if get("SNOWFLAKE_PRIVATE_KEY_FILE"):
            auth_method, secret = "keypair", get("SNOWFLAKE_PRIVATE_KEY_FILE_PWD")
        elif get("SNOWFLAKE_TOKEN"):
            auth_method, secret = "pat", get("SNOWFLAKE_TOKEN")
        elif get("SNOWFLAKE_PASSWORD"):
            auth_method, secret = "password", get("SNOWFLAKE_PASSWORD")
        elif (get("SNOWFLAKE_AUTHENTICATOR") or "").lower() == "externalbrowser":
            auth_method, secret = "externalbrowser", None
        else:
            auth_method, secret = "none", None
            missing.append(
                "one of SNOWFLAKE_TOKEN, SNOWFLAKE_PASSWORD, SNOWFLAKE_PRIVATE_KEY_FILE "
                "or SNOWFLAKE_AUTHENTICATOR=externalbrowser"
            )

        if missing:
            raise ConfigurationError("Missing Snowflake settings: " + "; ".join(missing))

        return cls(
            account=get("SNOWFLAKE_ACCOUNT"),
            user=get("SNOWFLAKE_USER"),
            warehouse=get("SNOWFLAKE_WAREHOUSE") or "RETAIL_ETL_WH",
            database=get("SNOWFLAKE_DATABASE") or "RETAIL_ETL_DB",
            schema=get("SNOWFLAKE_SCHEMA") or "RETAIL",
            role=get("SNOWFLAKE_ROLE"),
            auth_method=auth_method,
            secret=secret,
            private_key_file=get("SNOWFLAKE_PRIVATE_KEY_FILE"),
        )

    def connect_kwargs(self) -> dict:
        """Keyword arguments for ``snowflake.connector.connect``."""
        kwargs = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "application": "retail_etl_dashboard",
            "login_timeout": 30,
            "network_timeout": 60,
        }
        if self.role:
            kwargs["role"] = self.role
        if self.auth_method == "keypair":
            kwargs["private_key_file"] = self.private_key_file
            if self.secret:
                kwargs["private_key_file_pwd"] = self.secret
        elif self.auth_method == "pat":
            kwargs["authenticator"] = "PROGRAMMATIC_ACCESS_TOKEN"
            kwargs["token"] = self.secret
        elif self.auth_method == "password":
            kwargs["password"] = self.secret
        elif self.auth_method == "externalbrowser":
            kwargs["authenticator"] = "externalbrowser"
        return kwargs

    def describe(self) -> str:
        """Human-readable, secret-free summary for the UI."""
        return f"{self.database}.{self.schema} on {self.warehouse} ({self.auth_method} auth)"


# ============================================================
# SHARED HELPERS
# ============================================================


def date_params(start: date, end: date) -> dict:
    """Bind parameters for an inclusive [start, end] date range.

    The SQL uses ``>= start`` and ``< end + 1 day`` so the whole end day is
    included regardless of the time component.
    """
    if end < start:
        raise ValueError("End date must be on or after the start date.")
    return {
        "start_date": start.isoformat(),
        "end_date": (pd.Timestamp(end) + pd.Timedelta(days=1)).date().isoformat(),
    }


# ============================================================
# SNOWFLAKE BACKEND
# ============================================================


class SnowflakeSource:
    """Runs the dashboard SQL files on Snowflake (read-only)."""

    label = "Snowflake"

    def __init__(self, settings: SnowflakeSettings, connection=None):
        self.settings = settings
        self._connection = connection

    def describe(self) -> str:
        """Secret-free summary shown in the dashboard caption."""
        return self.settings.describe()

    def _connect(self):
        if self._connection is None:
            try:
                import snowflake.connector
            except ImportError as error:
                raise ConfigurationError(
                    "snowflake-connector-python is not installed. Run: pip install -r dashboard/requirements.txt"
                ) from error
            try:
                self._connection = snowflake.connector.connect(**self.settings.connect_kwargs())
            except Exception as error:  # noqa: BLE001 - surface a clean message to the UI
                raise DataSourceError(f"Could not connect to Snowflake: {type(error).__name__}: {error}") from error
        return self._connection

    def _query(self, name: str, params: dict) -> pd.DataFrame:
        cursor = self._connect().cursor()
        try:
            cursor.execute(load_sql(name), params)
            columns = [column[0].upper() for column in cursor.description]
            frame = pd.DataFrame(cursor.fetchall(), columns=columns)
        except DataSourceError:
            raise
        except Exception as error:  # noqa: BLE001
            raise DataSourceError(f"Query '{name}' failed: {type(error).__name__}: {error}") from error
        finally:
            cursor.close()
        return _normalise_numbers(frame)

    def kpis(self, start, end):
        return self._query("kpis", date_params(start, end))

    def monthly_revenue(self, start, end):
        return self._query("monthly_revenue", date_params(start, end))

    def top_products(self, start, end, limit=10, rank_by_net=False):
        params = {**date_params(start, end), "limit": int(limit), "rank_by_net": bool(rank_by_net)}
        return self._query("top_products", params)

    def revenue_by_country(self, start, end):
        return self._query("revenue_by_country", date_params(start, end))

    def customer_summary(self, start, end):
        return self._query("customer_summary", date_params(start, end))

    def data_quality(self, start, end):
        return self._query("data_quality", date_params(start, end))


def _normalise_numbers(frame: pd.DataFrame) -> pd.DataFrame:
    """Convert Snowflake Decimal values to floats for charting."""
    for column in frame.columns:
        if frame[column].dtype == object and frame[column].map(lambda v: hasattr(v, "as_tuple")).any():
            frame[column] = frame[column].astype(float)
    return frame


# ============================================================
# LOCAL CSV BACKEND (same metrics in pandas)
# ============================================================


class LocalCsvSource:
    """Computes the dashboard metrics from the local transformed CSVs."""

    label = "Local CSV (offline mode)"

    def __init__(self, tables_dir: Path = LOCAL_TABLES_DIR):
        missing = [n for n in ("orders.csv", "customers.csv", "products.csv") if not (tables_dir / n).is_file()]
        if missing:
            raise ConfigurationError(
                f"Local mode needs {missing} in {tables_dir}. Run the transform and split scripts first."
            )
        self.orders = pd.read_csv(
            tables_dir / "orders.csv",
            dtype={"invoice_number": "string", "stock_code": "string", "customer_id": "Int64"},
            parse_dates=["invoice_date"],
        )
        self.orders.columns = self.orders.columns.str.upper()
        self.orders["LINE_VALUE"] = self.orders["QUANTITY"] * self.orders["UNIT_PRICE"]
        self.customers = pd.read_csv(tables_dir / "customers.csv", dtype={"customer_id": "Int64"})
        self.customers.columns = self.customers.columns.str.upper()
        self.products = pd.read_csv(tables_dir / "products.csv", dtype={"stock_code": "string"})
        self.products.columns = self.products.columns.str.upper()

    def describe(self) -> str:
        return f"{LOCAL_TABLES_DIR.relative_to(PROJECT_ROOT).as_posix()}/*.csv"

    def _range(self, start, end) -> pd.DataFrame:
        params = date_params(start, end)
        dates = self.orders["INVOICE_DATE"]
        return self.orders[(dates >= params["start_date"]) & (dates < params["end_date"])]

    def kpis(self, start, end):
        o = self._range(start, end)
        regular = o[o["IS_REGULAR_SALE"]]
        cancelled = o[o["IS_CANCELLATION"]]
        return pd.DataFrame([{
            "TOTAL_LINES": len(o),
            "TOTAL_INVOICES": o["INVOICE_NUMBER"].nunique(),
            "REGULAR_SALE_LINES": len(regular),
            "SALES_ORDERS": regular["INVOICE_NUMBER"].nunique(),
            "REVENUE": float(regular["LINE_VALUE"].sum()),
            "UNITS_SOLD": int(regular["QUANTITY"].sum()),
            "ACTIVE_CUSTOMERS": regular["CUSTOMER_ID"].nunique(),
            "CANCELLED_INVOICES": cancelled["INVOICE_NUMBER"].nunique(),
            "CANCELLED_VALUE": float(-cancelled["LINE_VALUE"].sum()),
            "REVENUE_MISSING_CUSTOMER": float(regular.loc[regular["IS_MISSING_CUSTOMER"], "LINE_VALUE"].sum()),
        }])

    def monthly_revenue(self, start, end):
        regular = self._range(start, end).query("IS_REGULAR_SALE")
        month = regular["INVOICE_DATE"].dt.to_period("M").dt.to_timestamp().dt.date
        out = regular.groupby(month).agg(REVENUE=("LINE_VALUE", "sum"), ORDERS=("INVOICE_NUMBER", "nunique"))
        return out.rename_axis("MONTH").reset_index()

    def top_products(self, start, end, limit=10, rank_by_net=False):
        o = self._range(start, end)
        o = o[o["IS_REGULAR_SALE"] | o["IS_CANCELLATION"]]
        codes = o["STOCK_CODE"]
        o = o[~codes.isin(SERVICE_STOCK_CODES) & ~codes.str.startswith(SERVICE_STOCK_PREFIX)].copy()
        regular = o["IS_REGULAR_SALE"]
        o["REVENUE"] = o["LINE_VALUE"].where(regular, 0.0)
        o["UNITS"] = o["QUANTITY"].where(regular, 0)
        o["SALE_INVOICE"] = o["INVOICE_NUMBER"].where(regular)
        out = o.groupby("STOCK_CODE").agg(
            REVENUE=("REVENUE", "sum"),
            NET_REVENUE=("LINE_VALUE", "sum"),
            UNITS=("UNITS", "sum"),
            ORDERS=("SALE_INVOICE", "nunique"),
        ).reset_index()
        out = out[out["ORDERS"] > 0].merge(self.products, on="STOCK_CODE", how="left")
        out["DESCRIPTION"] = out["DESCRIPTION"].fillna("(no description)")
        rank = "NET_REVENUE" if rank_by_net else "REVENUE"
        out = out.sort_values([rank, "STOCK_CODE"], ascending=[False, True]).head(int(limit))
        return out[["STOCK_CODE", "DESCRIPTION", "REVENUE", "NET_REVENUE", "UNITS", "ORDERS"]].reset_index(drop=True)

    def revenue_by_country(self, start, end):
        regular = self._range(start, end).query("IS_REGULAR_SALE")
        joined = regular.merge(self.customers, on="CUSTOMER_ID", how="left")
        joined["COUNTRY"] = joined["PRIMARY_COUNTRY"].fillna("Unidentified customer")
        out = joined.groupby("COUNTRY").agg(
            REVENUE=("LINE_VALUE", "sum"), CUSTOMERS=("CUSTOMER_ID", "nunique"), ORDERS=("INVOICE_NUMBER", "nunique")
        ).reset_index()
        return out.sort_values(["REVENUE", "COUNTRY"], ascending=[False, True]).reset_index(drop=True)

    def customer_summary(self, start, end):
        o = self._range(start, end)
        identified = o[o["IS_REGULAR_SALE"] & ~o["IS_MISSING_CUSTOMER"]]
        return identified.groupby("CUSTOMER_ID").agg(
            ORDERS=("INVOICE_NUMBER", "nunique"),
            REVENUE=("LINE_VALUE", "sum"),
            FIRST_PURCHASE=("INVOICE_DATE", "min"),
            LAST_PURCHASE=("INVOICE_DATE", "max"),
        ).reset_index()

    def data_quality(self, start, end):
        o = self._range(start, end)
        return pd.DataFrame([{
            "TOTAL_LINES": len(o),
            "REGULAR_SALE": int(o["IS_REGULAR_SALE"].sum()),
            "CANCELLATION": int(o["IS_CANCELLATION"].sum()),
            "OTHER_NEGATIVE_ADJUSTMENT": int(o["IS_OTHER_NEGATIVE_ADJUSTMENT"].sum()),
            "NONPOSITIVE_PRICE": int(o["IS_NONPOSITIVE_PRICE"].sum()),
            "MISSING_CUSTOMER": int(o["IS_MISSING_CUSTOMER"].sum()),
            "NAIVE_NET_VALUE": float(o["LINE_VALUE"].sum()),
        }])


# ============================================================
# FACTORY
# ============================================================


def create_source(mode: str | None = None):
    """Create the backend selected by ``mode`` or DASHBOARD_DATA_SOURCE.

    Raises:
        ConfigurationError: For an unknown mode or missing settings.
    """
    mode = (mode or os.environ.get("DASHBOARD_DATA_SOURCE") or "snowflake").strip().lower()
    if mode == "snowflake":
        return SnowflakeSource(SnowflakeSettings.from_env())
    if mode == "local":
        return LocalCsvSource()
    raise ConfigurationError(f"Unknown DASHBOARD_DATA_SOURCE '{mode}'. Use 'snowflake' or 'local'.")
