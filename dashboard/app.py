"""
Retail Sales Dashboard (Streamlit).

Reads the validated final tables loaded by the Airflow full-refresh DAG
(RETAIL_ETL_DB.RETAIL.ORDERS / CUSTOMERS / PRODUCTS) and presents sales,
product, country, customer and data-quality metrics.

Run:
    pip install -r dashboard/requirements.txt
    streamlit run dashboard/app.py

Configuration: see dashboard/README.md (SNOWFLAKE_* environment variables,
or DASHBOARD_DATA_SOURCE=local to run from the local CSVs).

Revenue rule used everywhere: SUM(QUANTITY * UNIT_PRICE) over rows where
IS_REGULAR_SALE is TRUE. Cancellations are reported separately and never
netted off; adjustments and non-positive prices are excluded.
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

try:  # Optional: load dashboard/.env for local development.
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

from data_sources import ConfigurationError, DataSourceError, create_source

# Dataset coverage (first and last invoice in the source file).
DATA_START = date(2010, 12, 1)
DATA_END = date(2011, 12, 9)

# Single-series charts use one categorical slot (blue); identity is never
# encoded by colour alone because every chart has one series.
SERIES_COLOR = "#2a78d6"
CURRENCY = "£"

st.set_page_config(page_title="Retail Sales Dashboard", page_icon="📊", layout="wide")


# ============================================================
# DATA LOADING (cached)
# ============================================================


@st.cache_resource(show_spinner=False)
def get_source(mode: str):
    """One backend (and Snowflake connection) per data-source mode."""
    return create_source(mode)


@st.cache_data(ttl=600, show_spinner="Querying data…")
def run(mode: str, method: str, start: date, end: date, **kwargs) -> pd.DataFrame:
    """Run one metric query; results are cached for 10 minutes."""
    return getattr(get_source(mode), method)(start, end, **kwargs)


def money(value: float) -> str:
    return f"{CURRENCY}{value:,.0f}"


def compact_money(value: float) -> str:
    """Short form for KPI tiles (e.g. £10.67M) so values never truncate."""
    for threshold, suffix in ((1e6, "M"), (1e3, "k")):
        if abs(value) >= threshold:
            return f"{CURRENCY}{value / threshold:,.2f}{suffix}"
    return money(value)


def style_figure(fig, height: int = 380):
    """Recessive axes/grid, thin marks, consistent hover formatting."""
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=8, b=8),
        hovermode="closest",
        showlegend=False,
        xaxis_title=None,
        yaxis_title=None,
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridwidth=1)
    return fig


def bar(df, x, y, hover, orientation="h", height=380):
    """Single-series bar chart with 4px rounded data ends."""
    fig = px.bar(df, x=x, y=y, orientation=orientation, custom_data=hover)
    fig.update_traces(marker_color=SERIES_COLOR, marker_cornerradius=4)
    if orientation == "h":
        fig.update_yaxes(autorange="reversed")
    return style_figure(fig, height)


# ============================================================
# SIDEBAR: data source and filters
# ============================================================

st.sidebar.title("Filters")

default_mode = os.environ.get("DASHBOARD_DATA_SOURCE", "snowflake").lower()
mode_labels = {"snowflake": "Snowflake (live)", "local": "Local CSV (offline)"}
mode = st.sidebar.radio(
    "Data source",
    options=list(mode_labels),
    format_func=mode_labels.get,
    index=0 if default_mode != "local" else 1,
    help="Local mode computes identical metrics from assets/datasets/Transformed_tables/*.csv.",
)

date_range = st.sidebar.date_input(
    "Invoice date range", value=(DATA_START, DATA_END), min_value=DATA_START, max_value=DATA_END
)
if not isinstance(date_range, (tuple, list)) or len(date_range) != 2:
    st.sidebar.info("Select both a start and an end date.")
    st.stop()
start, end = date_range

top_n = st.sidebar.slider("Top products / countries", min_value=5, max_value=25, value=10)
exclude_uk = st.sidebar.checkbox(
    "Exclude United Kingdom from country chart",
    value=True,
    help="The UK is the home market (about 82% of attributable revenue) and dwarfs other countries.",
)
rank_by_net = st.sidebar.toggle(
    "Rank products net of cancellations",
    value=False,
    help="Gross ranks by regular-sale revenue; net subtracts cancelled lines of the same product.",
)
if st.sidebar.button("Refresh data", help="Clear the 10-minute query cache"):
    st.cache_data.clear()


# ============================================================
# CONNECT (with clear error handling)
# ============================================================

st.title("Retail Sales Dashboard")

try:
    source = get_source(mode)
    kpis = run(mode, "kpis", start, end).iloc[0]
except ConfigurationError as error:
    st.error(f"**Configuration problem:** {error}")
    st.markdown(
        "Set the Snowflake variables (see `dashboard/README.md`), for example in `dashboard/.env`:"
    )
    st.code(
        "SNOWFLAKE_ACCOUNT=<orgname-accountname>\n"
        "SNOWFLAKE_USER=<user>\n"
        "SNOWFLAKE_TOKEN=<programmatic access token>   # or SNOWFLAKE_PASSWORD / key pair\n"
        "SNOWFLAKE_ROLE=<role with SELECT on RETAIL_ETL_DB.RETAIL>",
        language="bash",
    )
    st.info("No Snowflake access? Choose **Local CSV (offline)** in the sidebar.")
    get_source.clear()
    st.stop()
except DataSourceError as error:
    st.error(f"**Could not load data:** {error}")
    st.info("Check the account identifier, network access, warehouse and role, then press *Refresh data*.")
    get_source.clear()
    st.stop()

st.caption(
    f"Source: **{source.label}** · {source.describe()} · "
    f"{start:%d %b %Y} – {end:%d %b %Y} · Revenue = regular sales only"
)

if int(kpis["TOTAL_LINES"]) == 0:
    st.warning("No transactions in the selected date range.")
    st.stop()


# ============================================================
# KPI ROW
# ============================================================

orders = int(kpis["SALES_ORDERS"])
revenue = float(kpis["REVENUE"])

cancelled_value = float(kpis["CANCELLED_VALUE"])

cols = st.columns(6)
cols[0].metric("Revenue", compact_money(revenue), help=f"Regular sales only. Exact: {money(revenue)}")
cols[1].metric("Sales orders", f"{orders:,}", help="Distinct invoices with at least one regular-sale line")
cols[2].metric("Avg order value", money(revenue / orders) if orders else "–")
cols[3].metric("Active customers", f"{int(kpis['ACTIVE_CUSTOMERS']):,}", help="Identified customers with a regular sale")
cols[4].metric(
    "Cancelled value",
    compact_money(cancelled_value),
    help=f"{money(cancelled_value)} across {int(kpis['CANCELLED_INVOICES']):,} cancelled invoices, reported separately from revenue",
)
cols[5].metric(
    "Net of cancellations",
    compact_money(revenue - cancelled_value),
    help=f"Exact: {money(revenue - cancelled_value)}. Revenue minus cancelled value. Approximate: a cancellation can relate to a sale outside the range.",
)

tab_trend, tab_products, tab_countries, tab_customers, tab_quality, tab_method = st.tabs(
    ["Revenue trend", "Top products", "Countries", "Customers", "Data quality", "How the numbers work"]
)


# ============================================================
# TAB: REVENUE TREND
# ============================================================

with tab_trend:
    monthly = run(mode, "monthly_revenue", start, end)
    monthly["MONTH"] = pd.to_datetime(monthly["MONTH"])
    fig = px.line(monthly, x="MONTH", y="REVENUE", markers=True, custom_data=["ORDERS"])
    fig.update_traces(
        line=dict(color=SERIES_COLOR, width=2),
        marker=dict(size=8, color=SERIES_COLOR),
        hovertemplate="%{x|%b %Y}<br>Revenue: " + CURRENCY + "%{y:,.0f}<br>Orders: %{customdata[0]:,}<extra></extra>",
    )
    fig.update_yaxes(tickprefix=CURRENCY, rangemode="tozero")
    st.subheader("Monthly revenue")
    st.plotly_chart(style_figure(fig).update_layout(hovermode="x"), width="stretch")
    if end >= DATA_END:
        st.caption("December 2011 is partial: the dataset ends on 9 Dec 2011.")
    with st.expander("Table view"):
        st.dataframe(
            monthly.assign(MONTH=monthly["MONTH"].dt.strftime("%Y-%m")),
            hide_index=True,
            column_config={"REVENUE": st.column_config.NumberColumn(format="£%.2f")},
        )


# ============================================================
# TAB: TOP PRODUCTS
# ============================================================

with tab_products:
    products = run(mode, "top_products", start, end, limit=top_n, rank_by_net=rank_by_net)
    measure = "NET_REVENUE" if rank_by_net else "REVENUE"
    st.subheader(f"Top {top_n} products by {'net' if rank_by_net else 'gross'} revenue")
    if products.empty:
        st.info("No product sales in this range.")
    else:
        products["LABEL"] = products["DESCRIPTION"].str.slice(0, 40) + " (" + products["STOCK_CODE"] + ")"
        fig = bar(products, measure, "LABEL", ["UNITS", "ORDERS", "REVENUE", "NET_REVENUE"],
                  height=max(320, 32 * len(products)))
        fig.update_traces(
            hovertemplate="%{y}<br>Gross: " + CURRENCY + "%{customdata[2]:,.0f}<br>Net: " + CURRENCY
            + "%{customdata[3]:,.0f}<br>Units: %{customdata[0]:,}<br>Orders: %{customdata[1]:,}<extra></extra>"
        )
        fig.update_xaxes(tickprefix=CURRENCY)
        st.plotly_chart(fig, width="stretch")
        st.caption(
            "Postage, carriage, manual lines, fees, bad-debt adjustments and gift vouchers are excluded. "
            "Gross ranking includes orders that were later cancelled. For example, 80,995 × PAPER CRAFT, "
            "LITTLE BIRDIE was ordered and cancelled on 9 Dec 2011. Switch to net ranking in the sidebar."
        )
        with st.expander("Table view"):
            st.dataframe(products.drop(columns="LABEL"), hide_index=True)


# ============================================================
# TAB: COUNTRIES
# ============================================================

with tab_countries:
    countries = run(mode, "revenue_by_country", start, end)
    unidentified = countries[countries["COUNTRY"] == "Unidentified customer"]
    known = countries[countries["COUNTRY"] != "Unidentified customer"]
    attributable = known["REVENUE"].sum()
    uk_share = known.loc[known["COUNTRY"] == "United Kingdom", "REVENUE"].sum() / attributable if attributable else 0

    st.subheader("Revenue by customer country")
    c1, c2, c3 = st.columns(3)
    c1.metric("Countries", f"{len(known):,}")
    c2.metric("UK share of attributable revenue", f"{uk_share:.1%}")
    c3.metric(
        "Revenue with no customer/country",
        money(float(unidentified["REVENUE"].sum())),
        help="Lines without a CustomerID cannot be linked to a country",
    )

    chart_data = known[known["COUNTRY"] != "United Kingdom"] if exclude_uk else known
    chart_data = chart_data.head(top_n)
    fig = bar(chart_data, "REVENUE", "COUNTRY", ["CUSTOMERS", "ORDERS"], height=max(320, 30 * len(chart_data)))
    fig.update_traces(
        hovertemplate="%{y}<br>Revenue: " + CURRENCY + "%{x:,.0f}<br>Customers: %{customdata[0]:,}"
        "<br>Orders: %{customdata[1]:,}<extra></extra>"
    )
    fig.update_xaxes(tickprefix=CURRENCY)
    st.plotly_chart(fig, width="stretch")
    st.caption(
        "Country = the customer's most frequent country (CUSTOMERS.PRIMARY_COUNTRY). "
        + ("United Kingdom hidden (sidebar toggle). " if exclude_uk else "")
    )
    with st.expander("Table view (all countries)"):
        st.dataframe(countries, hide_index=True)


# ============================================================
# TAB: CUSTOMERS
# ============================================================

with tab_customers:
    customers = run(mode, "customer_summary", start, end)
    st.subheader("Customer statistics (identified customers only)")
    if customers.empty:
        st.info("No identified customers in this range.")
    else:
        repeat_share = (customers["ORDERS"] >= 2).mean()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Customers", f"{len(customers):,}")
        c2.metric("Repeat customers", f"{repeat_share:.1%}", help="Placed 2 or more orders in the range")
        c3.metric("Median orders / customer", f"{customers['ORDERS'].median():.0f}")
        c4.metric("Median revenue / customer", money(float(customers["REVENUE"].median())))

        buckets = pd.cut(
            customers["ORDERS"], bins=[0, 1, 2, 5, 10, float("inf")], labels=["1", "2", "3–5", "6–10", "11+"]
        )
        distribution = buckets.value_counts(sort=False).rename_axis("ORDERS_PLACED").reset_index(name="CUSTOMERS")
        distribution["ORDERS_PLACED"] = distribution["ORDERS_PLACED"].astype(str)  # keeps bucket order
        fig = bar(distribution, "ORDERS_PLACED", "CUSTOMERS", ["CUSTOMERS"], orientation="v", height=320)
        fig.update_traces(hovertemplate="%{x} orders<br>Customers: %{y:,}<extra></extra>")
        # Bucket labels such as "1" and "2" must not be parsed as numbers.
        fig.update_xaxes(type="category", title_text="Orders placed")
        st.markdown("**Customers by number of orders placed**")
        st.plotly_chart(fig, width="stretch")

        st.markdown("**Top 10 customers by revenue**")
        top_customers = customers.nlargest(10, "REVENUE")[["CUSTOMER_ID", "ORDERS", "REVENUE", "LAST_PURCHASE"]]
        st.dataframe(top_customers, hide_index=True, column_config={"REVENUE": st.column_config.NumberColumn(format="£%.2f")})


# ============================================================
# TAB: DATA QUALITY
# ============================================================

with tab_quality:
    quality = run(mode, "data_quality", start, end).iloc[0]
    total = int(quality["TOTAL_LINES"])
    flags = pd.DataFrame(
        {
            "FLAG": ["Regular sale", "Missing customer", "Cancellation", "Non-positive price", "Other negative adjustment"],
            "LINES": [int(quality[k]) for k in
                      ("REGULAR_SALE", "MISSING_CUSTOMER", "CANCELLATION", "NONPOSITIVE_PRICE", "OTHER_NEGATIVE_ADJUSTMENT")],
        }
    )
    flags["SHARE"] = flags["LINES"] / total

    st.subheader("Data-quality flags")
    st.caption(f"{total:,} order lines in range. Flags overlap, so shares do not sum to 100%.")
    fig = bar(flags, "LINES", "FLAG", ["SHARE"], height=300)
    fig.update_traces(hovertemplate="%{y}<br>Lines: %{x:,}<br>Share: %{customdata[0]:.1%}<extra></extra>")
    st.plotly_chart(fig, width="stretch")
    st.dataframe(flags, hide_index=True, column_config={
        "SHARE": st.column_config.NumberColumn(format="percent"),
        "LINES": st.column_config.NumberColumn(format="localized"),
    })

    st.markdown("**Why revenue filters on `IS_REGULAR_SALE`**")
    st.dataframe(
        pd.DataFrame(
            {
                "Definition": [
                    "Regular-sale revenue (used on this page)",
                    "Naive SUM(QUANTITY × UNIT_PRICE) over all lines",
                    "Cancelled value (reported separately)",
                    "Regular-sale revenue with no customer ID",
                ],
                "Value": [revenue, float(quality["NAIVE_NET_VALUE"]), float(kpis["CANCELLED_VALUE"]),
                          float(kpis["REVENUE_MISSING_CUSTOMER"])],
            }
        ),
        hide_index=True,
        column_config={"Value": st.column_config.NumberColumn(format="£%.2f")},
    )


# ============================================================
# TAB: METHODOLOGY
# ============================================================

with tab_method:
    st.markdown(
        """
**Revenue** = `SUM(QUANTITY * UNIT_PRICE)` where `IS_REGULAR_SALE` is TRUE
(quantity > 0, unit price > 0, invoice not starting with `C`).

| Record type | Effect on the numbers |
|---|---|
| **Cancellations** (`C…` invoices) | Excluded from revenue and shown separately as *Cancelled value*. *Net of cancellations* subtracts them, but only approximately: a cancellation can reverse a sale from before the selected range. |
| **Other negative adjustments** | Excluded. All have price 0, so they carry no value. |
| **Non-positive prices** (free items, *Adjust bad debt*) | Excluded from revenue. A negative bad-debt line would otherwise reduce sales. |
| **Missing customer** (~25% of lines) | **Included** in revenue, orders and products. Excluded from customer statistics and shown as *Revenue with no customer/country* on the Countries tab. |
| **Service codes** (POST, DOT, M, fees, gift vouchers) | Included in revenue (they were invoiced). Excluded from *Top products*. |

**No double counting:** every query aggregates `ORDERS` at line level. Joins to
`CUSTOMERS` and `PRODUCTS` are on their unique keys (validated by the Airflow
DAG), so a join can never multiply a line. *Orders* = distinct invoice numbers.

SQL for every chart: `dashboard/sql/*.sql`. Lineage: `docs/lineage.md`.
"""
    )
