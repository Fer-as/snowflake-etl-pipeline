"""
Unit tests for the pandas transformation and splitting logic.

Small hand-built DataFrames cover each data-quality rule. An optional
full-dataset test runs only when the 45 MB source CSV is present locally.
"""

from pathlib import Path

import pandas as pd
import pytest

import split_data_into_tables as split
import transform_data_script as transform

RAW_ROWS = [
    # InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
    ("536365", "85123A", "HEART HOLDER", 6, "12/1/2010 8:26", 2.55, 17850.0, "United Kingdom"),  # regular sale
    ("C536379", "D", "Discount", -1, "12/1/2010 9:41", 27.50, 14527.0, "United Kingdom"),  # cancellation
    ("536414", "22139", None, -56, "12/1/2010 11:52", 0.0, None, "United Kingdom"),  # adjustment, 0 price, no customer
    ("536544", "21773", "BOX", 1, "12/1/2010 14:32", 2.51, None, "United Kingdom"),  # sale, missing customer
]


@pytest.fixture
def raw_df():
    columns = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    return pd.DataFrame(RAW_ROWS, columns=columns)


@pytest.fixture
def transformed(raw_df):
    return transform.transform(raw_df)


def test_columns_are_renamed_and_typed(transformed):
    assert list(transformed.columns[:8]) == list(transform.COLUMN_MAPPING.values())
    assert str(transformed["customer_id"].dtype) == "Int64"
    assert transformed.loc[0, "customer_id"] == 17850
    assert transformed.loc[0, "invoice_date"] == pd.Timestamp("2010-12-01 08:26")


def test_quality_flags(transformed):
    flags = transformed[transform.QUALITY_FLAGS].astype(bool).values.tolist()
    #           cancel  other_neg nonpos regular missing_cust
    assert flags[0] == [False, False, False, True, False]
    assert flags[1] == [True, False, False, False, False]
    assert flags[2] == [False, True, True, False, True]
    assert flags[3] == [False, False, False, True, True]


def test_no_rows_are_dropped(raw_df, transformed):
    assert len(transformed) == len(raw_df)


def test_missing_source_column_raises(raw_df):
    with pytest.raises(ValueError, match="missing columns"):
        transform.standardise_columns(raw_df.drop(columns=["UnitPrice"]))


def test_missing_input_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        transform.load_raw_data(tmp_path / "nope.csv")


def test_customers_use_most_frequent_country():
    df = pd.DataFrame(
        {
            "customer_id": pd.array([1, 1, 1, 2, None], dtype="Int64"),
            "country": ["France", "France", "Spain", "EIRE", "Germany"],
        }
    )
    customers = split.build_customers(df)

    assert customers.to_dict("records") == [
        {"customer_id": 1, "primary_country": "France"},
        {"customer_id": 2, "primary_country": "EIRE"},
    ]


def test_products_use_most_frequent_description():
    df = pd.DataFrame({"stock_code": ["A", "A", "A", "B"], "description": ["X", "Y", "X", None]})
    products = split.build_products(df).set_index("stock_code")["description"]

    assert products["A"] == "X"
    assert pd.isna(products["B"])  # a code with no description is kept, flagged as null


def test_orders_get_sequential_line_ids(transformed):
    orders = split.build_orders(transformed)

    assert orders["order_line_id"].tolist() == [1, 2, 3, 4]
    assert list(orders.columns) == ["order_line_id"] + split.ORDER_COLUMNS


FULL_DATASET = Path(transform.DEFAULT_INPUT)


@pytest.mark.skipif(not FULL_DATASET.is_file(), reason="Full source dataset not available")
def test_full_dataset_profile():
    """Counts used by the Airflow validation tasks and the documentation."""
    df = transform.transform(transform.load_raw_data(FULL_DATASET))

    assert len(df) == 541_909
    assert int(df["is_cancellation"].sum()) == 9_288
    assert int(df["is_other_negative_adjustment"].sum()) == 1_336
    assert int(df["is_nonpositive_price"].sum()) == 2_517
    assert int(df["is_regular_sale"].sum()) == 530_104
    assert int(df["is_missing_customer"].sum()) == 135_080
    assert len(split.build_customers(df)) == 4_372
    assert len(split.build_products(df)) == 4_070
