"""
Split the transformed dataset into customers, products and orders tables.

Input:   assets/datasets/Transformed_full/transformed_data.csv
Outputs: assets/datasets/Transformed_tables/customers.csv
         assets/datasets/Transformed_tables/products.csv
         assets/datasets/Transformed_tables/orders.csv

Table design (a simple star-like model for analytical queries):

    CUSTOMERS  one row per customer_id
               primary_country = most frequent country for that customer
    PRODUCTS   one row per stock_code
               description = most frequent description for that code
    ORDERS     one row per source line item (all 541,909 rows)
               order_line_id = generated 1..N surrogate key, based on the
               row position in the source file

Rows without a customer_id are kept in ORDERS (flagged with
is_missing_customer) but cannot appear in CUSTOMERS.

Usage:
    python python_scripts/split_data_into_tables.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "assets" / "datasets" / "Transformed_full" / "transformed_data.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "assets" / "datasets" / "Transformed_tables"

ORDER_COLUMNS = [
    "invoice_number",
    "stock_code",
    "customer_id",
    "invoice_date",
    "quantity",
    "unit_price",
    "is_cancellation",
    "is_other_negative_adjustment",
    "is_nonpositive_price",
    "is_regular_sale",
    "is_missing_customer",
]


def _most_frequent(values: pd.Series):
    """Return the most frequent value (mode); ties resolve to the smallest."""
    modes = values.mode()
    return modes.iloc[0] if not modes.empty else pd.NA


def load_transformed_data(path: Path) -> pd.DataFrame:
    """Read the transformed CSV, preserving identifier types.

    stock_code and invoice_number are read as strings so codes such as
    "85123A" and cancellation invoices such as "C536379" are preserved.
    """
    if not Path(path).is_file():
        raise FileNotFoundError(f"Transformed dataset not found: {path}")
    return pd.read_csv(
        path,
        encoding="utf-8",
        dtype={"customer_id": "Int64", "stock_code": "string", "invoice_number": "string"},
    )


def build_customers(df: pd.DataFrame) -> pd.DataFrame:
    """One row per customer with their most frequent country."""
    customers = df[["customer_id", "country"]].dropna(subset=["customer_id"])
    customers = customers.groupby("customer_id")["country"].agg(_most_frequent).reset_index()
    return customers.rename(columns={"country": "primary_country"})


def build_products(df: pd.DataFrame) -> pd.DataFrame:
    """One row per stock code with its most frequent description."""
    products = df[["stock_code", "description"]].dropna(subset=["stock_code"])
    return products.groupby("stock_code")["description"].agg(_most_frequent).reset_index()


def build_orders(df: pd.DataFrame) -> pd.DataFrame:
    """All line items plus a generated, position-based order_line_id."""
    orders = df[ORDER_COLUMNS].copy()
    orders.insert(0, "order_line_id", range(1, len(orders) + 1))
    return orders


def print_validation(customers: pd.DataFrame, products: pd.DataFrame, orders: pd.DataFrame) -> None:
    """Print key uniqueness checks to the Airflow task log."""
    print("\nCUSTOMERS")
    print("Shape:", customers.shape)
    print("Duplicate IDs:", customers["customer_id"].duplicated().sum())

    print("\nPRODUCTS")
    print("Shape:", products.shape)
    print("Duplicate codes:", products["stock_code"].duplicated().sum())
    print("Missing descriptions:", products["description"].isna().sum())

    print("\nORDERS")
    print("Shape:", orders.shape)
    print("Duplicate line IDs:", orders["order_line_id"].duplicated().sum())
    print("Missing customer IDs:", orders["customer_id"].isna().sum())


def parse_arguments() -> argparse.Namespace:
    """Read optional input path and output directory."""
    parser = argparse.ArgumentParser(description="Split transformed data into tables.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Transformed CSV path")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory")
    return parser.parse_args()


def main() -> None:
    """Build and write the three tables."""
    arguments = parse_arguments()
    df = load_transformed_data(arguments.input)

    tables = {
        "customers": build_customers(df),
        "products": build_products(df),
        "orders": build_orders(df),
    }
    print_validation(tables["customers"], tables["products"], tables["orders"])

    arguments.output_dir.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        table.to_csv(arguments.output_dir / f"{name}.csv", index=False, encoding="utf-8")
        print(f"Wrote {len(table)} rows to {name}.csv")


if __name__ == "__main__":
    main()
