"""
Transform the raw Online Retail CSV into a clean, typed dataset.

Input:   assets/datasets/Original_file/data.csv            (cp1252)
Output:  assets/datasets/Transformed_full/transformed_data.csv (utf-8)

Transformations:
    - Rename source columns to snake_case.
    - Cast CustomerID to a nullable integer (Int64) - the raw file stores
      it as a float because of missing values (17850.0 -> 17850).
    - Parse InvoiceDate from ``m/d/YYYY H:MM`` to a timestamp.
    - Add five boolean data-quality flags (see ``add_quality_flags``).

No rows are removed: every one of the 541,909 source rows is kept and
classified with flags, so downstream consumers decide what to exclude.

Usage:
    python python_scripts/transform_data_script.py
    python python_scripts/transform_data_script.py --input raw.csv --output out.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

# Paths are resolved from the project root so the script works both
# locally and inside the Airflow containers (/opt/airflow).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "assets" / "datasets" / "Original_file" / "data.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "assets" / "datasets" / "Transformed_full" / "transformed_data.csv"

# The raw file was exported from Excel on Windows; it is not valid UTF-8.
SOURCE_ENCODING = "cp1252"
SOURCE_DATE_FORMAT = "%m/%d/%Y %H:%M"

COLUMN_MAPPING = {
    "InvoiceNo": "invoice_number",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}

QUALITY_FLAGS = [
    "is_cancellation",
    "is_other_negative_adjustment",
    "is_nonpositive_price",
    "is_regular_sale",
    "is_missing_customer",
]


def load_raw_data(path: Path) -> pd.DataFrame:
    """Read the raw retail CSV using its original Windows encoding."""
    if not Path(path).is_file():
        raise FileNotFoundError(f"Raw dataset not found: {path}")
    return pd.read_csv(path, encoding=SOURCE_ENCODING)


def standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns and convert data types.

    Raises:
        ValueError: If an expected source column is missing.
    """
    missing = set(COLUMN_MAPPING) - set(df.columns)
    if missing:
        raise ValueError(f"Raw dataset is missing columns: {sorted(missing)}")

    df = df.rename(columns=COLUMN_MAPPING)
    df["customer_id"] = df["customer_id"].astype("Int64")
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], format=SOURCE_DATE_FORMAT)
    return df


def add_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Add boolean data-quality flags used by validation and analytics.

    is_cancellation:
        Invoice number starts with "C" (a credit note / cancelled order).
    is_other_negative_adjustment:
        Negative quantity that is NOT a cancellation (stock write-offs,
        damages, manual adjustments).
    is_nonpositive_price:
        Unit price is zero or negative (free items, bad-debt adjustments).
    is_regular_sale:
        Positive quantity, positive price and not a cancellation. This is
        the only flag that should be used to calculate sales revenue.
    is_missing_customer:
        CustomerID is null (guest checkout / unidentified customer).
    """
    df["is_cancellation"] = df["invoice_number"].str.startswith("C")
    df["is_other_negative_adjustment"] = (df["quantity"] < 0) & (~df["is_cancellation"])
    df["is_nonpositive_price"] = df["unit_price"] <= 0
    df["is_regular_sale"] = (df["quantity"] > 0) & (df["unit_price"] > 0) & (~df["is_cancellation"])
    df["is_missing_customer"] = df["customer_id"].isna()
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the full transformation to a raw DataFrame."""
    return add_quality_flags(standardise_columns(df))


def print_summary(df: pd.DataFrame) -> None:
    """Print a short profile that appears in the Airflow task log."""
    print("Shape:", df.shape)
    print("Columns:", df.columns.tolist())
    print("Data types:")
    print(df.dtypes)
    for flag in QUALITY_FLAGS:
        print(f"{flag}: {int(df[flag].sum())}")
    print("Total rows:", len(df))


def parse_arguments() -> argparse.Namespace:
    """Read optional input/output paths from the command line."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Raw CSV path")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Transformed CSV path")
    return parser.parse_args()


def main() -> None:
    """Run the transformation end to end."""
    arguments = parse_arguments()

    df = transform(load_raw_data(arguments.input))
    print_summary(df)

    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(arguments.output, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {arguments.output}")


if __name__ == "__main__":
    main()
