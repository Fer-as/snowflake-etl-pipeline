import pandas as pd

df = pd.read_csv('assets/datasets/Original_file/data.csv',
                 encoding='cp1252')

df.rename(columns={
    'InvoiceNo': 'invoice_number',
    'StockCode': 'stock_code',
    'Description': 'description',
    'Quantity': 'quantity',
    'InvoiceDate': 'invoice_date',
    'UnitPrice': 'unit_price',
    'CustomerID': 'customer_id',
    'Country': 'country'
}, inplace=True)

df['customer_id'] = df['customer_id'].astype('Int64')

df['invoice_date'] = pd.to_datetime(df['invoice_date'], format='%m/%d/%Y %H:%M')

# Identify cancellation transactions
df['is_cancellation'] = (
    df['invoice_number'].str.startswith('C')
)

# Identify negative adjustments that are not cancellations
df['is_other_negative_adjustment'] = (
    (df['quantity'] < 0)
    & (~df['is_cancellation'])
)

# Identify zero or negative prices
df['is_nonpositive_price'] = (
    df['unit_price'] <= 0
)

# Identify regular sales
df['is_regular_sale'] = (
    (df['quantity'] > 0)
    & (df['unit_price'] > 0)
    & (~df['is_cancellation'])
)

df['is_missing_customer'] = (
    df['customer_id'].isna()
)

print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print("Data types:")
print(df.dtypes)
print(df.head())

print("Cancellations:", df['is_cancellation'].sum())
print("Other negative adjustments:", df['is_other_negative_adjustment'].sum())
print("Non-positive prices:", df['is_nonpositive_price'].sum())
print("Regular sales:", df['is_regular_sale'].sum())
print("Total rows:", len(df))

print("Customer ID type:", df['customer_id'].dtype)
print("Missing Customer IDs:", df['customer_id'].isna().sum())

print("Missing customer flags:", df['is_missing_customer'].sum())

df.to_csv(
    'assets/datasets/Transformed_full/transformed_data.csv',
    index=False,
    encoding='utf-8'
)