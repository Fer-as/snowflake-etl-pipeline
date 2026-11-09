import pandas as pd

# 1. Read transformed dataset
df = pd.read_csv(
    'assets/datasets/Transformed_full/transformed_data.csv',
    encoding='utf-8',
    dtype={
        'customer_id': 'Int64',
        'stock_code': 'string',
        'invoice_number': 'string'
    }
)

# 2. Customers DataFrame
customer_df = df[['customer_id', 'country']].copy()
customer_df = customer_df.dropna(subset=['customer_id'])

customer_df = customer_df.groupby('customer_id')['country'].agg(
    lambda countries: countries.mode().iloc[0]
    if not countries.mode().empty else pd.NA
).reset_index()

customer_df = customer_df.rename(
    columns={'country': 'primary_country'}
)


# 3. Products DataFrame
products_df = df[['stock_code', 'description']].copy()
products_df = products_df.dropna(subset=['stock_code'])

products_df = products_df.groupby('stock_code')['description'].agg(
    lambda descriptions: descriptions.mode().iloc[0]
    if not descriptions.mode().empty else pd.NA
).reset_index()


# 4. Orders DataFrame
orders_df = df[[
    'invoice_number',
    'stock_code',
    'customer_id',
    'invoice_date',
    'quantity',
    'unit_price',
    'is_cancellation',
    'is_other_negative_adjustment',
    'is_nonpositive_price',
    'is_regular_sale',
    'is_missing_customer'
]].copy()

orders_df.insert(
    0,
    'order_line_id',
    range(1, len(orders_df) + 1)
)


# 5. Validation
print("\nCUSTOMERS")
print("Shape:", customer_df.shape)
print("Duplicate IDs:", customer_df['customer_id'].duplicated().sum())

print("\nPRODUCTS")
print("Shape:", products_df.shape)
print("Duplicate codes:", products_df['stock_code'].duplicated().sum())
print("Missing descriptions:", products_df['description'].isna().sum())

print("\nORDERS")
print("Shape:", orders_df.shape)
print("Duplicate line IDs:", orders_df['order_line_id'].duplicated().sum())
print("Missing customer IDs:", orders_df['customer_id'].isna().sum())

customer_df.to_csv(
    'assets/datasets/Transformed_tables/customers.csv',
    index=False,
    encoding='utf-8'
)

products_df.to_csv(
    'assets/datasets/Transformed_tables/products.csv',
    index=False,
    encoding='utf-8'
)

orders_df.to_csv(
    'assets/datasets/Transformed_tables/orders.csv',
    index=False,
    encoding='utf-8'
)