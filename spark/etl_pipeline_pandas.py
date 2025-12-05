"""
Simplified ETL Pipeline - Pandas Version (No Spark Required)
Processes data using Pandas instead of Spark for compatibility
"""

import pandas as pd
import json
import os
from pathlib import Path
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark.config import Config

print("=" * 80)
print("DATA LAKE ETL PIPELINE (Pandas Version)")
print("=" * 80)
print()

# Create output directories
os.makedirs(Config.PROCESSED_PATH, exist_ok=True)
os.makedirs(Config.WAREHOUSE_PATH, exist_ok=True)

# EXTRACT
print("EXTRACT PHASE - Reading raw data")
print("-" * 80)

print(f"Reading sales data from {Config.SALES_FILE}")
sales_df = pd.read_csv(Config.SALES_FILE)
print(f"✓ Loaded {len(sales_df)} sales records")

print(f"Reading products data from {Config.PRODUCTS_FILE}")
with open(Config.PRODUCTS_FILE, 'r') as f:
    products_data = json.load(f)
products_df = pd.DataFrame(products_data)
print(f"✓ Loaded {len(products_df)} products")

print(f"Reading customers data from {Config.CUSTOMERS_FILE}")
customers_df = pd.read_csv(Config.CUSTOMERS_FILE)
print(f"✓ Loaded {len(customers_df)} customers")
print()

# TRANSFORM
print("TRANSFORM PHASE - Cleaning and transforming data")
print("-" * 80)

# Clean sales
print("Cleaning sales data...")
sales_clean = sales_df.dropna(subset=['transaction_id', 'customer_id', 'product_id'])
sales_clean = sales_clean.drop_duplicates(subset=['transaction_id'])
sales_clean['transaction_date'] = pd.to_datetime(sales_clean['transaction_date'])
sales_clean['year'] = sales_clean['transaction_date'].dt.year
sales_clean['month'] = sales_clean['transaction_date'].dt.month
print(f"✓ Cleaned {len(sales_clean)} sales records")

# Clean products
print("Cleaning products data...")
products_clean = products_df.dropna(subset=['product_id'])
products_clean = products_clean.drop_duplicates(subset=['product_id'])
products_clean['profit_margin'] = ((products_clean['unit_price'] - products_clean['cost_price']) / products_clean['unit_price'] * 100).round(2)
print(f"✓ Cleaned {len(products_clean)} products")

# Clean customers
print("Cleaning customers data...")
customers_clean = customers_df.dropna(subset=['customer_id'])
customers_clean = customers_clean.drop_duplicates(subset=['customer_id'])
customers_clean['registration_date'] = pd.to_datetime(customers_clean['registration_date'])
customers_clean['full_name'] = customers_clean['first_name'] + ' ' + customers_clean['last_name']
print(f"✓ Cleaned {len(customers_clean)} customers")
print()

# Save processed data
print("Saving processed data...")
sales_clean.to_parquet(os.path.join(Config.PROCESSED_SALES, 'sales.parquet'), index=False)
products_clean.to_parquet(os.path.join(Config.PROCESSED_PRODUCTS, 'products.parquet'), index=False)
customers_clean.to_parquet(os.path.join(Config.PROCESSED_CUSTOMERS, 'customers.parquet'), index=False)
print("✓ Processed data saved")
print()

# LOAD - Create warehouse tables
print("LOAD PHASE - Creating warehouse tables")
print("-" * 80)

# Fact table
print("Creating fact_sales table...")
fact_sales = sales_clean.merge(
    products_clean[['product_id', 'product_name', 'category']], 
    on='product_id', 
    how='left'
).merge(
    customers_clean[['customer_id', 'full_name', 'city', 'state', 'customer_segment']], 
    on='customer_id', 
    how='left'
)
fact_sales.to_parquet(os.path.join(Config.WAREHOUSE_FACT_SALES, 'fact_sales.parquet'), index=False)
print(f"✓ Created fact_sales with {len(fact_sales)} records")

# Dimension tables
print("Creating dim_products table...")
dim_products = products_clean[['product_id', 'product_name', 'category', 'subcategory', 'brand', 
                                'unit_price', 'cost_price', 'profit_margin', 'stock_quantity', 
                                'supplier', 'weight_kg', 'launch_date']]
dim_products.to_parquet(os.path.join(Config.WAREHOUSE_DIM_PRODUCTS, 'dim_products.parquet'), index=False)
print(f"✓ Created dim_products with {len(dim_products)} records")

print("Creating dim_customers table...")
dim_customers = customers_clean[['customer_id', 'full_name', 'email', 'phone', 'city', 'state', 
                                  'zip_code', 'country', 'registration_date', 'customer_segment', 
                                  'age', 'gender']]
dim_customers.to_parquet(os.path.join(Config.WAREHOUSE_DIM_CUSTOMERS, 'dim_customers.parquet'), index=False)
print(f"✓ Created dim_customers with {len(dim_customers)} records")

# Aggregations
print("Creating aggregation tables...")

# Monthly revenue
agg_monthly = fact_sales[fact_sales['order_status'] == 'Completed'].groupby(['year', 'month']).agg({
    'total_amount': 'sum',
    'quantity': 'sum',
    'transaction_id': 'count',
    'discount_amount': 'sum'
}).reset_index()
agg_monthly.columns = ['year', 'month', 'total_revenue', 'total_quantity', 'transaction_count', 'total_discounts']
agg_monthly['avg_order_value'] = agg_monthly['total_revenue'] / agg_monthly['transaction_count']
agg_monthly.to_parquet(os.path.join(Config.WAREHOUSE_AGG_MONTHLY, 'agg_monthly.parquet'), index=False)
print(f"✓ Created agg_monthly_revenue with {len(agg_monthly)} records")

# Top products
agg_products = fact_sales[fact_sales['order_status'] == 'Completed'].groupby(['product_id', 'product_name', 'category']).agg({
    'quantity': 'sum',
    'total_amount': 'sum',
    'transaction_id': 'count'
}).reset_index()
agg_products.columns = ['product_id', 'product_name', 'category', 'total_quantity_sold', 'total_revenue', 'transaction_count']
agg_products['avg_sale_amount'] = agg_products['total_revenue'] / agg_products['transaction_count']
agg_products = agg_products.sort_values('total_revenue', ascending=False)
agg_products['revenue_rank'] = range(1, len(agg_products) + 1)
agg_products.to_parquet(os.path.join(Config.WAREHOUSE_AGG_PRODUCTS, 'agg_products.parquet'), index=False)
print(f"✓ Created agg_top_products with {len(agg_products)} records")

# Regional sales
agg_regions = fact_sales[fact_sales['order_status'] == 'Completed'].groupby(['state', 'city']).agg({
    'total_amount': 'sum',
    'transaction_id': 'count',
    'customer_id': 'nunique'
}).reset_index()
agg_regions.columns = ['state', 'city', 'total_revenue', 'transaction_count', 'unique_customers']
agg_regions['avg_order_value'] = agg_regions['total_revenue'] / agg_regions['transaction_count']
agg_regions = agg_regions.sort_values('total_revenue', ascending=False)
agg_regions.to_parquet(os.path.join(Config.WAREHOUSE_AGG_REGIONS, 'agg_regions.parquet'), index=False)
print(f"✓ Created agg_regional_sales with {len(agg_regions)} records")

print()
print("=" * 80)
print("ETL PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 80)
print()
print("Pipeline Summary:")
print(f"  Processed Sales Records: {len(fact_sales):,}")
print(f"  Products in Catalog: {len(dim_products):,}")
print(f"  Customers: {len(dim_customers):,}")
print(f"  Monthly Aggregations: {len(agg_monthly):,}")
print(f"  Product Performance Records: {len(agg_products):,}")
print(f"  Regional Aggregations: {len(agg_regions):,}")
print()
print(f"  Total Revenue: ${fact_sales[fact_sales['order_status'] == 'Completed']['total_amount'].sum():,.2f}")
print(f"  Average Order Value: ${fact_sales[fact_sales['order_status'] == 'Completed']['total_amount'].mean():,.2f}")
print("=" * 80)
