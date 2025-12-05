"""
Simplified ETL Script (Without Spark)
For testing the pipeline without requiring full Spark installation
"""
import pandas as pd
import json
import os
from datetime import datetime

def get_base_path():
    """Get the base project path"""
    return os.path.dirname(os.path.abspath(__file__))

def load_raw_data(base_path):
    """Load raw data from staging area"""
    staging_path = os.path.join(base_path, 'datalake', 'staging')
    
    print("=" * 60)
    print("LOADING RAW DATA FROM STAGING")
    print("=" * 60)
    
    # Load Sales
    sales_df = pd.read_csv(os.path.join(staging_path, 'sales.csv'))
    sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
    print(f"✓ Loaded {len(sales_df)} sales records")
    
    # Load Products
    with open(os.path.join(staging_path, 'products.json'), 'r') as f:
        products_data = json.load(f)
    products_df = pd.DataFrame(products_data)
    print(f"✓ Loaded {len(products_df)} product records")
    
    # Load Customers
    customers_df = pd.read_csv(os.path.join(staging_path, 'customers.csv'))
    customers_df['join_date'] = pd.to_datetime(customers_df['join_date'])
    print(f"✓ Loaded {len(customers_df)} customer records")
    
    return sales_df, products_df, customers_df

def clean_data(sales_df, products_df, customers_df):
    """Clean and standardize data"""
    print("\n" + "=" * 60)
    print("CLEANING DATA")
    print("=" * 60)
    
    # Clean Sales
    sales_clean = sales_df.dropna(subset=['sale_id', 'product_id', 'customer_id'])
    sales_clean = sales_clean.fillna({'quantity': 0, 'unit_price': 0.0})
    print(f"✓ Cleaned sales data: {len(sales_clean)} records")
    
    # Clean Products
    products_clean = products_df.dropna(subset=['product_id'])
    products_clean = products_clean.fillna({
        'name': 'Unknown',
        'category': 'Uncategorized',
        'supplier': 'Unknown',
        'cost': 0.0
    })
    print(f"✓ Cleaned products data: {len(products_clean)} records")
    
    # Clean Customers
    customers_clean = customers_df.dropna(subset=['customer_id'])
    customers_clean = customers_clean.fillna({
        'name': 'Unknown',
        'email': 'unknown@example.com',
        'region': 'Unknown'
    })
    print(f"✓ Cleaned customers data: {len(customers_clean)} records")
    
    return sales_clean, products_clean, customers_clean

def transform_data(sales_df, products_df, customers_df):
    """Perform transformations and create enriched datasets"""
    print("\n" + "=" * 60)
    print("TRANSFORMING DATA")
    print("=" * 60)
    
    # Calculate total revenue per sale
    sales_df['total_revenue'] = (sales_df['quantity'] * sales_df['unit_price']).round(2)
    
    # Add date components
    sales_df['year'] = sales_df['sale_date'].dt.year
    sales_df['month'] = sales_df['sale_date'].dt.month
    sales_df['day'] = sales_df['sale_date'].dt.day
    
    print("✓ Added revenue calculations and date components")
    
    # Join with products
    sales_with_products = sales_df.merge(
        products_df,
        on='product_id',
        how='left',
        suffixes=('', '_product')
    )
    sales_with_products = sales_with_products.rename(columns={
        'name': 'product_name',
        'category': 'product_category',
        'supplier': 'product_supplier',
        'cost': 'product_cost'
    })
    
    print("✓ Joined sales with products")
    
    # Join with customers
    fact_table = sales_with_products.merge(
        customers_df,
        on='customer_id',
        how='left',
        suffixes=('', '_customer')
    )
    fact_table = fact_table.rename(columns={
        'name': 'customer_name',
        'email': 'customer_email',
        'region': 'customer_region'
    })
    
    print("✓ Joined with customers to create fact table")
    print(f"✓ Final fact table: {len(fact_table)} records")
    
    return fact_table

def create_warehouse_tables(fact_table, products_df, customers_df):
    """Create dimensional model tables"""
    print("\n" + "=" * 60)
    print("CREATING WAREHOUSE TABLES")
    print("=" * 60)
    
    # Fact Sales Table
    fact_sales = fact_table[[
        'sale_id', 'product_id', 'customer_id', 'sale_date',
        'quantity', 'unit_price', 'total_revenue', 'year', 'month', 'day'
    ]].copy()
    
    print(f"✓ Created fact_sales: {len(fact_sales)} records")
    
    # Dimension Products Table
    dim_products = products_df[[
        'product_id', 'name', 'category', 'supplier', 'cost'
    ]].copy()
    dim_products = dim_products.rename(columns={
        'name': 'product_name',
        'category': 'product_category',
        'supplier': 'product_supplier',
        'cost': 'product_cost'
    })
    dim_products = dim_products.drop_duplicates(subset=['product_id'])
    
    print(f"✓ Created dim_products: {len(dim_products)} records")
    
    # Dimension Customers Table
    dim_customers = customers_df[[
        'customer_id', 'name', 'email', 'region', 'join_date'
    ]].copy()
    dim_customers = dim_customers.rename(columns={
        'name': 'customer_name',
        'email': 'customer_email',
        'region': 'customer_region',
        'join_date': 'customer_join_date'
    })
    dim_customers = dim_customers.drop_duplicates(subset=['customer_id'])
    
    print(f"✓ Created dim_customers: {len(dim_customers)} records")
    
    return fact_sales, dim_products, dim_customers

def save_to_processed(fact_table, base_path):
    """Save enriched fact table to processed zone"""
    processed_path = os.path.join(base_path, 'datalake', 'processed')
    
    print("\n" + "=" * 60)
    print("SAVING TO PROCESSED ZONE")
    print("=" * 60)
    
    output_path = os.path.join(processed_path, 'enriched_sales.parquet')
    fact_table.to_parquet(output_path, index=False)
    
    print(f"✓ Saved enriched sales to: {output_path}")

def save_to_warehouse(fact_sales, dim_products, dim_customers, base_path):
    """Save warehouse tables to warehouse zone"""
    warehouse_path = os.path.join(base_path, 'datalake', 'warehouse')
    
    print("\n" + "=" * 60)
    print("SAVING TO WAREHOUSE ZONE")
    print("=" * 60)
    
    # Save Fact Table
    fact_sales.to_parquet(
        os.path.join(warehouse_path, 'fact_sales.parquet'),
        index=False
    )
    print("✓ Saved fact_sales.parquet")
    
    # Save Dimension Tables
    dim_products.to_parquet(
        os.path.join(warehouse_path, 'dim_products.parquet'),
        index=False
    )
    print("✓ Saved dim_products.parquet")
    
    dim_customers.to_parquet(
        os.path.join(warehouse_path, 'dim_customers.parquet'),
        index=False
    )
    print("✓ Saved dim_customers.parquet")

def print_summary_stats(fact_sales):
    """Print summary statistics"""
    print("\n" + "=" * 60)
    print("SUMMARY STATISTICS")
    print("=" * 60)
    
    # Total revenue
    total_revenue = fact_sales['total_revenue'].sum()
    print(f"Total Revenue: ${total_revenue:,.2f}")
    
    # Total transactions
    total_transactions = len(fact_sales)
    print(f"Total Transactions: {total_transactions:,}")
    
    # Average transaction value
    avg_transaction = total_revenue / total_transactions if total_transactions > 0 else 0
    print(f"Average Transaction Value: ${avg_transaction:,.2f}")
    
    # Top 5 products by revenue
    print("\nTop 5 Products by Revenue:")
    top_products = fact_sales.groupby('product_id')['total_revenue'].sum() \
        .sort_values(ascending=False).head(5)
    for product_id, revenue in top_products.items():
        print(f"  {product_id}: ${revenue:,.2f}")

def copy_to_staging(base_path):
    """Copy raw files to staging"""
    import shutil
    
    raw_path = os.path.join(base_path, 'datalake', 'raw')
    staging_path = os.path.join(base_path, 'datalake', 'staging')
    
    print("=" * 60)
    print("STAGING RAW DATA")
    print("=" * 60)
    
    for file in ['sales.csv', 'products.json', 'customers.csv']:
        src = os.path.join(raw_path, file)
        dst = os.path.join(staging_path, file)
        shutil.copy2(src, dst)
        print(f"✓ Staged: {file}")

def main():
    """Main ETL execution"""
    print("\n" + "=" * 60)
    print("STARTING SIMPLIFIED ETL PIPELINE")
    print("=" * 60)
    
    base_path = get_base_path()
    
    try:
        # Copy to staging
        copy_to_staging(base_path)
        
        # Load data
        sales_df, products_df, customers_df = load_raw_data(base_path)
        
        # Clean data
        sales_clean, products_clean, customers_clean = clean_data(
            sales_df, products_df, customers_df
        )
        
        # Transform data
        fact_table = transform_data(sales_clean, products_clean, customers_clean)
        
        # Create warehouse tables
        fact_sales, dim_products, dim_customers = create_warehouse_tables(
            fact_table, products_clean, customers_clean
        )
        
        # Save to processed zone
        save_to_processed(fact_table, base_path)
        
        # Save to warehouse zone
        save_to_warehouse(fact_sales, dim_products, dim_customers, base_path)
        
        # Print summary
        print_summary_stats(fact_sales)
        
        print("\n" + "=" * 60)
        print("✓ ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
