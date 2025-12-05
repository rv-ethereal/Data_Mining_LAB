"""
Alternative ETL Pipeline using Pandas
(Lighter version for testing without Spark installed)
"""

import pandas as pd
import json
import os
from datetime import datetime
from pathlib import Path

def load_raw_data(base_path):
    """Load raw data from multiple sources"""
    print("[INFO] Loading raw data...")
    
    # Load sales data
    sales_df = pd.read_csv(f"sales.csv")
    
    # Load products data
    with open(f"products.json", 'r') as f:
        products_data = json.load(f)
    products_df = pd.DataFrame(products_data)
    
    # Load customers data
    customers_df = pd.read_csv(f"customers.csv")
    
    print(f"[INFO] Sales records: {len(sales_df)}")
    print(f"[INFO] Products records: {len(products_df)}")
    print(f"[INFO] Customers records: {len(customers_df)}")
    
    return sales_df, products_df, customers_df

def clean_sales_data(sales_df):
    """Clean and validate sales data"""
    print("[INFO] Cleaning sales data...")
    
    # Remove nulls
    sales_clean = sales_df.dropna(subset=['sale_id', 'product_id', 'customer_id', 'qty', 'price'])
    
    # Remove duplicates
    sales_clean = sales_clean.drop_duplicates(subset=['sale_id'])
    
    # Convert types
    sales_clean['qty'] = sales_clean['qty'].astype(int)
    sales_clean['price'] = sales_clean['price'].astype(float)
    sales_clean['sale_date'] = pd.to_datetime(sales_clean['sale_date'])
    
    # Calculate total amount
    sales_clean['total_amount'] = (sales_clean['qty'] * sales_clean['price']).round(2)
    
    print(f"[INFO] Cleaned sales records: {len(sales_clean)}")
    return sales_clean

def clean_customers_data(customers_df):
    """Clean and validate customers data"""
    print("[INFO] Cleaning customers data...")
    
    customers_clean = customers_df.dropna(subset=['customer_id', 'name'])
    customers_clean = customers_clean.drop_duplicates(subset=['customer_id'])
    customers_clean['signup_date'] = pd.to_datetime(customers_clean['signup_date'])
    
    print(f"[INFO] Cleaned customers records: {len(customers_clean)}")
    return customers_clean

def transform_data(sales_df, products_df, customers_df):
    """Apply business logic transformations"""
    print("[INFO] Applying transformations...")
    
    # Join sales with products (handle duplicate columns)
    sales_products = sales_df.merge(products_df, on='product_id', how='left', suffixes=('_sales', '_product'))
    
    # Join with customers
    enriched_df = sales_products.merge(customers_df, on='customer_id', how='left')
    
    # Add profit margin (use price from sales)
    price_col = 'price_sales' if 'price_sales' in enriched_df.columns else 'price'
    enriched_df['profit_margin'] = (enriched_df[price_col] * 0.25).round(2)
    
    # Add revenue
    enriched_df['revenue'] = enriched_df['total_amount']
    
    # Add timestamp
    enriched_df['processing_timestamp'] = datetime.now().date()
    
    print(f"[INFO] Enriched records: {len(enriched_df)}")
    return enriched_df

def aggregate_data(enriched_df):
    """Create aggregated views for analytics"""
    print("[INFO] Creating aggregated views...")
    
    # Revenue by month
    enriched_df['year_month'] = enriched_df['sale_date'].dt.to_period('M')
    revenue_by_month = enriched_df.groupby('year_month').agg({
        'revenue': 'sum',
        'sale_id': 'count'
    }).rename(columns={'revenue': 'total_revenue', 'sale_id': 'transaction_count'})
    
    # Revenue by region
    revenue_by_region = enriched_df.groupby('region').agg({
        'revenue': 'sum',
        'sale_id': 'count'
    }).rename(columns={'revenue': 'total_revenue', 'sale_id': 'transaction_count'})
    
    # Top products by revenue - use product_id since name might be renamed
    name_col = 'name_product' if 'name_product' in enriched_df.columns else ('name' if 'name' in enriched_df.columns else 'product_id')
    top_products = enriched_df.groupby('product_id').agg({
        'revenue': 'sum',
        'qty': 'sum'
    }).rename(columns={'revenue': 'total_revenue', 'qty': 'total_qty_sold'}) \
     .sort_values('total_revenue', ascending=False)
    
    return revenue_by_month, revenue_by_region, top_products

def save_processed_data(enriched_df, revenue_by_month, revenue_by_region, 
                        top_products, base_path):
    """Save processed data to warehouse"""
    print("[INFO] Saving processed data...")
    
    warehouse_path = f"{base_path}/warehouse"
    os.makedirs(warehouse_path, exist_ok=True)
    
    # Save as Parquet (similar to Spark output)
    try:
        enriched_df.to_parquet(f"{warehouse_path}/enriched_sales")
        print(f"[INFO] Saved enriched sales to parquet")
    except ImportError:
        # Fallback to CSV if parquet not available
        enriched_df.to_csv(f"{warehouse_path}/enriched_sales.csv", index=False)
        print(f"[INFO] Saved enriched sales to CSV")
    
    # Save aggregations
    revenue_by_month.to_parquet(f"{warehouse_path}/revenue_by_month") if 'parquet' in dir(pd) else revenue_by_month.to_csv(f"{warehouse_path}/revenue_by_month.csv")
    revenue_by_region.to_parquet(f"{warehouse_path}/revenue_by_region") if 'parquet' in dir(pd) else revenue_by_region.to_csv(f"{warehouse_path}/revenue_by_region.csv")
    top_products.to_parquet(f"{warehouse_path}/top_products") if 'parquet' in dir(pd) else top_products.to_csv(f"{warehouse_path}/top_products.csv")
    
    print(f"[INFO] All data successfully saved to warehouse")

def main(base_path=None):
    """Main ETL pipeline execution"""
    # Auto-detect path - works on any system
    if base_path is None:
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(os.path.dirname(current_dir), "datalake")
    
    print(f"\n{'='*60}")
    print(f"[START] Data Lake ETL Pipeline - {datetime.now()}")
    print(f"[PATH] {base_path}")
    print(f"{'='*60}\n")
    
    try:
        # Load
        sales_df, products_df, customers_df = load_raw_data(base_path)
        
        # Clean
        sales_clean = clean_sales_data(sales_df)
        customers_clean = clean_customers_data(customers_df)
        
        # Transform
        enriched_df = transform_data(sales_clean, products_df, customers_clean)
        
        # Aggregate
        revenue_by_month, revenue_by_region, top_products = aggregate_data(enriched_df)
        
        # Save
        save_processed_data(enriched_df, revenue_by_month, revenue_by_region, 
                           top_products, base_path)
        
        print(f"\n{'='*60}")
        print(f"[SUCCESS] ETL Pipeline completed - {datetime.now()}")
        print(f"{'='*60}\n")
        
        # Print summary stats
        print("📊 SUMMARY STATISTICS:")
        print(f"Total Revenue: ${enriched_df['revenue'].sum():,.2f}")
        print(f"Total Transactions: {len(enriched_df)}")
        print(f"Average Transaction Value: ${enriched_df['revenue'].mean():,.2f}")
        print(f"\nRevenue by Region:")
        print(revenue_by_region['total_revenue'])
        print(f"\nTop 5 Products:")
        print(top_products.head())
        
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {str(e)}")
        raise
    
if __name__ == "__main__":
    main()