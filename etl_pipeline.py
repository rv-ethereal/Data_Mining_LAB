"""
Apache Spark ETL Pipeline
Processes raw data from data lake, applies transformations, and loads to warehouse
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round, when, monotonically_increasing_id
import sys
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataLakeETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def load_raw_data(base_path):
    """Load raw data from multiple sources"""
    print("[INFO] Loading raw data...")
    
    # Load sales data
    sales_df = spark.read.csv(f"{base_path}/raw/sales.csv", header=True, inferSchema=True)
    
    # Load products data
    products_df = spark.read.json(f"{base_path}/raw/products.json")
    
    # Load customers data
    customers_df = spark.read.csv(f"{base_path}/raw/customers.csv", header=True, inferSchema=True)
    
    print(f"[INFO] Sales records: {sales_df.count()}")
    print(f"[INFO] Products records: {products_df.count()}")
    print(f"[INFO] Customers records: {customers_df.count()}")
    
    return sales_df, products_df, customers_df

def clean_sales_data(sales_df):
    """Clean and validate sales data"""
    print("[INFO] Cleaning sales data...")
    
    # Remove nulls
    sales_clean = sales_df.dropna(subset=['sale_id', 'product_id', 'customer_id', 'qty', 'price'])
    
    # Remove duplicates
    sales_clean = sales_clean.dropDuplicates(subset=['sale_id'])
    
    # Convert types
    sales_clean = sales_clean.withColumn("qty", col("qty").cast("int")) \
        .withColumn("price", col("price").cast("decimal(10,2)")) \
        .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
    
    # Calculate total amount
    sales_clean = sales_clean.withColumn("total_amount", 
                                        round(col("qty") * col("price"), 2))
    
    print(f"[INFO] Cleaned sales records: {sales_clean.count()}")
    return sales_clean

def clean_customers_data(customers_df):
    """Clean and validate customers data"""
    print("[INFO] Cleaning customers data...")
    
    customers_clean = customers_df.dropna(subset=['customer_id', 'name'])
    customers_clean = customers_clean.dropDuplicates(subset=['customer_id'])
    customers_clean = customers_clean.withColumn("signup_date", 
                                                 to_date(col("signup_date"), "yyyy-MM-dd"))
    
    print(f"[INFO] Cleaned customers records: {customers_clean.count()}")
    return customers_clean

def transform_data(sales_df, products_df, customers_df):
    """Apply business logic transformations"""
    print("[INFO] Applying transformations...")
    
    # Join sales with products
    sales_products = sales_df.join(products_df, on='product_id', how='left')
    
    # Join with customers
    enriched_df = sales_products.join(customers_df, on='customer_id', how='left')
    
    # Add additional features
    enriched_df = enriched_df.withColumn("profit_margin", 
                                        when(col("price") > 0, 
                                             round((col("price") * 0.25), 2))
                                        .otherwise(0))
    
    enriched_df = enriched_df.withColumn("revenue", col("total_amount"))
    
    enriched_df = enriched_df.withColumn("processing_timestamp", 
                                        col(spark.current_date()))
    
    print(f"[INFO] Enriched records: {enriched_df.count()}")
    return enriched_df

def aggregate_data(enriched_df):
    """Create aggregated views for analytics"""
    print("[INFO] Creating aggregated views...")
    
    # Revenue by month
    revenue_by_month = enriched_df.groupBy(
        enriched_df.sale_date.cast("date")
    ).agg({
        "revenue": "sum",
        "sale_id": "count"
    }).withColumnRenamed("sum(revenue)", "total_revenue") \
     .withColumnRenamed("count(sale_id)", "transaction_count")
    
    # Revenue by region
    revenue_by_region = enriched_df.groupBy("region").agg({
        "revenue": "sum",
        "sale_id": "count"
    }).withColumnRenamed("sum(revenue)", "total_revenue") \
     .withColumnRenamed("count(sale_id)", "transaction_count")
    
    # Top products by revenue
    top_products = enriched_df.groupBy("product_id", "name").agg({
        "revenue": "sum",
        "qty": "sum"
    }).withColumnRenamed("sum(revenue)", "total_revenue") \
     .withColumnRenamed("sum(qty)", "total_qty_sold") \
     .orderBy(col("total_revenue").desc())
    
    return revenue_by_month, revenue_by_region, top_products

def save_processed_data(enriched_df, revenue_by_month, revenue_by_region, 
                        top_products, base_path):
    """Save processed data to warehouse"""
    print("[INFO] Saving processed data...")
    
    # Save enriched sales
    enriched_df.write.mode("overwrite") \
        .parquet(f"{base_path}/warehouse/enriched_sales")
    print(f"[INFO] Saved enriched sales to parquet")
    
    # Save aggregations
    revenue_by_month.write.mode("overwrite") \
        .parquet(f"{base_path}/warehouse/revenue_by_month")
    
    revenue_by_region.write.mode("overwrite") \
        .parquet(f"{base_path}/warehouse/revenue_by_region")
    
    top_products.write.mode("overwrite") \
        .parquet(f"{base_path}/warehouse/top_products")
    
    print(f"[INFO] All data successfully saved to warehouse")

def main(base_path="/home/claude/data_mining_lab/datalake"):
    """Main ETL pipeline execution"""
    print(f"\n{'='*60}")
    print(f"[START] Data Lake ETL Pipeline - {datetime.now()}")
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
        
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()