"""
PySpark ETL Job - Data Cleaning, Transformation & Warehouse Loading
Processes data from staging to processed zone and creates warehouse tables
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, round as _round, to_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import os
import sys

# Initialize Spark Session with minimal resources for local execution
def create_spark_session():
    """Create a Spark session configured for local execution"""
    spark = SparkSession.builder \
        .appName("ETL Pipeline - Data Engineering") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_base_path():
    """Get the base project path"""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_raw_data(spark, base_path):
    """Load raw data from staging area"""
    staging_path = os.path.join(base_path, 'datalake', 'staging')
    
    print("=" * 60)
    print("LOADING RAW DATA FROM STAGING")
    print("=" * 60)
    
    # Load Sales
    sales_schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    sales_df = spark.read.csv(
        os.path.join(staging_path, 'sales.csv'),
        header=True,
        schema=sales_schema
    )
    print(f"✓ Loaded {sales_df.count()} sales records")
    
    # Load Products
    products_df = spark.read.json(os.path.join(staging_path, 'products.json'))
    print(f"✓ Loaded {products_df.count()} product records")
    
    # Load Customers
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("region", StringType(), True),
        StructField("join_date", StringType(), True)
    ])
    
    customers_df = spark.read.csv(
        os.path.join(staging_path, 'customers.csv'),
        header=True,
        schema=customers_schema
    )
    print(f"✓ Loaded {customers_df.count()} customer records")
    
    return sales_df, products_df, customers_df

def clean_data(sales_df, products_df, customers_df):
    """Clean and standardize data"""
    print("\n" + "=" * 60)
    print("CLEANING DATA")
    print("=" * 60)
    
    # Clean Sales
    sales_clean = sales_df \
        .dropna(subset=['sale_id', 'product_id', 'customer_id']) \
        .withColumn('sale_date', to_date(col('sale_date'), 'yyyy-MM-dd')) \
        .fillna({'quantity': 0, 'unit_price': 0.0})
    
    print(f"✓ Cleaned sales data: {sales_clean.count()} records")
    
    # Clean Products
    products_clean = products_df \
        .dropna(subset=['product_id']) \
        .fillna({'name': 'Unknown', 'category': 'Uncategorized', 'supplier': 'Unknown'}) \
        .fillna({'cost': 0.0})
    
    print(f"✓ Cleaned products data: {products_clean.count()} records")
    
    # Clean Customers
    customers_clean = customers_df \
        .dropna(subset=['customer_id']) \
        .withColumn('join_date', to_date(col('join_date'), 'yyyy-MM-dd')) \
        .fillna({'name': 'Unknown', 'email': 'unknown@example.com', 'region': 'Unknown'})
    
    print(f"✓ Cleaned customers data: {customers_clean.count()} records")
    
    return sales_clean, products_clean, customers_clean

def transform_data(sales_df, products_df, customers_df):
    """Perform transformations and create enriched datasets"""
    print("\n" + "=" * 60)
    print("TRANSFORMING DATA")
    print("=" * 60)
    
    # Calculate total revenue per sale
    sales_enriched = sales_df.withColumn(
        'total_revenue',
        _round(col('quantity') * col('unit_price'), 2)
    )
    
    # Add date components
    sales_enriched = sales_enriched \
        .withColumn('year', year(col('sale_date'))) \
        .withColumn('month', month(col('sale_date'))) \
        .withColumn('day', dayofmonth(col('sale_date')))
    
    print("✓ Added revenue calculations and date components")
    
    # Join with products
    sales_with_products = sales_enriched.join(
        products_df,
        sales_enriched.product_id == products_df.product_id,
        'left'
    ).select(
        sales_enriched['*'],
        products_df['name'].alias('product_name'),
        products_df['category'].alias('product_category'),
        products_df['supplier'].alias('product_supplier'),
        products_df['cost'].alias('product_cost')
    )
    
    print("✓ Joined sales with products")
    
    # Join with customers
    fact_table = sales_with_products.join(
        customers_df,
        sales_with_products.customer_id == customers_df.customer_id,
        'left'
    ).select(
        sales_with_products['*'],
        customers_df['name'].alias('customer_name'),
        customers_df['email'].alias('customer_email'),
        customers_df['region'].alias('customer_region')
    )
    
    print("✓ Joined with customers to create fact table")
    print(f"✓ Final fact table: {fact_table.count()} records")
    
    return fact_table

def create_warehouse_tables(fact_table, products_df, customers_df):
    """Create dimensional model tables"""
    print("\n" + "=" * 60)
    print("CREATING WAREHOUSE TABLES")
    print("=" * 60)
    
    # Fact Sales Table
    fact_sales = fact_table.select(
        'sale_id',
        'product_id',
        'customer_id',
        'sale_date',
        'quantity',
        'unit_price',
        'total_revenue',
        'year',
        'month',
        'day'
    )
    
    print(f"✓ Created fact_sales: {fact_sales.count()} records")
    
    # Dimension Products Table
    dim_products = products_df.select(
        'product_id',
        col('name').alias('product_name'),
        col('category').alias('product_category'),
        col('supplier').alias('product_supplier'),
        col('cost').alias('product_cost')
    ).distinct()
    
    print(f"✓ Created dim_products: {dim_products.count()} records")
    
    # Dimension Customers Table
    dim_customers = customers_df.select(
        'customer_id',
        col('name').alias('customer_name'),
        col('email').alias('customer_email'),
        col('region').alias('customer_region'),
        col('join_date').alias('customer_join_date')
    ).distinct()
    
    print(f"✓ Created dim_customers: {dim_customers.count()} records")
    
    return fact_sales, dim_products, dim_customers

def save_to_processed(fact_table, base_path):
    """Save enriched fact table to processed zone"""
    processed_path = os.path.join(base_path, 'datalake', 'processed')
    
    print("\n" + "=" * 60)
    print("SAVING TO PROCESSED ZONE")
    print("=" * 60)
    
    output_path = os.path.join(processed_path, 'enriched_sales.parquet')
    
    fact_table.write \
        .mode('overwrite') \
        .parquet(output_path)
    
    print(f"✓ Saved enriched sales to: {output_path}")

def save_to_warehouse(fact_sales, dim_products, dim_customers, base_path):
    """Save warehouse tables to warehouse zone"""
    warehouse_path = os.path.join(base_path, 'datalake', 'warehouse')
    
    print("\n" + "=" * 60)
    print("SAVING TO WAREHOUSE ZONE")
    print("=" * 60)
    
    # Save Fact Table
    fact_sales.write \
        .mode('overwrite') \
        .parquet(os.path.join(warehouse_path, 'fact_sales.parquet'))
    print("✓ Saved fact_sales.parquet")
    
    # Save Dimension Tables
    dim_products.write \
        .mode('overwrite') \
        .parquet(os.path.join(warehouse_path, 'dim_products.parquet'))
    print("✓ Saved dim_products.parquet")
    
    dim_customers.write \
        .mode('overwrite') \
        .parquet(os.path.join(warehouse_path, 'dim_customers.parquet'))
    print("✓ Saved dim_customers.parquet")

def print_summary_stats(fact_sales):
    """Print summary statistics"""
    print("\n" + "=" * 60)
    print("SUMMARY STATISTICS")
    print("=" * 60)
    
    # Total revenue
    total_revenue = fact_sales.agg(_sum('total_revenue').alias('total')).collect()[0]['total']
    print(f"Total Revenue: ${total_revenue:,.2f}")
    
    # Total transactions
    total_transactions = fact_sales.count()
    print(f"Total Transactions: {total_transactions:,}")
    
    # Average transaction value
    avg_transaction = total_revenue / total_transactions if total_transactions > 0 else 0
    print(f"Average Transaction Value: ${avg_transaction:,.2f}")
    
    # Top 5 products by revenue
    print("\nTop 5 Products by Revenue:")
    fact_sales.groupBy('product_id') \
        .agg(_sum('total_revenue').alias('revenue')) \
        .orderBy(col('revenue').desc()) \
        .limit(5) \
        .show()

def main():
    """Main ETL execution"""
    print("\n" + "=" * 60)
    print("STARTING SPARK ETL PIPELINE")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    base_path = get_base_path()
    
    try:
        # Load data
        sales_df, products_df, customers_df = load_raw_data(spark, base_path)
        
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
        print(f"\n✗ ERROR: {str(e)}", file=sys.stderr)
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
