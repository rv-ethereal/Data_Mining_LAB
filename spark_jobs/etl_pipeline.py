"""
Apache Spark ETL Pipeline for Data Lake
Author: Susanta Kumar Mohanty
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max, min as _min,
    year, month, dayofmonth, when, round as _round, lit
)
from pyspark.sql.types import DoubleType
import yaml
import logging
from datetime import datetime
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load configuration
config_path = os.getenv('CONFIG_PATH', '/opt/airflow/config/config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

class DataLakeETL:
    """ETL Pipeline for On-Premise Data Lake"""
    
    def __init__(self):
        """Initialize Spark session"""
        self.spark = self._create_spark_session()
        self.raw_path = config['paths']['raw']
        self.staging_path = config['paths']['staging']
        self.processed_path = config['paths']['processed']
        self.warehouse_path = config['paths']['warehouse']
        
    def _create_spark_session(self):
        """Create and configure Spark session"""
        logger.info("Creating Spark session...")
        
        spark = SparkSession.builder \
            .appName(config['spark']['app_name']) \
            .master(config['spark']['master']) \
            .config("spark.driver.memory", config['spark']['driver_memory']) \
            .config("spark.executor.memory", config['spark']['executor_memory']) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark session created successfully")
        return spark
    
    def extract_data(self):
        """Extract data from raw zone"""
        logger.info("="*60)
        logger.info("PHASE 1: EXTRACT - Reading raw data")
        logger.info("="*60)
        
        # Read sales data
        logger.info("Reading sales.csv...")
        sales_df = self.spark.read.csv(
            f"{self.raw_path}/sales.csv",
            header=True,
            inferSchema=True
        )
        logger.info(f"✓ Sales records loaded: {sales_df.count()}")
        
        # Read products data (JSON)
        logger.info("Reading products.json...")
        products_df = self.spark.read.json(f"{self.raw_path}/products.json")
        logger.info(f"✓ Products loaded: {products_df.count()}")
        
        # Read customers data
        logger.info("Reading customers.csv...")
        customers_df = self.spark.read.csv(
            f"{self.raw_path}/customers.csv",
            header=True,
            inferSchema=True
        )
        logger.info(f"✓ Customers loaded: {customers_df.count()}")
        
        return sales_df, products_df, customers_df
    
    def transform_data(self, sales_df, products_df, customers_df):
        """Transform and clean data"""
        logger.info("\n" + "="*60)
        logger.info("PHASE 2: TRANSFORM - Data cleaning and enrichment")
        logger.info("="*60)
        
        # ===== Data Quality Checks =====
        logger.info("\n1. Data Quality Checks")
        initial_count = sales_df.count()
        logger.info(f"Initial sales records: {initial_count}")
        
        # Remove null values
        sales_clean = sales_df.dropna(subset=['customer_id', 'product_id', 'quantity', 'price'])
        after_null_removal = sales_clean.count()
        logger.info(f"After removing nulls: {after_null_removal} (removed {initial_count - after_null_removal})")
        
        # Filter only completed transactions
        sales_clean = sales_clean.filter(col('status') == 'Completed')
        after_status_filter = sales_clean.count()
        logger.info(f"After filtering completed only: {after_status_filter}")
        
        # ===== Feature Engineering =====
        logger.info("\n2. Feature Engineering")
        
        # Calculate financial metrics
        sales_clean = sales_clean \
            .withColumn('discount_amount', _round(col('price') * col('quantity') * col('discount'), 2)) \
            .withColumn('gross_amount', _round(col('price') * col('quantity'), 2)) \
            .withColumn('net_amount', _round(col('gross_amount') - col('discount_amount'), 2))
        
        # Extract date features
        sales_clean = sales_clean \
            .withColumn('year', year(col('transaction_date'))) \
            .withColumn('month', month(col('transaction_date'))) \
            .withColumn('day', dayofmonth(col('transaction_date')))
        
        logger.info("✓ Financial metrics calculated")
        logger.info("✓ Date features extracted")
        
        # ===== Data Enrichment =====
        logger.info("\n3. Data Enrichment - Joining datasets")
        
        # Join with products
        enriched_df = sales_clean.join(
            products_df.select('product_id', 'product_name', 'category', 'brand', 'cost'),
            on='product_id',
            how='left'
        )
        logger.info("✓ Joined with products data")
        
        # Join with customers
        enriched_df = enriched_df.join(
            customers_df.select('customer_id', 'first_name', 'last_name', 'city', 'state', 'customer_segment'),
            on='customer_id',
            how='left'
        )
        logger.info("✓ Joined with customers data")
        
        # Calculate profit
        enriched_df = enriched_df.withColumn(
            'profit',
            _round(col('net_amount') - (col('cost') * col('quantity')), 2)
        )
        
        logger.info(f"✓ Final enriched records: {enriched_df.count()}")
        
        # ===== Aggregations for Analytics =====
        logger.info("\n4. Creating analytical aggregations")
        
        # Revenue by month
        revenue_monthly = enriched_df.groupBy('year', 'month').agg(
            _sum('net_amount').alias('total_revenue'),
            _sum('profit').alias('total_profit'),
            count('order_id').alias('total_orders'),
            avg('net_amount').alias('avg_order_value')
        ).orderBy('year', 'month')
        logger.info(f"✓ Monthly revenue aggregation: {revenue_monthly.count()} months")
        
        # Revenue by category
        revenue_category = enriched_df.groupBy('category').agg(
            _sum('net_amount').alias('total_revenue'),
            _sum('profit').alias('total_profit'),
            count('order_id').alias('total_orders')
        ).orderBy(col('total_revenue').desc())
        logger.info(f"✓ Category revenue aggregation: {revenue_category.count()} categories")
        
        # Top products
        top_products = enriched_df.groupBy('product_id', 'product_name', 'category').agg(
            _sum('quantity').alias('total_quantity_sold'),
            _sum('net_amount').alias('total_revenue'),
            count('order_id').alias('times_ordered')
        ).orderBy(col('total_revenue').desc()).limit(20)
        logger.info("✓ Top 20 products identified")
        
        # Customer analytics
        customer_analytics = enriched_df.groupBy('customer_id', 'customer_segment', 'city').agg(
            count('order_id').alias('total_orders'),
            _sum('net_amount').alias('lifetime_value'),
            avg('net_amount').alias('avg_order_value')
        ).orderBy(col('lifetime_value').desc())
        logger.info(f"✓ Customer analytics: {customer_analytics.count()} customers")
        
        # State-wise sales
        state_sales = enriched_df.groupBy('state').agg(
            _sum('net_amount').alias('total_revenue'),
            count('order_id').alias('total_orders')
        ).orderBy(col('total_revenue').desc())
        logger.info(f"✓ State-wise sales: {state_sales.count()} states")
        
        return {
            'enriched_sales': enriched_df,
            'revenue_monthly': revenue_monthly,
            'revenue_category': revenue_category,
            'top_products': top_products,
            'customer_analytics': customer_analytics,
            'state_sales': state_sales
        }
    
    def load_data(self, transformed_data):
        """Load data to processed and warehouse zones"""
        logger.info("\n" + "="*60)
        logger.info("PHASE 3: LOAD - Writing to data lake")
        logger.info("="*60)
        
        # Load enriched sales to processed zone
        logger.info("\n1. Writing to PROCESSED zone...")
        transformed_data['enriched_sales'].write \
            .mode('overwrite') \
            .parquet(f"{self.processed_path}/sales_enriched")
        logger.info(f"✓ Enriched sales written to {self.processed_path}/sales_enriched")
        
        # Load analytics tables to warehouse zone
        logger.info("\n2. Writing to WAREHOUSE zone...")
        
        tables = [
            ('revenue_monthly', 'revenue_by_month'),
            ('revenue_category', 'revenue_by_category'),
            ('top_products', 'top_products'),
            ('customer_analytics', 'customer_analytics'),
            ('state_sales', 'state_wise_sales')
        ]
        
        for data_key, table_name in tables:
            transformed_data[data_key].write \
                .mode('overwrite') \
                .parquet(f"{self.warehouse_path}/{table_name}")
            logger.info(f"✓ {table_name} written")
        
        logger.info("\n✓ All data loaded successfully!")
    
    def validate_data(self):
        """Validate processed data"""
        logger.info("\n" + "="*60)
        logger.info("DATA VALIDATION")
        logger.info("="*60)
        
        # Check if files exist and have data
        enriched_df = self.spark.read.parquet(f"{self.processed_path}/sales_enriched")
        record_count = enriched_df.count()
        
        logger.info(f"\nEnriched sales records: {record_count}")
        
        if record_count > 0:
            logger.info("✓ Validation successful - Data pipeline completed!")
            return True
        else:
            logger.error("✗ Validation failed - No records found!")
            return False
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        try:
            start_time = datetime.now()
            logger.info("\n" + "="*60)
            logger.info("STARTING ETL PIPELINE")
            logger.info(f"Timestamp: {start_time}")
            logger.info("="*60)
            
            # Extract
            sales_df, products_df, customers_df = self.extract_data()
            
            # Transform
            transformed_data = self.transform_data(sales_df, products_df, customers_df)
            
            # Load
            self.load_data(transformed_data)
            
            # Validate
            self.validate_data()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("\n" + "="*60)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("="*60 + "\n")
            
        except Exception as e:
            logger.error(f"\n✗ ETL Pipeline failed: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    etl = DataLakeETL()
    etl.run_pipeline()
