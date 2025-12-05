"""
Apache Spark ETL Pipeline for Data Lake
Extracts data from raw zone, transforms it, and loads into warehouse
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, month, year, 
    to_date, round as spark_round, when, lit, concat_ws,
    row_number, desc, dense_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark.config import Config
from spark.utils import (
    log_dataframe_info, check_null_values, check_duplicates,
    get_data_quality_report, write_parquet, logger
)

class DataLakeETL:
    """Main ETL Pipeline class"""
    
    def __init__(self):
        """Initialize Spark session"""
        logger.info("Initializing Spark session...")
        
        self.spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME) \
            .master(Config.SPARK_MASTER) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")
    
    def extract_data(self):
        """Extract data from raw zone"""
        logger.info("=" * 80)
        logger.info("EXTRACT PHASE - Reading raw data")
        logger.info("=" * 80)
        
        # Read Sales CSV
        logger.info(f"Reading sales data from {Config.SALES_FILE}")
        sales_raw = self.spark.read.csv(
            Config.SALES_FILE,
            header=True,
            inferSchema=True
        )
        log_dataframe_info(sales_raw, "Sales Raw")
        
        # Read Products JSON
        logger.info(f"Reading products data from {Config.PRODUCTS_FILE}")
        products_raw = self.spark.read.json(Config.PRODUCTS_FILE)
        log_dataframe_info(products_raw, "Products Raw")
        
        # Read Customers CSV
        logger.info(f"Reading customers data from {Config.CUSTOMERS_FILE}")
        customers_raw = self.spark.read.csv(
            Config.CUSTOMERS_FILE,
            header=True,
            inferSchema=True
        )
        log_dataframe_info(customers_raw, "Customers Raw")
        
        return sales_raw, products_raw, customers_raw
    
    def transform_data(self, sales_raw, products_raw, customers_raw):
        """Transform and clean data"""
        logger.info("=" * 80)
        logger.info("TRANSFORM PHASE - Cleaning and transforming data")
        logger.info("=" * 80)
        
        # Clean Sales Data
        logger.info("Cleaning sales data...")
        sales_clean = sales_raw \
            .dropna(subset=['transaction_id', 'customer_id', 'product_id']) \
            .dropDuplicates(['transaction_id']) \
            .withColumn('transaction_date', to_date(col('transaction_date'))) \
            .withColumn('year', year(col('transaction_date'))) \
            .withColumn('month', month(col('transaction_date'))) \
            .withColumn('total_amount', col('total_amount').cast(DoubleType())) \
            .withColumn('quantity', col('quantity').cast(IntegerType()))
        
        check_null_values(sales_clean, "Sales Clean")
        check_duplicates(sales_clean, ['transaction_id'], "Sales Clean")
        
        # Clean Products Data
        logger.info("Cleaning products data...")
        products_clean = products_raw \
            .dropna(subset=['product_id']) \
            .dropDuplicates(['product_id']) \
            .withColumn('unit_price', col('unit_price').cast(DoubleType())) \
            .withColumn('cost_price', col('cost_price').cast(DoubleType())) \
            .withColumn('stock_quantity', col('stock_quantity').cast(IntegerType())) \
            .withColumn('profit_margin', 
                       spark_round((col('unit_price') - col('cost_price')) / col('unit_price') * 100, 2))
        
        check_null_values(products_clean, "Products Clean")
        
        # Clean Customers Data
        logger.info("Cleaning customers data...")
        customers_clean = customers_raw \
            .dropna(subset=['customer_id']) \
            .dropDuplicates(['customer_id']) \
            .withColumn('registration_date', to_date(col('registration_date'))) \
            .withColumn('age', col('age').cast(IntegerType())) \
            .withColumn('full_name', concat_ws(' ', col('first_name'), col('last_name')))
        
        check_null_values(customers_clean, "Customers Clean")
        
        # Save cleaned data to processed zone
        logger.info("Saving cleaned data to processed zone...")
        write_parquet(sales_clean, Config.PROCESSED_SALES)
        write_parquet(products_clean, Config.PROCESSED_PRODUCTS)
        write_parquet(customers_clean, Config.PROCESSED_CUSTOMERS)
        
        return sales_clean, products_clean, customers_clean
    
    def create_fact_and_dimensions(self, sales_clean, products_clean, customers_clean):
        """Create fact and dimension tables"""
        logger.info("=" * 80)
        logger.info("Creating Fact and Dimension Tables")
        logger.info("=" * 80)
        
        # Fact Table: Sales with enriched data
        logger.info("Creating fact_sales table...")
        fact_sales = sales_clean \
            .join(products_clean.select('product_id', 'product_name', 'category', 'unit_price'), 
                  'product_id', 'left') \
            .join(customers_clean.select('customer_id', 'full_name', 'city', 'state', 'customer_segment'), 
                  'customer_id', 'left') \
            .select(
                'transaction_id',
                'transaction_date',
                'year',
                'month',
                'customer_id',
                col('full_name').alias('customer_name'),
                'customer_segment',
                'city',
                'state',
                'product_id',
                'product_name',
                'category',
                'quantity',
                sales_clean['unit_price'].alias('sale_unit_price'),
                'discount_percent',
                'discount_amount',
                'subtotal',
                'tax_amount',
                'total_amount',
                'payment_method',
                'shipping_method',
                'order_status',
                'store_location'
            )
        
        log_dataframe_info(fact_sales, "Fact Sales")
        write_parquet(fact_sales, Config.WAREHOUSE_FACT_SALES)
        
        # Dimension: Products
        logger.info("Creating dim_products table...")
        dim_products = products_clean.select(
            'product_id',
            'product_name',
            'category',
            'subcategory',
            'brand',
            'unit_price',
            'cost_price',
            'profit_margin',
            'stock_quantity',
            'supplier',
            'weight_kg',
            'launch_date'
        )
        
        write_parquet(dim_products, Config.WAREHOUSE_DIM_PRODUCTS)
        
        # Dimension: Customers
        logger.info("Creating dim_customers table...")
        dim_customers = customers_clean.select(
            'customer_id',
            'full_name',
            'email',
            'phone',
            'city',
            'state',
            'zip_code',
            'country',
            'registration_date',
            'customer_segment',
            'age',
            'gender'
        )
        
        write_parquet(dim_customers, Config.WAREHOUSE_DIM_CUSTOMERS)
        
        return fact_sales, dim_products, dim_customers
    
    def create_aggregations(self, fact_sales):
        """Create aggregated tables for analytics"""
        logger.info("=" * 80)
        logger.info("Creating Aggregated Tables")
        logger.info("=" * 80)
        
        # Monthly Revenue Aggregation
        logger.info("Creating monthly revenue aggregation...")
        agg_monthly = fact_sales \
            .filter(col('order_status') == 'Completed') \
            .groupBy('year', 'month') \
            .agg(
                spark_sum('total_amount').alias('total_revenue'),
                spark_sum('quantity').alias('total_quantity'),
                count('transaction_id').alias('transaction_count'),
                avg('total_amount').alias('avg_order_value'),
                spark_sum('discount_amount').alias('total_discounts')
            ) \
            .orderBy('year', 'month')
        
        log_dataframe_info(agg_monthly, "Monthly Revenue")
        write_parquet(agg_monthly, Config.WAREHOUSE_AGG_MONTHLY)
        
        # Top Products Aggregation
        logger.info("Creating top products aggregation...")
        agg_products = fact_sales \
            .filter(col('order_status') == 'Completed') \
            .groupBy('product_id', 'product_name', 'category') \
            .agg(
                spark_sum('quantity').alias('total_quantity_sold'),
                spark_sum('total_amount').alias('total_revenue'),
                count('transaction_id').alias('transaction_count'),
                avg('total_amount').alias('avg_sale_amount')
            ) \
            .orderBy(desc('total_revenue'))
        
        # Add ranking
        window_spec = Window.orderBy(desc('total_revenue'))
        agg_products = agg_products.withColumn('revenue_rank', dense_rank().over(window_spec))
        
        log_dataframe_info(agg_products, "Product Performance")
        write_parquet(agg_products, Config.WAREHOUSE_AGG_PRODUCTS)
        
        # Regional Sales Aggregation
        logger.info("Creating regional sales aggregation...")
        agg_regions = fact_sales \
            .filter(col('order_status') == 'Completed') \
            .groupBy('state', 'city') \
            .agg(
                spark_sum('total_amount').alias('total_revenue'),
                count('transaction_id').alias('transaction_count'),
                avg('total_amount').alias('avg_order_value'),
                count('customer_id').alias('unique_customers')
            ) \
            .orderBy(desc('total_revenue'))
        
        log_dataframe_info(agg_regions, "Regional Sales")
        write_parquet(agg_regions, Config.WAREHOUSE_AGG_REGIONS)
        
        return agg_monthly, agg_products, agg_regions
    
    def run(self):
        """Execute the complete ETL pipeline"""
        logger.info("=" * 80)
        logger.info("STARTING DATA LAKE ETL PIPELINE")
        logger.info("=" * 80)
        
        try:
            # Extract
            sales_raw, products_raw, customers_raw = self.extract_data()
            
            # Transform
            sales_clean, products_clean, customers_clean = self.transform_data(
                sales_raw, products_raw, customers_raw
            )
            
            # Load - Create fact and dimension tables
            fact_sales, dim_products, dim_customers = self.create_fact_and_dimensions(
                sales_clean, products_clean, customers_clean
            )
            
            # Create aggregations
            agg_monthly, agg_products, agg_regions = self.create_aggregations(fact_sales)
            
            logger.info("=" * 80)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            
            # Print summary
            logger.info("\nPipeline Summary:")
            logger.info(f"  Processed Sales Records: {fact_sales.count()}")
            logger.info(f"  Products in Catalog: {dim_products.count()}")
            logger.info(f"  Customers: {dim_customers.count()}")
            logger.info(f"  Monthly Aggregations: {agg_monthly.count()}")
            logger.info(f"  Product Performance Records: {agg_products.count()}")
            logger.info(f"  Regional Aggregations: {agg_regions.count()}")
            
            return True
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {str(e)}", exc_info=True)
            return False
        
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main entry point"""
    etl = DataLakeETL()
    success = etl.run()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
