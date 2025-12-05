"""Enterprise Data Lake ETL Pipeline - Class-based Architecture"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, to_date, count, avg, round as spark_round
from typing import Dict, Tuple
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class DataLakeETLPipeline:
    """Enterprise-grade ETL pipeline for data lake processing."""
    
    def __init__(self):
        self._initialize_spark()
        self._setup_paths()
    
    def _initialize_spark(self) -> None:
        """Initialize Spark session with Java 17+ compatibility."""
        config_options = {
            "spark.sql.adaptive.enabled": "true",
            "spark.driver.extraJavaOptions": " ".join([
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
            ]),
            "spark.executor.extraJavaOptions": " ".join([
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
            ])
        }
        
        builder = SparkSession.builder.appName("EnterpriseDataLakeETL").master("local[*]")
        for key, value in config_options.items():
            builder = builder.config(key, value)
        
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
    
    def _setup_paths(self) -> None:
        """Configure data lake directory structure."""
        base = Path(__file__).parent.parent
        self.paths = {
            "raw_sales": str(base / "datalake/raw/sales.csv"),
            "raw_customers": str(base / "datalake/raw/customers.csv"),
            "processed": str(base / "datalake/processed"),
            "warehouse": str(base / "datalake/warehouse")
        }
    
    def ingest_raw_data(self) -> Tuple[DataFrame, DataFrame]:
        """Load raw datasets from data lake."""
        logger.info("Starting data ingestion phase...")
        
        sales_df = self.spark.read.csv(self.paths["raw_sales"], header=True, inferSchema=True)
        customers_df = self.spark.read.csv(self.paths["raw_customers"], header=True, inferSchema=True)
        
        logger.info(f"Loaded {sales_df.count():,} sales transactions")
        logger.info(f"Loaded {customers_df.count():,} customer records")
        return sales_df, customers_df
    
    def cleanse_sales_dataset(self, df: DataFrame) -> DataFrame:
        """Apply data quality transformations to sales data."""
        logger.info("Cleansing sales dataset...")
        
        result = (df
            .filter(
                col("id").isNotNull() & col("date").isNotNull() & col("product").isNotNull() & 
                col("price").isNotNull() & col("qty").isNotNull()
            )
            .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            .withColumn("price", col("price").cast("double"))
            .withColumn("qty", col("qty").cast("int"))
            .withColumn("discount_percent", col("discount_percent").cast("int"))
            .withColumn("total_amount", col("price") * col("qty"))
            .withColumn("discount_amount", col("total_amount") * col("discount_percent") / 100)
            .withColumn("final_amount", col("total_amount") - col("discount_amount"))
            .dropDuplicates(["id"])
        )
        
        logger.info(f"Cleansed sales data: {result.count():,} records")
        return result
    
    def cleanse_customers_dataset(self, df: DataFrame) -> DataFrame:
        """Apply data quality transformations to customer data."""
        logger.info("Cleansing customer dataset...")
        
        result = df.filter(col("cust_id").isNotNull()).dropDuplicates(["cust_id"])
        logger.info(f"Cleansed customer data: {result.count():,} records")
        return result
    
    def merge_datasets(self, sales_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        """Enrich sales with customer dimensions."""
        logger.info("Merging datasets...")
        
        return (sales_df
            .join(customers_df, sales_df.cust_id == customers_df.cust_id, "left")
            .select(
                sales_df["*"],
                customers_df["name"].alias("customer_name"),
                customers_df["city"].alias("customer_city")
            )
        )
    
    def compute_aggregate_metrics(self, sales_df: DataFrame, merged_df: DataFrame) -> Dict[str, DataFrame]:
        """Generate analytical aggregations."""
        logger.info("Computing aggregate metrics...")
        
        metrics = {}
        
        metrics["revenue_by_product"] = (sales_df
            .groupBy("product")
            .agg(
                spark_sum("final_amount").alias("total_revenue"),
                spark_sum("qty").alias("total_quantity"),
                count("id").alias("total_orders"),
                avg("price").alias("avg_price")
            )
            .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
            .withColumn("avg_price", spark_round(col("avg_price"), 2))
            .orderBy(col("total_revenue").desc())
        )
        
        metrics["revenue_by_region"] = (sales_df
            .groupBy("region")
            .agg(
                spark_sum("final_amount").alias("total_revenue"),
                spark_sum("qty").alias("total_quantity"),
                count("id").alias("total_orders"),
                avg("final_amount").alias("avg_order_value")
            )
            .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
            .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
            .orderBy(col("total_revenue").desc())
        )
        
        metrics["payment_analysis"] = (sales_df
            .groupBy("payment_method")
            .agg(
                count("id").alias("transaction_count"),
                spark_sum("final_amount").alias("total_revenue")
            )
            .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
            .orderBy(col("transaction_count").desc())
        )
        
        metrics["status_summary"] = (sales_df
            .groupBy("status")
            .agg(count("id").alias("count"), spark_sum("final_amount").alias("revenue"))
            .withColumn("revenue", spark_round(col("revenue"), 2))
        )
        
        metrics["customer_summary"] = (merged_df
            .groupBy("cust_id", "customer_name", "customer_city")
            .agg(
                count("id").alias("total_purchases"),
                spark_sum("final_amount").alias("total_spent"),
                avg("final_amount").alias("avg_order_value")
            )
            .withColumn("total_spent", spark_round(col("total_spent"), 2))
            .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
            .orderBy(col("total_spent").desc())
        )
        
        metrics["monthly_sales"] = (sales_df
            .withColumn("year_month", col("date").substr(1, 7))
            .groupBy("year_month")
            .agg(
                spark_sum("final_amount").alias("monthly_revenue"),
                count("id").alias("monthly_orders")
            )
            .withColumn("monthly_revenue", spark_round(col("monthly_revenue"), 2))
            .orderBy("year_month")
        )
        
        return metrics
    
    def persist_processed_data(self, sales_df: DataFrame, customers_df: DataFrame, merged_df: DataFrame) -> None:
        """Store cleansed datasets."""
        logger.info("Writing processed data...")
        sales_df.repartition(1).write.mode("overwrite").parquet(f"{self.paths['processed']}/sales_clean")
        customers_df.repartition(1).write.mode("overwrite").parquet(f"{self.paths['processed']}/customers_clean")
        merged_df.repartition(1).write.mode("overwrite").parquet(f"{self.paths['processed']}/sales_with_customers")
    
    def persist_warehouse_tables(self, metrics: Dict[str, DataFrame]) -> None:
        """Store analytical tables."""
        logger.info("Writing warehouse tables...")
        for table_name, dataframe in metrics.items():
            output_path = f"{self.paths['warehouse']}/{table_name}"
            dataframe.repartition(1).write.mode("overwrite").parquet(output_path)
            logger.info(f"  ✓ {table_name}")
    
    def execute(self) -> None:
        """Execute complete ETL pipeline."""
        print("\n" + "="*70)
        print("█" * 70)
        print("█  ENTERPRISE DATA LAKE ETL PIPELINE EXECUTION".ljust(69) + "█")
        print("█" * 70)
        print("="*70 + "\n")
        
        try:
            sales_raw, customers_raw = self.ingest_raw_data()
            sales_clean = self.cleanse_sales_dataset(sales_raw)
            customers_clean = self.cleanse_customers_dataset(customers_raw)
            sales_enriched = self.merge_datasets(sales_clean, customers_clean)
            metrics = self.compute_aggregate_metrics(sales_clean, sales_enriched)
            self.persist_processed_data(sales_clean, customers_clean, sales_enriched)
            self.persist_warehouse_tables(metrics)
            
            print("\n" + "="*70)
            print("TOP PERFORMING PRODUCTS BY REVENUE")
            print("="*70)
            metrics["revenue_by_product"].show(5, truncate=False)
            
            print("\n✓ PIPELINE EXECUTION COMPLETED SUCCESSFULLY\n")
            
        except Exception as error:
            logger.error(f"Pipeline failed: {str(error)}")
            raise
        finally:
            self.spark.stop()

def main():
    """Entry point for ETL pipeline."""
    pipeline = DataLakeETLPipeline()
    pipeline.execute()

if __name__ == "__main__":
    main()
