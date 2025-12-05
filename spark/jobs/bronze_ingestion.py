"""
Bronze Layer Ingestion
Reads raw CSV files from MinIO and writes to Delta Lake bronze tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

# Initialize Spark with Delta Lake
spark = SparkSession.builder \
    .appName("Bronze-Layer-Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("=== Bronze Layer Ingestion Started ===")

# Tables to ingest
tables = ['customers', 'products', 'orders', 'order_items']

for table_name in tables:
    print(f"\nProcessing {table_name}...")
    
    # Read CSV from MinIO raw-data bucket
    df = spark.read.csv(
        f"s3a://raw-data/{table_name}.csv",
        header=True,
        inferSchema=True
    )
    
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit(f"{table_name}.csv"))
    
    # Write to Delta Lake bronze bucket
    df_with_metadata.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"s3a://bronze/{table_name}")
    
    record_count = df_with_metadata.count()
    print(f"[SUCCESS] {table_name}: {record_count:,} records written to bronze")

print("\n=== Bronze Layer Ingestion Complete ===")
spark.stop()