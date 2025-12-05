"""
Silver Layer Transformation
Cleans and validates data from Bronze, applies business logic
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, when, current_timestamp
from pyspark.sql.types import DecimalType

# Initialize Spark
spark = SparkSession.builder \
    .appName("Silver-Layer-Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("=== Silver Layer Transformation Started ===")

# 1. Customers - Clean and standardize
print("\nTransforming customers...")
customers = spark.read.format("delta").load("s3a://bronze/customers")
customers_clean = customers \
    .dropna(subset=["customer_id", "email"]) \
    .withColumn("email", trim(col("email"))) \
    .withColumn("city", upper(trim(col("city")))) \
    .withColumn("processing_timestamp", current_timestamp()) \
   .drop("ingestion_timestamp", "source_file")

customers_clean.write.format("delta").mode("overwrite").save("s3a://silver/customers")
print(f"[SUCCESS] Customers: {customers_clean.count():,} records")

# 2. Products - Clean and validate
print("\nTransforming products...")
products = spark.read.format("delta").load("s3a://bronze/products")
products_clean = products \
    .dropna(subset=["product_id", "product_name"]) \
    .withColumn("category", upper(trim(col("category")))) \
    .withColumn("price", col("price").cast(DecimalType(10, 2))) \
    .filter(col("price") > 0) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .drop("ingestion_timestamp", "source_file")

products_clean.write.format("delta").mode("overwrite").save("s3a://silver/products")
print(f"[SUCCESS] Products: {products_clean.count():,} records")

# 3. Orders - Filter valid statuses
print("\nTransforming orders...")
orders = spark.read.format("delta").load("s3a://bronze/orders")
orders_clean = orders \
    .dropna(subset=["order_id", "customer_id"]) \
    .withColumn("status", upper(trim(col("status")))) \
    .filter(col("status").isin(["COMPLETED", "PENDING", "CANCELLED"])) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .drop("ingestion_timestamp", "source_file")

orders_clean.write.format("delta").mode("overwrite").save("s3a://silver/orders")
print(f"[SUCCESS] Orders: {orders_clean.count():,} records")

# 4. Order Items - Validate amounts
print("\nTransforming order_items...")
order_items = spark.read.format("delta").load("s3a://bronze/order_items")
order_items_clean = order_items \
    .dropna(subset=["order_item_id", "order_id", "product_id"]) \
    .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2))) \
    .withColumn("total_price", col("total_price").cast(DecimalType(10, 2))) \
    .filter((col("quantity") > 0) & (col("unit_price") > 0)) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .drop("ingestion_timestamp", "source_file")

order_items_clean.write.format("delta").mode("overwrite").save("s3a://silver/order_items")
print(f"[SUCCESS] Order Items: {order_items_clean.count():,} records")

print("\n=== Silver Layer Transformation Complete ===")
spark.stop()
