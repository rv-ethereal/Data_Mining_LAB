"""
Gold Layer Aggregation
Creates business-ready analytics tables for dashboards
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, countDistinct, avg, max as _max, min as _min, current_timestamp, to_date

# Initialize Spark
spark = SparkSession.builder \
    .appName("Gold-Layer-Aggregation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("=== Gold Layer Aggregation Started ===")

# Load silver tables
customers = spark.read.format("delta").load("s3a://silver/customers")
products = spark.read.format("delta").load("s3a://silver/products")
orders = spark.read.format("delta").load("s3a://silver/orders")
order_items = spark.read.format("delta").load("s3a://silver/order_items")

# 1. Daily Sales Summary
print("\nCreating daily_sales...")
daily_sales = orders.filter(col("status") == "COMPLETED") \
    .join(order_items, "order_id") \
    .groupBy(to_date(col("order_date")).alias("sale_date")) \
    .agg(
        _sum("total_price").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        avg("total_price").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("created_at", current_timestamp())

daily_sales.write.format("delta").mode("overwrite").save("s3a://gold/daily_sales")
print(f"[SUCCESS] Daily Sales: {daily_sales.count():,} records")

# 2. Product Performance
print("\nCreating product_performance...")
product_performance = order_items \
    .join(orders.filter(col("status") == "COMPLETED"), "order_id") \
    .join(products, "product_id") \
    .groupBy("product_id", "product_name", "category") \
    .agg(
        _sum("total_price").alias("total_revenue"),
        _sum("quantity").alias("total_quantity_sold"),
        count("order_id").alias("total_orders"),
        avg("unit_price").alias("avg_price")
    ) \
    .withColumn("created_at", current_timestamp())

product_performance.write.format("delta").mode("overwrite").save("s3a://gold/product_performance")
print(f"[SUCCESS] Product Performance: {product_performance.count():,} records")

# 3. Category Analytics
print("\nCreating category_analytics...")
category_analytics = order_items \
    .join(orders.filter(col("status") == "COMPLETED"), "order_id") \
    .join(products, "product_id") \
    .groupBy("category") \
    .agg(
        _sum("total_price").alias("total_sales"),
        count("order_id").alias("total_orders"),
        avg("total_price").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("created_at", current_timestamp())

category_analytics.write.format("delta").mode("overwrite").save("s3a://gold/category_analytics")
print(f"[SUCCESS] Category Analytics: {category_analytics.count():,} records")

# 4. Customer Analytics  
print("\nCreating customer_analytics...")
customer_analytics = orders.filter(col("status") == "COMPLETED") \
    .join(order_items, "order_id") \
    .join(customers, "customer_id") \
    .groupBy("customer_id", "customer_name", "city", "email") \
    .agg(
        count("order_id").alias("total_orders"),
        _sum("total_price").alias("total_spent"),
        avg("total_price").alias("avg_order_value"),
        _max("order_date").alias("last_order_date")
    ) \
    .withColumn("created_at", current_timestamp())

customer_analytics.write.format("delta").mode("overwrite").save("s3a://gold/customer_analytics")
print(f"[SUCCESS] Customer Analytics: {customer_analytics.count():,} records")

# 5. Monthly Trends
print("\nCreating monthly_trends...")
from pyspark.sql.functions import date_format

monthly_trends = orders.filter(col("status") == "COMPLETED") \
    .join(order_items, "order_id") \
    .withColumn("month", date_format(col("order_date"), "yyyy-MM")) \
    .groupBy("month") \
    .agg(
        _sum("total_price").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        avg("total_price").alias("avg_order_value")
    ) \
    .orderBy("month") \
    .withColumn("created_at", current_timestamp())

monthly_trends.write.format("delta").mode("overwrite").save("s3a://gold/monthly_trends")
print(f"[SUCCESS] Monthly Trends: {monthly_trends.count():,} records")

print("\n=== Gold Layer Aggregation Complete ===")
spark.stop()