from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Simple_ETL").getOrCreate()

# ---------------------------
# Load Data
# ---------------------------
customers = spark.read.csv("/datalake/raw/customers.csv", header=True)
sales = spark.read.csv("/datalake/raw/sales.csv", header=True)
products = spark.read.json("/datalake/raw/products.json")

# Fix types
customers = customers.withColumn("customer_id", col("customer_id").cast("int"))
sales = sales.withColumn("product_id", col("product_id").cast("int")) \
             .withColumn("customer_id", col("customer_id").cast("int"))
products = products.withColumn("product_id", col("product_id").cast("int"))

# ---------------------------
# Join Final Table
# ---------------------------
df = sales.join(customers, "customer_id", "left") \
          .join(products, "product_id", "left")

# ---------------------------
# Save as Parquet
# ---------------------------
df.write.mode("overwrite").parquet("/datalake/analytics/sales_fact")

print("ETL DONE!")

