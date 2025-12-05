# Technical Documentation

## Architecture Deep Dive

### Data Lake Zones

#### 1. Raw Zone (`datalake/raw/`)
- **Purpose:** Store original, unmodified data
- **Format:** CSV, JSON as received from sources
- **Retention:** Permanent (for audit/replay)
- **Schema:** No enforcement

#### 2. Staging Zone (`datalake/staging/`)
- **Purpose:** Temporary storage for in-process data
- **Format:** Parquet
- **Retention:** 7-30 days
- **Schema:** Basic validation

#### 3. Processed Zone (`datalake/processed/`)
- **Purpose:** Cleaned, enriched, business-ready data
- **Format:** Parquet with partitioning
- **Retention:** Long-term
- **Schema:** Strictly enforced

#### 4. Warehouse Zone (`datalake/warehouse/`)
- **Purpose:** Aggregated analytics tables
- **Format:** Parquet optimized for queries
- **Retention:** Long-term
- **Schema:** Star/snowflake schema

---

## Spark ETL Pipeline Details

### Performance Optimizations

1. **Adaptive Query Execution**
```python
.config("spark.sql.adaptive.enabled", "true")
```

2. **Dynamic Partition Coalescing**
```python
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

3. **Broadcast Joins**
- Automatically used for small tables (<10MB)
- Product and customer dimensions are broadcasted

4. **Partition Management**
```python
# Write with partitioning
df.write.partitionBy("year", "month").parquet(path)
```

### Data Quality Checks

1. **Null Handling**
```python
sales_clean = sales_df.dropna(subset=['customer_id', 'product_id'])
```

2. **Status Filtering**
```python
sales_clean = sales_clean.filter(col('status') == 'Completed')
```

3. **Data Validation**
```python
# Validate record counts
assert processed_df.count() > 0, "No records in processed data"
```

### Transformations

#### Financial Calculations
```python
# Discount amount
discount_amount = price * quantity * discount_rate

# Gross amount (before discount)
gross_amount = price * quantity

# Net amount (after discount)
net_amount = gross_amount - discount_amount

# Profit
profit = net_amount - (cost * quantity)
```

#### Date Feature Engineering
```python
# Extract date components
df.withColumn('year', year(col('transaction_date')))
df.withColumn('month', month(col('transaction_date')))
df.withColumn('day', dayofmonth(col('transaction_date')))
```

#### Aggregations
```python
# Monthly revenue
revenue_monthly = df.groupBy('year', 'month').agg(
    sum('net_amount').alias('total_revenue'),
    count('order_id').alias('total_orders'),
    avg('net_amount').alias('avg_order_value')
)
```

---

## Airflow DAG Details

### Task Dependencies

```
validate_raw_data → check_spark → run_spark_etl
                                        ↓
                              validate_processed_data
                                        ↓
                              validate_warehouse_data
                                        ↓
                                 generate_report
                                        ↓
                              success_notification
```

### Retry Logic

```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

### Error Handling

- **On Failure:** Email notification (if configured)
- **On Retry:** Wait 5 minutes, then retry
- **Max Retries:** 2 attempts
- **On Success:** Generate execution report

### Scheduling

- **Default:** Daily at midnight (`@daily`)
- **Custom:** Edit in `config.yaml`

```yaml
airflow:
  schedule_interval: "0 0 * * *"  # Cron format
```

---

## Superset Configuration

### Data Source Connection

#### Option 1: Parquet via DuckDB

```python
# Install DuckDB support
pip install duckdb

# In Superset, add database:
# Database Type: DuckDB
# SQLAlchemy URI: duckdb:////path/to/datalake/warehouse/
```

#### Option 2: PostgreSQL

```python
# Load data to PostgreSQL
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:pass@localhost/db')
df.to_sql('table_name', engine, if_exists='replace')

# In Superset:
# Database Type: PostgreSQL
# SQLAlchemy URI: postgresql://user:pass@localhost/db
```

### Dashboard Creation Steps

1. **Add Database Connection**
   - Settings → Database Connections → + Database

2. **Add Dataset**
   - Data → Datasets → + Dataset
   - Select table from warehouse

3. **Create Charts**
   - Charts → + Chart
   - Select chart type and dataset

4. **Build Dashboard**
   - Dashboards → + Dashboard
   - Add charts to layout

### Recommended Charts

| Chart Type | Use Case | Dataset |
|------------|----------|---------|
| Line Chart | Revenue trends | revenue_by_month |
| Bar Chart | Category sales | revenue_by_category |
| Pie Chart | Payment methods | enriched_sales |
| Table | Top products | top_products |
| Map | Geographic sales | state_wise_sales |
| Big Number | Total revenue | revenue_by_month |

---

## Performance Tuning

### Spark Memory Configuration

For **4GB RAM system:**
```yaml
spark:
  driver_memory: "2g"
  executor_memory: "2g"
```

For **8GB RAM system:**
```yaml
spark:
  driver_memory: "4g"
  executor_memory: "4g"
```

For **16GB+ RAM system:**
```yaml
spark:
  driver_memory: "8g"
  executor_memory: "8g"
```

### Data Volume Guidelines

| Records | Recommended Spark Memory |
|---------|-------------------------|
| < 10K | 2GB |
| 10K - 100K | 4GB |
| 100K - 1M | 8GB |
| 1M+ | 16GB+ |

### Parquet Optimization

```python
# Enable compression
df.write.option("compression", "snappy").parquet(path)

# Optimize file size
df.coalesce(4).write.parquet(path)  # 4 files
```

---

## Monitoring & Logging

### Log Locations

```
logs/
├── airflow-webserver.log    # Airflow web UI logs
├── airflow-scheduler.log    # Airflow task scheduler logs
└── superset.log             # Superset application logs
```

### Spark Logs

```bash
# View Spark driver logs
tail -f logs/spark-etl.log

# View in Airflow UI
# Airflow → DAGs → spark_etl_pipeline → Task → Logs
```

### Monitoring Metrics

1. **ETL Duration:** Check execution time in logs
2. **Data Volume:** Monitor input/output record counts
3. **Data Quality:** Check validation pass rates
4. **System Resources:** Monitor CPU/memory usage

```bash
# Monitor system resources
htop  # CPU/Memory
df -h  # Disk space
```

---

## Security Best Practices

### 1. Credentials Management

```python
# Use environment variables
import os
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Use .env file
from dotenv import load_dotenv
load_dotenv()
```

### 2. Access Control

- Change default passwords immediately
- Implement role-based access in Airflow/Superset
- Restrict file permissions on data lake

```bash
chmod 700 datalake/
chmod 600 config/config.yaml
```

### 3. Data Encryption

```python
# Encrypt sensitive columns
from pyspark.sql.functions import sha2

df.withColumn('email_hash', sha2(col('email'), 256))
```

---

## Backup & Recovery

### Data Backup

```bash
#!/bin/bash
# Backup script
DATE=$(date +%Y%m%d)
tar -czf backup_${DATE}.tar.gz datalake/
```

### Recovery

```bash
# Restore from backup
tar -xzf backup_20240101.tar.gz
```

### Airflow Backup

```bash
# Backup Airflow database
cp -r airflow/airflow.db airflow/airflow.db.backup
```

---

## Testing Strategy

### Unit Tests

```python
import unittest
from spark_jobs.etl_pipeline import DataLakeETL

class TestETL(unittest.TestCase):
    def test_data_extraction(self):
        etl = DataLakeETL()
        sales_df, _, _ = etl.extract_data()
        self.assertGreater(sales_df.count(), 0)
```

### Integration Tests

```bash
# Run full pipeline test
./scripts/test_etl.sh

# Verify outputs
ls -lh datalake/warehouse/
```

### Data Quality Tests

```python
# Check for nulls
assert df.filter(col('customer_id').isNull()).count() == 0

# Check data ranges
assert df.filter(col('quantity') < 0).count() == 0
```

---

## Troubleshooting Guide

### Common Issues

#### 1. Spark Out of Memory

**Symptoms:** `java.lang.OutOfMemoryError`

**Solution:**
```yaml
# Reduce data or increase memory
spark:
  driver_memory: "8g"
```

#### 2. Airflow Tasks Stuck

**Symptoms:** Tasks in "queued" state

**Solution:**
```bash
# Restart scheduler
./scripts/stop_project.sh
./scripts/run_project.sh
```

#### 3. Port Already in Use

**Symptoms:** `Address already in use`

**Solution:**
```bash
# Find and kill process
sudo lsof -i :8080
kill -9 <PID>
```

#### 4. Data Not Appearing in Superset

**Symptoms:** Empty tables

**Solution:**
```bash
# Refresh metadata
# In Superset: Data → Datasets → Refresh
```

---

## Scaling Considerations

### Horizontal Scaling

To scale to multiple machines:

1. **Setup Spark Cluster**
   - Deploy Spark standalone/YARN cluster
   - Update `spark.master` in config

2. **Distributed Storage**
   - Use HDFS or S3 instead of local filesystem
   - Update paths in config

3. **Airflow Executor**
   - Switch from Sequential to Celery executor
   - Add Redis/RabbitMQ for task queue

### Vertical Scaling

For larger datasets on single machine:

1. Increase RAM allocation
2. Use SSD for faster I/O
3. Optimize Spark partitions
4. Enable Spark shuffle service

---

## Extension Ideas

### 1. Real-Time Processing
- Add Apache Kafka
- Implement streaming pipeline
- Use Spark Structured Streaming

### 2. Machine Learning
- Add recommendation engine
- Implement churn prediction
- Forecast sales trends

### 3. Data Quality Framework
- Implement Great Expectations
- Add data profiling
- Create data quality dashboards

### 4. Cost Optimization
- Implement data lifecycle policies
- Add incremental processing
- Optimize storage formats

---

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Superset Documentation](https://superset.apache.org/docs/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
