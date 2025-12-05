# 🎉 Data Lakehouse - System Ready!

## ✅ All Systems Operational

### Services Status
| Service | Status | URL | Credentials |
|---------|--------|-----|-------------|
| **Airflow** | ✅ Running | http://localhost:8082 | admin/admin |
| **Superset** | ✅ Running | http://localhost:8088 | admin/admin |
| **Spark Master** | ✅ Running | http://localhost:8080 | - |
| **Spark Worker** | ✅ Running | http://localhost:8081 | - |
| **MinIO** | ✅ Healthy | http://localhost:9001 | admin/minio123 |
| **Trino** | ✅ Healthy | http://localhost:8083 | - |
| **PostgreSQL** | ✅ Healthy | Internal | - |

### Recent Fixes
- ✅ **Superset Login** - Admin user created
- ✅ **Docker API** - Airflow containers upgraded to Docker CLI v26.1.3
- ✅ **Sample Data** - 4 CSV files (7.2 MB) uploaded to MinIO

### Data Uploaded
- ✅ customers.csv (652 KB)
- ✅ products.csv (31 KB)  
- ✅ orders.csv (1.6 MB)
- ✅ order_items.csv (4.9 MB)

## 🚀 Next Steps

### Option 1: Use Airflow UI (Recommended)

1. **Open Airflow**: http://localhost:8082
2. **Login**: admin/admin
3. **Find DAG**: Look for `lakehouse_pipeline`
4. **Enable**: Toggle the switch to enable the DAG
5. **Trigger**: Click the "▶" Play button to run
6. **Monitor**: Watch in Grid or Graph view

### Option 2: Command Line

```bash
# Trigger the pipeline
docker exec lakehouse-airflow-webserver airflow dags trigger lakehouse_pipeline

# Monitor status
docker exec lakehouse-airflow-webserver airflow dags list-runs -d lakehouse_pipeline
```

### Option 3: Manual Execution (For Testing)

If you want to run Spark jobs directly without Airflow:

#### Bronze Layer
```bash
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/delta-core_2.12-2.4.0.jar,/opt/spark/extra-jars/delta-storage-2.4.0.jar,/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-apps/jobs/bronze_ingestion.py
```

#### Silver Layer
```bash
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/delta-core_2.12-2.4.0.jar,/opt/spark/extra-jars/delta-storage-2.4.0.jar,/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-apps/jobs/silver_transformation.py
```

#### Gold Layer
```bash
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/extra-jars/delta-core_2.12-2.4.0.jar,/opt/spark/extra-jars/delta-storage-2.4.0.jar,/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-apps/jobs/gold_aggregation.py
```

## 📊 After Pipeline Runs

### Query Data in Trino

```bash
# Connect to Trino CLI
docker exec -it lakehouse-trino trino

# Run queries
SHOW SCHEMAS FROM delta;
USE delta.gold;
SHOW TABLES;
SELECT * FROM daily_sales LIMIT 10;
```

### Create Superset Dashboard

1. **Login to Superset**: http://localhost:8088 (admin/admin)

2. **Add Database Connection**:
   - Click **Settings** → **Database Connections**
   - Click **+ Database**
   - Select **Trino** from the list
   - **SQLAlchemy URI**: `trino://admin@trino:8080/delta`
   - Click **Test Connection**
   - Click **Connect**

3. **Create Dataset**:
   - Go to **Data** → **Datasets**
   - Click **+ Dataset**
   - **Database**: Select your Trino connection
   - **Schema**: `gold`
   - **Table**: `daily_sales` (or other tables)
   - Click **Add**

4. **Create Charts**:
   - Go to **Charts** → **+ Chart**
   - Select your dataset
   - Choose visualization type (Bar Chart, Line Chart, etc.)
   - Configure metrics and filters
   - Save to dashboard

## 🔍 Monitoring & Debugging

### Check Airflow Logs
```bash
docker logs lakehouse-airflow-scheduler --tail 50
docker logs lakehouse-airflow-webserver --tail 50
```

### Check Spark Logs
```bash
docker logs lakehouse-spark-master --tail 50
docker logs lakehouse-spark-worker --tail 50
```

### Check MinIO Buckets
```bash
docker exec lakehouse-minio mc ls minio/bronze/
docker exec lakehouse-minio mc ls minio/silver/
docker exec lakehouse-minio mc ls minio/gold/
```

### Verify Trino Connection
```bash
curl http://localhost:8083/v1/info
```

## 📁 Project Structure

```
MSD24001-L1/
├── airflow/
│   ├── dags/
│   │   └── lakehouse_pipeline.py  # Main ETL DAG
│   ├── logs/                       # Airflow logs
│   ├── plugins/                    # Custom plugins
│   └── Dockerfile                  # Custom Airflow with updated Docker CLI
├── spark/
│   ├── jobs/
│   │   ├── bronze_ingestion.py      # Raw data ingestion
│   │   ├── silver_transformation.py # Data quality & cleaning
│   │   └── gold_aggregation.py      # Business metrics
│   └── jars/                        # Delta Lake JARs
├── data/
│   └── sample/
│       ├── generator.py             # Sample data generator
│       ├── customers.csv
│       ├── products.csv
│       ├── orders.csv
│       └── order_items.csv
├── trino/
│   └── catalog/
│       └── delta.properties         # Delta Lake connector config
├── superset/
│   └── Dockerfile                   # Custom Superset with Trino driver
├── scripts/
│   └── upload_to_minio.py          # Data upload utility
├── docker-compose.yml               # Container orchestration
├── .env                            # Environment variables
├── README.md                       # Full documentation
└── QUICKSTART.md                   # This file
```

## 🎯 Expected Pipeline Results

After running the full pipeline, you will have:

### Bronze Layer
- Raw CSV data ingested from MinIO
- Stored in Delta Lake format
- Full data lineage preserved

### Silver Layer
- Cleaned and validated data
- Data quality checks applied
- Standardized schemas

### Gold Layer (Business Metrics)
- `daily_sales` - Daily revenue & transactions
- `product_performance` - Product sales analytics
- `customer_analytics` - Customer behavior metrics

## 🛠️ Troubleshooting

### Airflow DAG Not Appearing
```bash
# Refresh DAGs
docker exec lakehouse-airflow-webserver airflow dags list

# Check for import errors
docker exec lakehouse-airflow-webserver airflow dags list-import-errors
```

### Spark Job Fails
```bash
# Check if JARs exist
docker exec lakehouse-spark-master ls -la /opt/spark/extra-jars/

# Test manual submission
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit --version
```

### Trino Can't Query Delta Tables
```bash
# Check Trino catalog
docker logs lakehouse-trino | grep delta

# Verify metastore location in MinIO
docker exec lakehouse-minio mc ls minio/gold/metastore/
```

### Superset Connection Error
1. Verify Trino is running: `curl http://localhost:8083/v1/info`
2. Check URI format: `trino://admin@trino:8080/delta`
3. Ensure no typos in database name (`delta` not `Delta`)

## 📚 Additional Resources

- **Airflow Docs**: https://airflow.apache.org/docs/
- **Spark Docs**: https://spark.apache.org/docs/latest/
- **Delta Lake**: https://docs.delta.io/
- **Trino**: https://trino.io/docs/current/
- **Superset**: https://superset.apache.org/docs/intro

---

## 🎉 You're All Set!

Your data lakehouse is now fully operational with:
- ✅ Automated ETL orchestration via Airflow
- ✅ Distributed processing with Spark
- ✅ ACID transactions with Delta Lake
- ✅ Fast SQL queries with Trino
- ✅ Interactive dashboards with Superset

Start by triggering the pipeline in Airflow or run the manual commands above!

**Happy Data Engineering! 🚀**
