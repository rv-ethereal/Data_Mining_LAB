# Quick Reference Guide

## 🚀 Quick Commands

### Installation
```bash
chmod +x scripts/*.sh
./scripts/setup.sh
```

### Start Services
```bash
./scripts/run_project.sh
```

### Stop Services
```bash
./scripts/stop_project.sh
```

### Test ETL
```bash
./scripts/test_etl.sh
```

### Generate New Data
```bash
source venv/bin/activate
python3 scripts/generate_data.py
```

### Run ETL Manually
```bash
source venv/bin/activate
python3 spark_jobs/etl_pipeline.py
```

---

## 🌐 Access URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |

---

## 📁 Important Paths

```bash
# Project root
/home/claude/spark-airflow-datalake-project

# Data lake zones
datalake/raw/           # Original data
datalake/staging/       # Temporary
datalake/processed/     # Cleaned data
datalake/warehouse/     # Analytics tables

# Code
spark_jobs/             # Spark ETL scripts
dags/                   # Airflow DAGs
scripts/                # Helper scripts

# Configs
config/config.yaml      # Main configuration

# Logs
logs/                   # All application logs
```

---

## 🔧 Common Tasks

### Change ETL Schedule

Edit `config/config.yaml`:
```yaml
airflow:
  schedule_interval: "@hourly"  # or "0 */6 * * *" for every 6 hours
```

### Adjust Data Size

Edit `config/config.yaml`:
```yaml
data_generation:
  num_customers: 5000      # More customers
  num_sales_records: 50000  # More sales
```

### Increase Spark Memory

Edit `config/config.yaml`:
```yaml
spark:
  driver_memory: "8g"
  executor_memory: "8g"
```

---

## 📊 Data Flow

```
Raw CSV/JSON
    ↓
Spark Extract (Read)
    ↓
Spark Transform (Clean, Join, Aggregate)
    ↓
Spark Load (Write Parquet)
    ↓
Warehouse (Analytics Tables)
    ↓
Superset (Visualize)
```

---

## 🐛 Quick Fixes

### "Java not found"
```bash
sudo apt-get install openjdk-11-jdk
```

### "Port already in use"
```bash
sudo lsof -i :8080
kill -9 <PID>
```

### "Out of memory"
```bash
# Reduce data size in config.yaml OR
# Increase Spark memory in config.yaml
```

### "Airflow tasks not running"
```bash
./scripts/stop_project.sh
./scripts/run_project.sh
```

### "Virtual environment issues"
```bash
rm -rf venv/
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 📝 Airflow Operations

### Trigger DAG Manually
```bash
# CLI
airflow dags trigger spark_etl_pipeline

# Or use Airflow UI
# DAGs → spark_etl_pipeline → Trigger DAG
```

### View Task Logs
```bash
# Airflow UI → DAGs → Tasks → Logs
# OR check logs/airflow-scheduler.log
```

### Pause/Unpause DAG
```bash
airflow dags pause spark_etl_pipeline
airflow dags unpause spark_etl_pipeline
```

---

## 📈 Superset Quick Start

### Add Database
1. Settings → Database Connections
2. + Database
3. DuckDB or PostgreSQL
4. Test connection

### Create Dataset
1. Data → Datasets
2. + Dataset
3. Select database and table

### Create Chart
1. Charts → + Chart
2. Select dataset and chart type
3. Configure and save

### Create Dashboard
1. Dashboards → + Dashboard
2. Add charts
3. Arrange layout
4. Save

---

## 🔍 Monitoring

### Check ETL Status
```bash
# View logs
tail -f logs/spark-etl.log

# Check Airflow UI
# DAGs → spark_etl_pipeline → Graph View
```

### Check Data Quality
```bash
# Count records
ls -lh datalake/processed/sales_enriched/
ls -lh datalake/warehouse/

# Or use Python
python3 -c "from pyspark.sql import SparkSession; \
spark = SparkSession.builder.getOrCreate(); \
df = spark.read.parquet('datalake/processed/sales_enriched'); \
print(f'Records: {df.count()}'); \
spark.stop()"
```

### System Resources
```bash
# CPU and Memory
htop

# Disk space
df -h

# Spark UI (when running)
http://localhost:4040
```

---

## 🛠️ Development

### Edit ETL Logic
```bash
# Edit Spark job
nano spark_jobs/etl_pipeline.py

# Test changes
python3 spark_jobs/etl_pipeline.py
```

### Modify DAG
```bash
# Edit DAG
nano dags/spark_etl_dag.py

# Copy to Airflow
cp dags/spark_etl_dag.py airflow/dags/

# Refresh in Airflow UI
```

### Add New Transformation
```python
# In etl_pipeline.py, add to transform_data():
new_metric = enriched_df.groupBy('column').agg(...)
return {
    'existing': ...,
    'new_metric': new_metric  # Add here
}

# In load_data(), add:
tables.append(('new_metric', 'new_table_name'))
```

---

## 📦 File Formats

### CSV (Raw Zone)
- Human-readable
- Easy to inspect
- Used for input

### JSON (Raw Zone)
- Nested structure support
- Used for complex data

### Parquet (Processed/Warehouse)
- Columnar format
- Compressed
- Fast queries
- Industry standard

---

## ⚡ Performance Tips

1. **Use Parquet** - 10x faster than CSV
2. **Partition data** - By year/month for large datasets
3. **Broadcast small tables** - Automatic in joins
4. **Cache frequently used data** - `df.cache()`
5. **Monitor Spark UI** - Check for skew

---

## 🎓 Learning Path

### Beginner
1. Understand data flow
2. Run the project
3. Explore generated data
4. View Superset dashboards

### Intermediate
1. Modify transformations
2. Add new aggregations
3. Create custom charts
4. Adjust scheduling

### Advanced
1. Optimize Spark performance
2. Add data quality checks
3. Implement incremental loads
4. Scale to cluster

---

## 📞 Need Help?

### Check Logs
```bash
# Airflow
tail -f logs/airflow-scheduler.log

# Superset
tail -f logs/superset.log

# ETL
# Check Airflow UI → Task Logs
```

### Reset Everything
```bash
./scripts/stop_project.sh
rm -rf venv/ airflow/ datalake/*
./scripts/setup.sh
```

---

## 📝 Notes

- **Default ports:** Airflow (8080), Superset (8088)
- **Default credentials:** admin/admin (change in production!)
- **Data location:** `datalake/` directory
- **Logs location:** `logs/` directory
- **Config file:** `config/config.yaml`

---

**🌟 Pro Tip:** Always activate virtual environment before running scripts!
```bash
source venv/bin/activate
```
