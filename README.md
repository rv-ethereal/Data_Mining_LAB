# On-Premises Data Lake

A data lake solution with Apache Spark ETL, Apache Airflow orchestration, and Apache Superset analytics dashboard.

## Overview

Processes sales and customer data through three layers:
- **Raw**: Source CSV files
- **Processed**: Cleaned data (Parquet format)
- **Warehouse**: Aggregated analytics tables

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Raw Data Layer                       │
│        (sales.csv, customers.csv)                       │
└──────────────────────────┬──────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│          Apache Airflow Orchestration                   │
│  ┌─────────────┐  ┌──────────┐  ┌─────────────────┐   │
│  │ File Check  │─▶│Spark ETL │─▶│ Output Validate │   │
│  └─────────────┘  └──────────┘  └─────────────────┘   │
└──────────────────────────┬──────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│            Processed Data Layer                         │
│  sales_clean | customers_clean | sales_with_customers  │
└──────────────────────────┬──────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│             Warehouse Analytics Layer                   │
│  revenue_by_product | revenue_by_region                 │
│  payment_analysis | status_summary                      │
│  customer_summary | monthly_sales                       │
└──────────────────────────┬──────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│         Apache Superset (Analytics Dashboard)           │
└─────────────────────────────────────────────────────────┘
```

## Tech Stack

- Apache Spark 3.5.0+ (ETL processing)
- Apache Airflow 2.6.3 (Orchestration)
- Apache Superset (Visualization)
- Python 3.11+, Pandas, PyArrow, SQLAlchemy

## Quick Start

1. **Install Dependencies**
```powershell
pip install -r requirements.txt
```

2. **Initialize Airflow**
```powershell
.\init_superset.ps1
```

3. **Prepare Data** - Place files in `datalake/raw/`:
   - `customers.csv`
   - `sales.csv`

4. **Start Services**
```powershell
airflow standalone  # http://localhost:8080
.\run_superset.ps1  # http://localhost:8088
```

## Project Structure

```
onprem-datalake/
├── airflow/dags/spark_etl_dag.py       # Daily ETL workflow
├── spark/spark_etl.py                  # ETL logic
├── datalake/
│   ├── raw/                  # Source CSV
│   ├── processed/            # Cleaned (Parquet)
│   └── warehouse/            # Analytics tables
├── app.py                    # Superset app
├── superset_config.py        # Config
└── requirements.txt
```

## ETL Pipeline

**DAG**: `data_lake_etl_pipeline` (daily)

**Tasks:**
1. `check_input_files` - Verify data exists
2. `run_spark_etl` - Transform & clean
3. `validate_warehouse_output` - Confirm tables created

## Analytics Tables

| Table | Purpose |
|-------|---------|
| revenue_by_product | Sales metrics per product |
| revenue_by_region | Regional performance |
| payment_analysis | Payment method breakdown |
| status_summary | Order status distribution |
| customer_summary | Customer purchase history |
| monthly_sales | Time-series trend |

## Execution Commands

### Setup & Installation
```powershell
# Install dependencies
pip install -r requirements.txt

# Initialize Airflow
.\init_superset.ps1

# Set Airflow home directory (optional)
[Environment]::SetEnvironmentVariable("AIRFLOW_HOME", "$PWD\airflow", "User")
```

### Start Services
```powershell
# Terminal 1: Start Airflow webserver
airflow webserver --port 8080

# Terminal 2: Start Airflow scheduler
airflow scheduler

# Terminal 3: Start Superset
.\run_superset.ps1
```

### Or Use Standalone Mode
```powershell
# Single command (includes webserver + scheduler)
airflow standalone
```

### Trigger ETL Pipeline
```powershell
# Via CLI (trigger manually)
airflow dags trigger data_lake_etl_pipeline

# Or access Airflow UI at http://localhost:8080
# Login → data_lake_etl_pipeline DAG → Trigger
```

### Run Spark ETL Directly
```powershell
# Execute ETL standalone
spark-submit spark/spark_etl.py

# With custom memory allocation
spark-submit --driver-memory 4g --executor-memory 4g spark/spark_etl.py

# With local cluster (8 cores)
spark-submit --master local[8] spark/spark_etl.py
```

### Check Execution Status
```powershell
# View Airflow logs
airflow logs --dag-id data_lake_etl_pipeline

# View specific task logs
airflow logs --dag-id data_lake_etl_pipeline --task-id run_spark_etl

# List all DAG runs
airflow dags list-runs --dag-id data_lake_etl_pipeline

# Check warehouse tables created
ls datalake/warehouse/
```

## Query Data

### SQL in Superset
```sql
-- Top 10 Products by Revenue
SELECT product, total_revenue, total_quantity 
FROM revenue_by_product 
ORDER BY total_revenue DESC 
LIMIT 10;

-- Revenue by Region
SELECT region, total_revenue, total_orders 
FROM revenue_by_region 
ORDER BY total_revenue DESC;

-- Customer Spending Analysis
SELECT customer_name, customer_city, total_spent, total_purchases
FROM customer_summary
ORDER BY total_spent DESC
LIMIT 20;
```

### Pandas
```python
import pandas as pd

# Load revenue by product
df = pd.read_parquet("datalake/warehouse/revenue_by_product/data.parquet")
print(df.head(10))

# Load customer summary
df = pd.read_parquet("datalake/warehouse/customer_summary/data.parquet")
print(df.sort_values('total_spent', ascending=False).head())

# Load monthly sales trend
df = pd.read_parquet("datalake/warehouse/monthly_sales/data.parquet")
print(df)
```

### PySpark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("QueryWarehouse").getOrCreate()

# Query revenue by product
df = spark.read.parquet("datalake/warehouse/revenue_by_product")
df.show(10, truncate=False)

# Query customer summary
df = spark.read.parquet("datalake/warehouse/customer_summary")
df.filter(df.total_spent > 1000).show()

# Perform SQL queries
df.createOrReplaceTempView("revenue_by_product")
spark.sql("SELECT * FROM revenue_by_product WHERE total_revenue > 500").show()
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| File not found | Verify CSV files in `datalake/raw/` |
| Java error | Install Java 17+, set JAVA_HOME |
| DAG not visible | Restart Airflow webserver |
| Memory error | Increase: `--driver-memory 4g` |

## Documentation

- [Apache Spark](https://spark.apache.org/docs/latest/)
- [Apache Airflow](https://airflow.apache.org/docs/)
- [Apache Superset](https://superset.apache.org/)

## License

## Author:
Gaurav Kumar(MSA24002)
DATA MINING AND WAREHOUSING ASSIGNEMT 
