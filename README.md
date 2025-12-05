# 🚀 On-Premise Data Lake with Apache Spark ETL Pipeline

**Student Name:** Bhawana Dhaka  
**Roll Number:** MSD24002  
**Course:** Data Mining Lab  
**Project:** End-to-End Data Engineering Pipeline

---

## 📋 Project Overview

This project demonstrates a complete data engineering workflow using Apache Spark for ETL (Extract, Transform, Load) operations, Apache Airflow for workflow orchestration, and Apache Superset for data visualization.

**Key Achievement:** Successfully processes 10,000+ sales transactions with automated data quality checks and generates 5 analytical datasets in under 20 seconds.

---

## 🏗️ Architecture

```
Raw Data (CSV/JSON)
    ↓
Apache Spark ETL Processing
    ↓
Data Lake Storage (Parquet)
    ├── Raw Zone (Original data)
    ├── Staging Zone (Cleaned data)
    ├── Processed Zone (Transformed data)
    └── Warehouse Zone (Analytics-ready data)
    ↓
Apache Airflow Orchestration
    ↓
Apache Superset Visualization
```

---

## 🛠️ Technologies Used

- **Apache Spark 3.5.0** - Distributed data processing engine
- **Apache Airflow 2.10.4** - Workflow orchestration and scheduling
- **Apache Superset 4.1.1** - Business intelligence and data visualization
- **Python 3.11+** - Core programming language
- **PySpark** - Python API for Apache Spark
- **Parquet** - Columnar storage format
- **PostgreSQL 14** - Metadata database for Airflow
- **Docker & Docker Compose** - Containerization platform

---

## 📊 Project Features

### Data Pipeline
- Processes 10,000 sales transactions
- 1,000 customer records
- 100 product records
- 13 months of historical data

### ETL Operations
✅ Data extraction from CSV/JSON formats  
✅ Data quality checks (null removal, validation)  
✅ Feature engineering (financial metrics, date features)  
✅ Data enrichment (joins across multiple datasets)  
✅ Complex aggregations and analytics  
✅ Optimized Parquet storage with partitioning  

### Analytics Generated
1. **Monthly Revenue Analysis** - Revenue trends over time
2. **Category Performance** - Sales by product category
3. **Top Products** - Best-selling items by revenue
4. **Customer Analytics** - Lifetime value and segmentation
5. **Geographic Analysis** - State-wise sales distribution

---

## 📁 Project Structure

```
spark-airflow-datalake-project/
├── config/                         # Configuration files
│   └── config.yaml                # Project configuration
├── dags/                          # Airflow DAG definitions
│   ├── spark_etl_dag_docker.py   # Production DAG for Docker
│   └── spark_etl_dag_local.py.backup  # Backup for local execution
├── spark_jobs/                    # Spark ETL job scripts
│   └── etl_pipeline.py           # Main ETL pipeline
├── scripts/                       # Utility and setup scripts
│   ├── setup.sh                  # Complete local setup
│   ├── docker_setup.sh           # Docker environment setup
│   ├── run_project.sh            # Start all services (local)
│   ├── stop_project.sh           # Stop all services
│   ├── generate_data_fixed.py    # Generate sample data
│   ├── generate_data.py          # Alternative data generator
│   ├── install_pyspark.sh        # PySpark installation
│   └── test_etl.sh               # Test ETL pipeline
├── datalake/                      # Data lake storage
│   ├── raw/                      # Raw data (CSV, JSON)
│   │   ├── customers.csv
│   │   ├── products.json
│   │   └── sales.csv
│   ├── staging/                  # Staging area
│   ├── processed/                # Processed data
│   │   └── sales_enriched/      # Enriched sales data
│   └── warehouse/                # Analytics warehouse
│       ├── customer_analytics/
│       ├── revenue_by_category/
│       ├── revenue_by_month/
│       ├── state_wise_sales/
│       └── top_products/
├── logs/                          # Application logs
│   ├── dag_id=spark_etl_pipeline_msd24002/
│   ├── dag_processor_manager/
│   └── scheduler/
├── docs/                          # Documentation
│   ├── QUICK_REFERENCE.md
│   └── TECHNICAL_DOCS.md
├── docker-compose.yml             # Docker orchestration
├── Dockerfile.airflow             # Custom Airflow image
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

---

## 🚀 Complete End-to-End Setup Guide

### Prerequisites

**System Requirements:**
- **OS:** Linux/Ubuntu 20.04+ (Recommended) or macOS
- **RAM:** Minimum 8GB (16GB recommended)
- **Disk Space:** 10GB free space
- **CPU:** 4+ cores recommended

**Software Requirements:**
- **Python:** 3.11 or higher
- **Java:** OpenJDK 17 (required for Spark)
- **Docker:** Latest version (for containerized deployment)
- **Docker Compose:** v2.0+

---

## 📥 Installation Methods

### **Option 1: Docker Setup (Recommended for Production)**

This method runs Airflow, Spark, and Superset in isolated Docker containers.

#### Step 1: Clone/Navigate to Project Directory
```bash
cd /home/sushi/Downloads/spark-airflow-datalake-project
```

#### Step 2: Install Docker (if not installed)
```bash
# Install Docker
sudo apt-get update
sudo apt-get install -y docker.io docker-compose

# Add user to docker group
sudo usermod -aG docker $USER

# Restart session or run:
newgrp docker

# Verify installation
docker --version
docker-compose --version
```

#### Step 3: Set Environment Variables
```bash
# Create .env file with required variables
echo "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_GID=0" >> .env
```

#### Step 4: Create Required Directories
```bash
# Create directories with proper permissions
mkdir -p logs dags plugins datalake/{raw,staging,processed,warehouse}
chmod -R 777 logs dags plugins datalake
```

#### Step 5: Generate Sample Data
```bash
# Install Python dependencies for data generation
pip install pandas numpy faker pyyaml

# Generate sample data
python3 scripts/generate_data_fixed.py
```

Expected output:
```
Generating 1000 customer records...
✓ Customers saved to datalake/raw/customers.csv
Generating 100 product records...
✓ Products saved to datalake/raw/products.json
Generating 10000 sales records...
✓ Sales saved to datalake/raw/sales.csv
```

#### Step 6: Build Docker Images
```bash
# Build custom Airflow image with Spark
docker-compose build
```

#### Step 7: Initialize Airflow Database
```bash
# Initialize Airflow metadata database
docker-compose up airflow-init
```

Wait for: `airflow-init_1 exited with code 0`

#### Step 8: Start All Services
```bash
# Start all containers in detached mode
docker-compose up -d
```

#### Step 9: Verify Services
```bash
# Check running containers
docker-compose ps

# Expected output:
# airflow-webserver   running   0.0.0.0:8080->8080/tcp
# airflow-scheduler   running
# postgres            running   0.0.0.0:5432->5432/tcp
# superset            running   0.0.0.0:8088->8088/tcp
```

#### Step 10: Access Web Interfaces

**Apache Airflow:**
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

**Apache Superset:**
- URL: http://localhost:8088
- Username: `admin`
- Password: `admin`

#### Step 11: Trigger the ETL Pipeline

1. Open Airflow UI at http://localhost:8080
2. Find DAG: `spark_etl_pipeline_msd24002`
3. Toggle the DAG to "ON" (unpause)
4. Click "Trigger DAG" button (play icon)
5. Monitor execution in "Graph" or "Logs" view

#### Step 12: Verify Output

```bash
# Check processed data
docker-compose exec airflow-webserver ls -lh /opt/airflow/datalake/processed/

# Check warehouse data
docker-compose exec airflow-webserver ls -lh /opt/airflow/datalake/warehouse/

# View ETL logs
docker-compose logs airflow-scheduler | grep "ETL"
```

#### Step 13: Stop Services
```bash
# Stop all containers
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

---

### **Option 2: Local Setup (Development/Testing)**

This method runs everything directly on your local machine without Docker.

#### Step 1: Navigate to Project
```bash
cd /home/sushi/Downloads/spark-airflow-datalake-project
```

#### Step 2: Install Java (Required for Spark)
```bash
# Install OpenJDK 17
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk

# Set JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Verify installation
java -version
```

#### Step 3: Create Virtual Environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip setuptools wheel
```

#### Step 4: Install Python Dependencies
```bash
# Install all required packages
pip install -r requirements.txt

# This installs:
# - pyspark==3.5.0
# - apache-airflow==2.10.4
# - apache-superset==4.1.1
# - pandas, numpy, faker, pyyaml, etc.
```

#### Step 5: Setup Airflow
```bash
# Set Airflow home directory
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

#### Step 6: Update Configuration
```bash
# Update config.yaml with local paths
nano config/config.yaml

# Ensure paths section has local paths:
# paths:
#   project_root: "/home/sushi/Downloads/spark-airflow-datalake-project"
#   datalake_root: "/home/sushi/Downloads/spark-airflow-datalake-project/datalake"
```

#### Step 7: Generate Sample Data
```bash
# Generate sample datasets
python3 scripts/generate_data_fixed.py
```

#### Step 8: Test ETL Pipeline (Standalone)
```bash
# Update config path environment variable
export CONFIG_PATH="$(pwd)/config/config.yaml"

# Run ETL pipeline directly
python3 spark_jobs/etl_pipeline.py
```

Expected output:
```
============================================================
PHASE 1: EXTRACT - Reading raw data
============================================================
✓ Sales records loaded: 10000
✓ Products loaded: 100
✓ Customers loaded: 1000

============================================================
PHASE 2: TRANSFORM - Data cleaning and enrichment
============================================================
...

============================================================
ETL PIPELINE COMPLETED SUCCESSFULLY!
============================================================
Duration: 16.33 seconds
✓ Processed 8,528 completed transactions
✓ Created 5 analytical tables
```

#### Step 9: Start Airflow Services

**Terminal 1 - Start Airflow Webserver:**
```bash
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
```

**Terminal 2 - Start Airflow Scheduler:**
```bash
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

#### Step 10: Access Airflow UI
- Open browser: http://localhost:8080
- Login: admin / admin
- Enable and trigger DAG: `spark_etl_pipeline_msd24002`

#### Step 11: Setup Superset (Optional)

```bash
# Terminal 3 - Initialize Superset
source venv/bin/activate
export FLASK_APP=superset

# Initialize database
superset db upgrade

# Create admin user
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin

# Initialize Superset
superset init

# Start Superset server
superset run -h 0.0.0.0 -p 8088 --with-threads
```

Access Superset: http://localhost:8088

---

## 🔍 Verification & Testing

### Verify Data Generation
```bash
# Check raw data files
ls -lh datalake/raw/
# Should show: customers.csv, products.json, sales.csv

# View sample data
head -5 datalake/raw/customers.csv
head -5 datalake/raw/sales.csv
```

### Verify ETL Output
```bash
# Check processed data
ls -lh datalake/processed/sales_enriched/

# Check warehouse tables
ls -lh datalake/warehouse/

# Should show:
# - customer_analytics/
# - revenue_by_category/
# - revenue_by_month/
# - state_wise_sales/
# - top_products/
```

### View Generated Analytics
```bash
# View revenue by month (CSV)
cat revenue_by_month.csv

# View top products
cat top_products.csv

# View customer analytics
cat customer_analytics.csv
```

### Check Airflow DAG Status
```bash
# List all DAGs
airflow dags list

# Check DAG status
airflow dags show spark_etl_pipeline_msd24002

# View recent DAG runs
airflow dags list-runs
```

---

## 📊 Expected Results & Outputs

### Data Quality Metrics
- **Input Records:** 10,000 sales transactions
- **Valid Records:** 8,528 (after quality checks)
- **Data Quality:** 85.28% completion rate
- **Processing Time:** ~16-20 seconds

### Analytics Output Files

The pipeline generates 5 analytical CSV files in the project root:

1. **revenue_by_month.csv** - Monthly revenue trends
2. **revenue_by_category.csv** - Category-wise performance
3. **top_products.csv** - Top 10 products by revenue
4. **customer_analytics.csv** - Customer lifetime value
5. **state_wise_sales.csv** - Geographic sales distribution

---

## 🛠️ Troubleshooting Guide

### Common Issues and Solutions

#### Issue 1: Docker Permission Denied
```bash
# Solution: Add user to docker group
sudo usermod -aG docker $USER
newgrp docker
```

#### Issue 2: Port Already in Use
```bash
# Check what's using port 8080
sudo lsof -i :8080

# Kill the process
kill -9 <PID>

# Or change Airflow port in docker-compose.yml
```

#### Issue 3: Java Not Found (Local Setup)
```bash
# Install OpenJDK 17
sudo apt-get install openjdk-17-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

#### Issue 4: Airflow DAG Not Appearing
```bash
# Check DAG file syntax
python3 dags/spark_etl_dag_docker.py

# Refresh Airflow UI or restart scheduler
docker-compose restart airflow-scheduler
```

#### Issue 5: Out of Memory Error
```bash
# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory → 8GB+

# Or modify config.yaml to reduce Spark memory:
spark:
  driver_memory: "2g"
  executor_memory: "2g"
```

#### Issue 6: Data Not Generated
```bash
# Check if datalake/raw directory exists
mkdir -p datalake/raw

# Regenerate data
python3 scripts/generate_data_fixed.py

# Verify files
ls -lh datalake/raw/
```

---

## 🔧 Advanced Configuration

### Customize Data Generation

Edit `scripts/generate_data_fixed.py`:

```python
# Modify these parameters
num_customers = 1000    # Change customer count
num_products = 100      # Change product count
num_sales = 10000       # Change sales transactions
```

### Modify ETL Logic

Edit `spark_jobs/etl_pipeline.py` to add custom transformations:

```python
# Add new analytics in transform_data() method
# Example: Calculate average order value
avg_order_df = sales_df.groupBy("customer_id").agg(
    avg("total_amount").alias("avg_order_value")
)
```

### Change Airflow Schedule

Edit `dags/spark_etl_dag_docker.py`:

```python
dag = DAG(
    dag_id='spark_etl_pipeline_msd24002',
    schedule_interval='@daily',    # Change to '@hourly', '@weekly', etc.
    # ...
)
```

### Configure Spark Resources

Edit `config/config.yaml`:

```yaml
spark:
  app_name: "DataLake-ETL-Pipeline"
  master: "local[*]"              # Use all cores
  driver_memory: "4g"             # Adjust based on RAM
  executor_memory: "4g"
  executor_cores: 2
```

---

## 📊 Monitoring and Logs

### View Logs (Docker Setup)

```bash
# Airflow webserver logs
docker-compose logs -f airflow-webserver

# Airflow scheduler logs
docker-compose logs -f airflow-scheduler

# Superset logs
docker-compose logs -f superset

# All logs combined
docker-compose logs -f
```

### View Logs (Local Setup)

```bash
# Airflow logs directory
ls -lh logs/dag_id=spark_etl_pipeline_msd24002/

# View specific task log
cat logs/dag_id=spark_etl_pipeline_msd24002/run_id=*/task_id=run_spark_etl/attempt=1.log

# Real-time scheduler logs
tail -f airflow/logs/scheduler/latest/*.log
```

### Monitor System Resources

```bash
# Check CPU and memory usage
htop

# Check disk space
df -h

# Check Docker container resources
docker stats
```

---

## 🎯 Project Highlights & Achievements

### Technical Accomplishments
✅ Implemented complete ETL pipeline with Spark  
✅ Automated workflow orchestration with Airflow  
✅ Built scalable data lake architecture  
✅ Achieved 85%+ data quality rate  
✅ Optimized processing time under 20 seconds  
✅ Generated 5 analytical datasets  
✅ Containerized deployment with Docker  

### Data Engineering Best Practices
✅ Separation of raw, staging, processed, and warehouse zones  
✅ Data quality checks and validation  
✅ Error handling and logging  
✅ Modular and maintainable code  
✅ Configuration management (YAML)  
✅ Parquet format for efficient storage  
✅ Partitioning for query optimization  

### Skills Demonstrated
- **Data Engineering:** ETL pipeline development
- **Big Data:** Apache Spark (PySpark)
- **Orchestration:** Apache Airflow DAGs
- **Containerization:** Docker & Docker Compose
- **Data Modeling:** Star schema, dimensional modeling
- **Programming:** Python, SQL, Bash scripting
- **Data Quality:** Validation, cleansing, deduplication
- **DevOps:** CI/CD concepts, automation

---

## 💡 Key Learnings

1. **Data Lake Architecture** - Understanding multi-zone data lake with raw, staging, processed, and warehouse layers
2. **Apache Spark** - Distributed data processing, transformations, and optimizations
3. **Apache Airflow** - DAG design, task dependencies, and scheduling
4. **ETL Best Practices** - Data quality checks, error handling, and logging
5. **Parquet Optimization** - Columnar storage benefits for analytical queries
6. **Docker Containerization** - Building production-ready containerized applications
7. **Data Quality Management** - Implementing validation and cleansing strategies

---

## 📚 Documentation & Resources

### Project Documentation
- **README.md** - Complete project guide (this file)
- **docs/TECHNICAL_DOCS.md** - Technical architecture and design
- **docs/QUICK_REFERENCE.md** - Command reference and cheat sheet

### Official Documentation
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Superset Documentation](https://superset.apache.org/docs/)
- [Docker Documentation](https://docs.docker.com/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

### Useful Commands Reference

```bash
# Docker Commands
docker-compose up -d              # Start services
docker-compose down               # Stop services
docker-compose ps                 # Check status
docker-compose logs -f            # View logs
docker-compose restart            # Restart services

# Airflow Commands
airflow dags list                 # List all DAGs
airflow dags trigger <dag_id>     # Trigger DAG
airflow tasks test <dag> <task>   # Test single task
airflow db init                   # Initialize DB

# Data Commands
python3 scripts/generate_data_fixed.py    # Generate data
python3 spark_jobs/etl_pipeline.py        # Run ETL
ls -lh datalake/warehouse/                # Check output
```

---

## 🚦 Quick Start Summary

### For Docker (Production):
```bash
cd spark-airflow-datalake-project
python3 scripts/generate_data_fixed.py
docker-compose up -d
# Access: http://localhost:8080 (Airflow)
# Access: http://localhost:8088 (Superset)
```

### For Local (Development):
```bash
cd spark-airflow-datalake-project
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
python3 scripts/generate_data_fixed.py
python3 spark_jobs/etl_pipeline.py
```

---

## 📧 Contact & Support

**Student Information:**  
- **Name:** Bhawana Dhaka  
- **Roll Number:** MSD24002  
- **Course:** Data Mining Lab  
- **Institution:** [Your Institution Name]

For questions or issues, please refer to the documentation in the `docs/` directory.

---

## 📄 License

This project is created for academic purposes as part of the Data Mining Lab coursework.

---

## 🙏 Acknowledgments

- Apache Software Foundation for Spark, Airflow, and Superset
- Data Mining Lab instructors and mentors
- Open-source community for tools and libraries

---

**Last Updated:** December 2025  
**Project Version:** 1.0.0

---
