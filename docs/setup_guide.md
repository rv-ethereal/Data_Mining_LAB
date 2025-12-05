# Setup Guide - Windows

This guide provides step-by-step instructions for setting up the On-Premise Data Lake project on Windows.

## Prerequisites Installation

### 1. Python 3.11+

Download and install from [python.org](https://www.python.org/downloads/)

```powershell
# Verify installation
python --version
```

### 2. Docker Desktop

Download and install from [docker.com](https://www.docker.com/products/docker-desktop/)

```powershell
# Verify installation
docker --version
docker-compose --version
```

### 3. Java 8+ (for Spark)

Download and install from [adoptium.net](https://adoptium.net/)

```powershell
# Verify installation
java -version

# Set JAVA_HOME environment variable
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-11.0.x-hotspot"
```

### 4. PostgreSQL 13+ (Optional - can use Docker)

Download from [postgresql.org](https://www.postgresql.org/download/windows/)

Or use Docker:

```powershell
docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:13
```

## Project Setup

### Step 1: Navigate to Project Directory

```powershell
cd "c:\Users\tb619\Videos\Dm assignment"
```

### Step 2: Setup Virtual Environment (Recommended)

**Option A: Automated Setup (Easiest)**

```powershell
# Run the setup script
.\setup.ps1
```

This will automatically:
- Create a virtual environment in `venv/`
- Upgrade pip
- Install all dependencies from `requirements.txt`
- Verify the installation

**Option B: Manual Setup**

```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# You should see (venv) in your prompt

# Upgrade pip
python -m pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Verify Virtual Environment

```powershell
# Check if virtual environment is active
# You should see (venv) at the start of your prompt

# Verify installations
python -c "import pyspark; print(f'PySpark: {pyspark.__version__}')"
python -c "import pandas; print(f'Pandas: {pandas.__version__}')"
python -c "import faker; print(f'Faker: {faker.__version__}')"
```

### Step 4: Configure Environment Variables (Optional)

This installs:
- PySpark 3.5.0
- Apache Airflow 2.8.0
- PostgreSQL driver
- Pandas, Faker, and utilities

### Step 4: Configure Environment Variables

```powershell
# Copy example environment file
copy .env.example .env

# Edit .env with your settings (use notepad or any editor)
notepad .env
```

Update these values if needed:
- PostgreSQL credentials
- Data lake paths
- Spark configuration

## Data Generation

> **💡 Important**: Make sure your virtual environment is activated before running these commands!
> 
> If you see `(venv)` at the start of your prompt, you're good to go.
> If not, run: `.\venv\Scripts\Activate.ps1`

### Generate Sample Datasets

```powershell
python scripts\generate_sample_data.py
```

**Output**:
- `datalake/raw/sales.csv` - 10,000 sales transactions
- `datalake/raw/products.json` - 100 products
- `datalake/raw/customers.csv` - 1,000 customers

**Verify**:
```powershell
dir datalake\raw
```

## Spark ETL Pipeline

### Run ETL Pipeline

```powershell
python spark\etl_pipeline.py
```

**What it does**:
1. Reads raw CSV and JSON files
2. Cleans and transforms data
3. Creates fact and dimension tables
4. Generates aggregations
5. Writes Parquet files to processed and warehouse zones

**Expected Output**:
```
================================================================================
STARTING DATA LAKE ETL PIPELINE
================================================================================
...
✓ Processed Sales Records: 10000
✓ Products in Catalog: 100
✓ Customers: 1000
================================================================================
ETL PIPELINE COMPLETED SUCCESSFULLY
================================================================================
```

### Validate Pipeline

```powershell
python scripts\validate_pipeline.py --stage all
```

## PostgreSQL Setup (Optional)

### Option 1: Local PostgreSQL Installation

```powershell
# Run setup script
python scripts\setup_postgres.py
```

**Note**: Update `DB_CONFIG` in `setup_postgres.py` with your PostgreSQL credentials.

### Option 2: Docker PostgreSQL

```powershell
# Start PostgreSQL container
docker run --name datalake-postgres `
  -e POSTGRES_PASSWORD=postgres `
  -p 5432:5432 `
  -d postgres:13

# Wait for PostgreSQL to start
timeout /t 10

# Run setup script
python scripts\setup_postgres.py
```

## Apache Airflow Setup

### Step 1: Navigate to Airflow Directory

```powershell
cd airflow
```

### Step 2: Create Required Directories

```powershell
mkdir logs, plugins -Force
```

### Step 3: Start Airflow with Docker Compose

```powershell
# Start Airflow services
docker-compose up -d

# Check status
docker-compose ps
```

**Services started**:
- PostgreSQL (Airflow metadata)
- Airflow Webserver (port 8080)
- Airflow Scheduler
- Airflow Init (one-time setup)

### Step 4: Access Airflow Web UI

1. Open browser: **http://localhost:8080**
2. Login:
   - Username: `admin`
   - Password: `admin`

### Step 5: Enable and Trigger DAG

1. Find DAG: `spark_etl_pipeline`
2. Toggle to enable it
3. Click "Trigger DAG" to run manually
4. Monitor task execution in Graph or Tree view

### Airflow Commands

```powershell
# View logs
docker-compose logs -f airflow-scheduler

# Stop Airflow
docker-compose down

# Restart Airflow
docker-compose restart

# Remove all data (fresh start)
docker-compose down -v
```

## Apache Superset Setup

### Step 1: Navigate to Superset Directory

```powershell
cd ..\superset
```

### Step 2: Start Superset with Docker Compose

```powershell
# Start Superset
docker-compose up -d

# Check status
docker-compose ps
```

### Step 3: Wait for Initialization

```powershell
# Wait for Superset to initialize (2-3 minutes)
timeout /t 120

# Check logs
docker-compose logs -f superset
```

### Step 4: Access Superset Web UI

1. Open browser: **http://localhost:8088**
2. Login:
   - Username: `admin`
   - Password: `admin`

### Step 5: Connect to PostgreSQL

1. Click **Settings** → **Database Connections**
2. Click **+ Database**
3. Select **PostgreSQL**
4. Enter connection details:
   ```
   Host: host.docker.internal
   Port: 5432
   Database: datalake_warehouse
   Username: datalake_user
   Password: datalake_password
   ```
5. Test connection and save

### Step 6: Create Datasets

1. Click **Data** → **Datasets**
2. Add datasets for each table:
   - `fact_sales`
   - `dim_products`
   - `dim_customers`
   - `agg_monthly_revenue`
   - `agg_top_products`
   - `agg_regional_sales`

### Step 7: Create Charts

Example charts to create:

#### 1. Revenue by Month (Line Chart)
- Dataset: `agg_monthly_revenue`
- X-axis: `month`
- Y-axis: `total_revenue`

#### 2. Top 10 Products (Bar Chart)
- Dataset: `agg_top_products`
- X-axis: `product_name`
- Y-axis: `total_revenue`
- Limit: 10

#### 3. Sales by Region (Table)
- Dataset: `agg_regional_sales`
- Columns: `state`, `city`, `total_revenue`
- Sort by: `total_revenue` DESC

#### 4. Average Order Value (Big Number)
- Dataset: `agg_monthly_revenue`
- Metric: `AVG(avg_order_value)`

### Step 8: Create Dashboard

1. Click **Dashboards** → **+ Dashboard**
2. Name it: "Sales Analytics Dashboard"
3. Drag and drop created charts
4. Arrange and resize as needed
5. Save dashboard

## Verification Checklist

### ✅ Data Generation
- [ ] Raw data files created in `datalake/raw/`
- [ ] Files contain expected number of records

### ✅ Spark ETL
- [ ] Processed files created in `datalake/processed/`
- [ ] Warehouse files created in `datalake/warehouse/`
- [ ] No errors in console output

### ✅ Validation
- [ ] All validation checks pass
- [ ] Parquet files readable

### ✅ Airflow
- [ ] Airflow UI accessible at localhost:8080
- [ ] DAG visible in UI
- [ ] DAG runs successfully
- [ ] All tasks show green (success)

### ✅ Superset
- [ ] Superset UI accessible at localhost:8088
- [ ] Database connection successful
- [ ] Datasets created
- [ ] Charts display data
- [ ] Dashboard created

## Troubleshooting

### Python Issues

**Error**: `python: command not found`
- **Solution**: Add Python to PATH or use full path

**Error**: `No module named 'pyspark'`
- **Solution**: Activate virtual environment and reinstall requirements

### Java Issues

**Error**: `JAVA_HOME is not set`
- **Solution**: Set JAVA_HOME environment variable
  ```powershell
  setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-11.0.x-hotspot"
  ```

### Docker Issues

**Error**: `Cannot connect to Docker daemon`
- **Solution**: Start Docker Desktop

**Error**: `Port already in use`
- **Solution**: Stop conflicting services or change ports in docker-compose.yaml

### Airflow Issues

**Error**: `Airflow webserver not accessible`
- **Solution**: Wait 2-3 minutes for initialization, check logs

**Error**: `DAG not showing up`
- **Solution**: Check DAG file syntax, view scheduler logs

### Superset Issues

**Error**: `Cannot connect to database`
- **Solution**: Use `host.docker.internal` instead of `localhost` when connecting from Docker

**Error**: `Admin user not created`
- **Solution**: Run init script manually:
  ```powershell
  docker exec superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin
  ```

### Spark Issues

**Error**: `Java heap space`
- **Solution**: Increase Spark memory in config.py

**Error**: `File not found`
- **Solution**: Check file paths are absolute and correct

## Next Steps

1. **Customize Data**: Modify `generate_sample_data.py` for your use case
2. **Add Transformations**: Extend `etl_pipeline.py` with business logic
3. **Schedule DAG**: Adjust `schedule_interval` in Airflow DAG
4. **Create More Charts**: Build comprehensive dashboards in Superset
5. **Optimize Performance**: Tune Spark and database configurations
6. **Add Monitoring**: Set up email alerts in Airflow
7. **Document Changes**: Update README with customizations

## Useful Commands

```powershell
# Activate virtual environment
.\venv\Scripts\activate

# Run ETL pipeline
python spark\etl_pipeline.py

# Validate data
python scripts\validate_pipeline.py --stage all

# Start all services
cd airflow && docker-compose up -d
cd ..\superset && docker-compose up -d

# Stop all services
cd airflow && docker-compose down
cd ..\superset && docker-compose down

# View logs
docker-compose logs -f [service-name]

# List running containers
docker ps
```

## Support

For issues or questions:
1. Check troubleshooting section
2. Review logs for error messages
3. Verify all prerequisites are installed
4. Ensure Docker Desktop is running

---

**🎉 Congratulations! Your data lake is now operational!**
