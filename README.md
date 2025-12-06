
---

# On-Premises Data Lake using Apache Spark, Airflow & Superset

A complete **on-premises data lake and analytics pipeline** built using **Apache Spark for ETL processing**, **Apache Airflow for workflow orchestration**, and **Apache Superset for business intelligence and visualization**.
This project demonstrates the full **Raw → Processed → Warehouse → Analytics** lifecycle for structured enterprise data.

---

##  Project Objective

To design and implement a **local data lake architecture** that:

* Ingests raw transactional data
* Performs scalable ETL using Apache Spark
* Automates workflows using Apache Airflow
* Builds analytical warehouse tables
* Visualizes business insights using Apache Superset

This project is developed as part of the **Data Mining and Warehousing Laboratory**.

---

##  System Architecture

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

---

## 🛠️ Technology Stack

* **Apache Spark 3.5+** – Distributed ETL processing
* **Apache Airflow 2.6.3** – Workflow orchestration
* **Apache Superset** – Business Intelligence dashboard
* **Python 3.11+** – Core programming language
* **Pandas & PyArrow** – Data handling
* **SQLite** – Analytical warehouse database

---

## 📁 Project Structure

```
onprem-datalake/
├── airflow/dags/
│   └── spark_etl_dag.py
├── spark/
│   └── spark_etl.py
├── tools/
│   └── parquet_to_sqlite.py
├── datalake/
│   ├── raw/
│   ├── processed/
│   └── warehouse/
├── app.py
├── superset_config.py
├── init_superset.ps1
├── run_superset.ps1
├── requirements.txt
└── README.md
```

---

## 🔄 ETL Pipeline Description

**Airflow DAG:** `data_lake_etl_pipeline`
**Schedule:** Daily

### Pipeline Tasks:

1. **Check Input Files** – Verifies presence of raw CSV files
2. **Run Spark ETL** – Data cleaning, transformation, and aggregation
3. **Validate Warehouse Output** – Confirms all analytics tables are created
4. **Success Notification**

The Spark ETL also includes a **Pandas fallback mode** to ensure execution even if Spark fails.

---

## 📊 Warehouse Analytics Tables

| Table Name         | Description                    |
| ------------------ | ------------------------------ |
| revenue_by_product | Product-wise sales and revenue |
| revenue_by_region  | Regional business performance  |
| payment_analysis   | Payment method analysis        |
| status_summary     | Order status distribution      |
| customer_summary   | Customer purchase summary      |
| monthly_sales      | Monthly sales trend            |

---

## ⚙️ Setup Instructions (Windows)

### 1️⃣ Install Dependencies

```powershell
pip install -r requirements.txt
```

---

### 2️⃣ Prepare Input Data

Place the following files in:

```
datalake/raw/
- sales.csv
- customers.csv
```

---

### 3️⃣ Initialize Superset

```powershell
.\init_superset.ps1
```

This will:

* Initialize Superset database
* Create default admin user
* Configure roles and permissions

---

### 4️⃣ Start Services

#### Option 1: Airflow Standalone

```powershell
airflow standalone
```

#### Option 2: Manual Mode

```powershell
airflow webserver --port 8080
airflow scheduler
```

#### Start Superset

```powershell
.\run_superset.ps1
```

**Access URLs:**

* Airflow: [http://localhost:8080](http://localhost:8080)
* Superset: [http://localhost:8088](http://localhost:8088)
* Login: admin / admin

---

## ▶️ Trigger ETL Pipeline

```powershell
airflow dags trigger data_lake_etl_pipeline
```

Or from Airflow Web UI.

---

##  Run Spark ETL Manually

```powershell
spark-submit spark/spark_etl.py
```

With memory:

```powershell
spark-submit --driver-memory 4g --executor-memory 4g spark/spark_etl.py
```

---

##  Convert Warehouse Parquet to SQLite

```powershell
python tools/parquet_to_sqlite.py
```

Output:

```
datalake/warehouse.db
```

Used as the Superset data source.

---

## 🖥️  Complete Final Execution Flow (For Viva & Demo)

### Step 1: Open Project in VS Code

```powershell
cd onprem-datalake
```

### Step 2: Install Dependencies

```powershell
pip install -r requirements.txt
```

### Step 3: Start Airflow

```powershell
airflow standalone
```

### Step 4: Trigger ETL Pipeline

```powershell
airflow dags trigger data_lake_etl_pipeline
```

### Step 5: Verify Output Data

Check:

```
datalake/processed/
datalake/warehouse/
```

### Step 6: Convert to SQLite for Superset

```powershell
python tools/parquet_to_sqlite.py
```

### Step 7: Start Superset

```powershell
.\run_superset.ps1
```

### Step 8: Open Dashboard

```
http://localhost:8088
Login: admin / admin
Dashboards → Sales Analytics Dashboard
```

---

## 📈 Superset Dashboard Visualizations

1. **Revenue by Product** – Area Chart
2. **Revenue by Region** – Pie Chart
3. **Monthly Sales Trend** – Line Chart

These charts provide real-time business intelligence from warehouse data.

---

## ⭐ Key Features

* Fully automated daily ETL pipeline
* Spark + Pandas fallback system
* Multi-layer on-premise data lake
* Workflow orchestration using Apache Airflow
* Interactive analytics via Apache Superset
* Lightweight SQLite warehouse

---

## ✅ Results & Observations

* End-to-end data pipeline executed successfully
* Airflow DAG runs without failure
* Spark processes raw data accurately
* Analytics tables generated correctly
* Superset dashboards display correct business insights




---

## 📚 References

* Apache Spark Documentation
* Apache Airflow Documentation
* Apache Superset Documentation

---

## 👨‍🎓 Author

**Gaurav Kumar (MSA24002)**
Course: Data Mining and Warehousing Laboratory
Project Type: **On-Premises Data Lake Implementation**

---
