
---

# On-Premises Data Lake using Apache Spark, Airflow & Superset

A complete **on-premises data lake and analytics pipeline** built using **Apache Spark for ETL processing**, **Apache Airflow for workflow orchestration**, and **Apache Superset for business intelligence and visualization**.
This project demonstrates the full **Raw → Processed → Warehouse → Analytics** lifecycle for structured enterprise data.

---

## 🎯 Project Objective

To design and implement a **local data lake architecture** that:

* Ingests raw transactional data
* Performs scalable ETL using Apache Spark
* Automates workflows using Apache Airflow
* Builds analytical warehouse tables
* Visualizes business insights using Apache Superset

This project is developed as part of the **Data Mining and Warehousing Laboratory**.

---

## 🏗️ System Architecture

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
│   └── spark_etl_dag.py        # Daily Airflow ETL pipeline
├── spark/
│   └── spark_etl.py            # Spark ETL with Pandas fallback
├── tools/
│   └── parquet_to_sqlite.py    # Warehouse Parquet → SQLite converter
├── datalake/
│   ├── raw/                    # Input CSV files
│   ├── processed/              # Cleaned Parquet data
│   └── warehouse/              # Aggregated analytics tables
├── app.py                      # Superset application entry
├── superset_config.py          # Superset configuration
├── init_superset.ps1           # Superset initialization script
├── run_superset.ps1            # Script to start Superset
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

| Table Name           | Description                    |
| -------------------- | ------------------------------ |
| `revenue_by_product` | Product-wise sales and revenue |
| `revenue_by_region`  | Regional business performance  |
| `payment_analysis`   | Payment method statistics      |
| `status_summary`     | Order status distribution      |
| `customer_summary`   | Customer purchase behavior     |
| `monthly_sales`      | Monthly business trend         |

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
* Set up roles and permissions

---

### 4️⃣ Start Services

**Option 1: Airflow Standalone Mode**

```powershell
airflow standalone
```

**Option 2: Manual Services**

```powershell
airflow webserver --port 8080
airflow scheduler
```

**Start Superset**

```powershell
.\run_superset.ps1
```

* Airflow UI → [http://localhost:8080](http://localhost:8080)
* Superset UI → [http://localhost:8088](http://localhost:8088)
* Superset Login → `admin / admin`

---

## ▶️ Trigger ETL Pipeline

```powershell
airflow dags trigger data_lake_etl_pipeline
```

You may also trigger the DAG directly from the **Airflow Web UI**.

---

## 🧪 Run Spark ETL Manually

```powershell
spark-submit spark/spark_etl.py
```

With custom memory:

```powershell
spark-submit --driver-memory 4g --executor-memory 4g spark/spark_etl.py
```

---

## 🗄️ Convert Warehouse Parquet to SQLite (For Superset)

```powershell
python tools/parquet_to_sqlite.py
```

This generates:

```
datalake/warehouse.db
```

which is used as the **Superset data source**.

---

## ⭐ Key Features

* Fully automated **daily ETL pipeline**
* **Spark + Pandas fallback system**
* Multi-layer **On-Premise Data Lake architecture**
* Workflow monitoring with **Apache Airflow**
* Interactive dashboards with **Apache Superset**
* Lightweight **SQLite warehouse integration**



---

## 📚 References

* Apache Spark Documentation
* Apache Airflow Documentation
* Apache Superset Documentation

---

## 👨‍🎓 Author

* Gaurav Kumar (MSA24002)
* Course: Data Mining and Warehousing Laboratory
* Project Type:** On-Premises Data Lake Implementation

---
