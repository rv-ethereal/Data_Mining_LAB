
# <a id="readme-top"></a>

<div align="center">

<img src="https://cdn-icons-png.flaticon.com/512/3135/3135715.png" alt="Logo" width="120" height="120">

# 🚀 On-Premise Data Lake with Apache Spark ETL, Apache Airflow Orchestration & Apache Superset Dashboards

### **Data Mining Laboratory Project**

**Under the guidance of *Prof. Sandeep Kumar Srivastava***

</div>

---

## 📊 GitHub Repository Badges

[![Repo Stars](https://img.shields.io/github/stars/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/stargazers)
[![Repo Forks](https://img.shields.io/github/forks/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/network/members)
[![Repo Issues](https://img.shields.io/github/issues/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/issues)
[![Contributors](https://img.shields.io/github/contributors/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/graphs/contributors)
[![License](https://img.shields.io/github/license/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](LICENSE)

---

<div align="center">

## 🎬 Dashboard Demo (GIF Preview)

<img src="https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExNmFvaTEyM3h1czFoN25ja2U2N2l0d3gzNWMzZjMzYmx3aGg2Y3QwaiZlcD12MV9naWZzX3NlYXJjaCZjdD1n/3oEjI6SIIHBdRxXI40/giphy.gif" width="700"/>

> *Replace this GIF with your own Superset dashboard screen recording later.*

</div>

---

## 📌 About the Project

This is a **complete on-premise data engineering pipeline**, built as part of the **Data Mining Laboratory** under **Prof. Sandeep Kumar Srivastava**.

The project simulates a **real-world enterprise data system**, implemented entirely on a **local machine** (no cloud):

* A structured **local data lake**
* **Apache Spark** for ETL processing
* **Apache Airflow** for scheduling & orchestration
* **Apache Superset** for dashboards

This framework provides a full workflow for **data ingestion → transformation → warehousing → visualization**.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## 🏗 System Architecture Overview

```
                      ┌──────────────────────┐
                      │     Data Sources      │
                      │  CSV / JSON / APIs    │
                      └─────────┬────────────┘
                                │
                                ▼
                  ┌─────────────────────────────┐
                  │     On-Premise Data Lake     │
                  │  raw / staging / processed   │
                  └──────────┬───────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │         Apache Airflow (Scheduler)        │
          │  Triggers Spark ETL on schedule           │
          └──────────┬───────────────────────────────┘
                     │
                     ▼
          ┌──────────────────────────────────────────┐
          │           Apache Spark ETL Pipeline       │
          │   Cleaning | Transforming | Aggregations  │
          └──────────┬───────────────────────────────┘
                     │
                     ▼
   ┌───────────────────────────────────────────────────────────┐
   │    Local Data Warehouse (Parquet / SQLite / PostgreSQL)   │
   └──────────┬────────────────────────────────────────────────┘
              │
              ▼
     ┌───────────────────────────────────┐
     │         Apache Superset           │
     │     Dashboards & Visual Analytics │
     └───────────────────────────────────┘
```

---

## 📂 Data Lake Structure

```
datalake/
    ├── raw/
    │     ├── sales.csv
    │     ├── products.json
    │     ├── customers.csv
    ├── staging/
    ├── processed/
    └── warehouse/
```

---

## 🧰 Built With

### **Storage**

* Local File System (Data Lake)

### **Processing**

* Apache Spark
* PySpark

### **Workflow Orchestration**

* Apache Airflow (DAG-based automation)

### **Analytics**

* Apache Superset

### **Warehouse Engine**

* Parquet
* SQLite / PostgreSQL (Optional)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## 🔧 Detailed Workflow

### **1️⃣ Data Ingestion — Raw Zone**

Sources include:

* CSV files
* JSON dumps
* API exports (optional)

All files are placed inside:

```
datalake/raw/
```

---

### **2️⃣ Apache Airflow DAG — Pipeline Automation**

Airflow manages:

✔ Ingest raw files
✔ Trigger Spark ETL job
✔ Validate processed outputs
✔ Load curated datasets into warehouse
✔ Trigger Superset refresh (optional)

Example DAG:

```python
with DAG('spark_etl_pipeline', schedule_interval='@daily') as dag:

    ingest = BashOperator(...)
    spark_etl = SparkSubmitOperator(...)
    validate = PythonOperator(...)
    load_warehouse = BashOperator(...)
```

---

### **3️⃣ Apache Spark ETL — Transform Phase**

Spark performs:

* Null and anomaly removal
* Data type conversions
* Feature engineering
* Aggregations
* Joins between datasets
* Writing cleaned data to `/processed`
* Writing final analytics-ready data to `/warehouse`

Example:

```python
df = spark.read.csv("datalake/raw/sales.csv", header=True)
cleaned = df.dropna().withColumn("total", df.qty * df.price)
cleaned.write.mode("overwrite").parquet("datalake/processed/sales")
```

---

### **4️⃣ Data Warehouse — Load Phase**

Data stored as:

* Parquet files
* OR SQL tables (SQLite/PostgreSQL)

---

### **5️⃣ Apache Superset — Dashboard Layer**

Superset builds visualizations such as:

📊 Revenue by Month
📈 Sales Trend Analysis
🗺 Region-wise Sales Map
📦 Top Products by Revenue
👤 Customers by Location
📉 Return Percentage

All panels combined into a clean BI dashboard.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## 🎯 Final Deliverables

✔ Complete On-Prem Data Lake
✔ Spark ETL PySpark Scripts
✔ Airflow Automated DAG
✔ Structured Warehouse (Parquet/SQL)
✔ Superset Dashboard (6–10 charts)
✔ Architecture Diagram
✔ Detailed Project Documentation + README

---

## 📞 Contact

**Student:** (Add your name here)
**Instructor:** *Prof. Sandeep Kumar Srivastava*
**Repository:** [https://github.com/rv-ethereal/Data_Mining_LAB](https://github.com/rv-ethereal/Data_Mining_LAB)

<p align="right">(<a href="#readme-top">back to top</a>)</p>


