# 🚀 On-Premise Data Lake with Apache Spark ETL, Apache Airflow Orchestration & Apache Superset Dashboards

<p align="center">
  <img src="https://cdn-icons-png.flaticon.com/512/3135/3135715.png" alt="Logo" width="120" height="120">
</p>

<div align="center">

**Data Mining Laboratory Project**

**Under the guidance of *Prof. Sandeep Kumar Srivastava***

</div>

---

[![Repo Stars](https://img.shields.io/github/stars/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/stargazers)
[![Repo Forks](https://img.shields.io/github/forks/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/network/members)
[![Repo Issues](https://img.shields.io/github/issues/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/issues)
[![Contributors](https://img.shields.io/github/contributors/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/graphs/contributors)
[![License](https://img.shields.io/github/license/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](LICENSE)

---

<p align="center">
  <img src="https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExbXN0MjFtc2tjNWliOGpwbjlsc250NnJ2dHdlcjNiMXRmcGluOHl2byZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/WTO8QA0mX2Cfw5vhkp/giphy.gif" width="700" alt="data-engineering-gif"/>
</p>

---

## ✨ Project Summary

This repository contains a **complete on-premise data engineering pipeline** built to simulate a production-like enterprise workflow on a local machine (no cloud required). The stack demonstrates ingestion, ETL, orchestration, warehousing, and BI visualization using open-source tooling.

Key components:

* Local Data Lake (raw / staging / processed)
* Apache Spark (PySpark) ETL
* Apache Airflow DAG-based orchestration
* Local warehouse (Parquet / SQLite / optional PostgreSQL)
* Apache Superset dashboards for analytics

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## 🏗 System Architecture (visual)

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

**Storage**

* Local File System (Data Lake)

**Processing**

* Apache Spark (PySpark)

**Workflow Orchestration**

* Apache Airflow (DAGs)

**Analytics**

* Apache Superset

**Warehouse**

* Parquet
* SQLite / (PostgreSQL optional)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## 🔧 Workflow (detailed)

### 1️⃣ Data Ingestion — Raw Zone

Sources include CSV, JSON and optional API exports. Dropped into `datalake/raw/` for automated pick-up.

### 2️⃣ Apache Airflow DAG — Pipeline Automation

Airflow does the orchestration (ingest → ETL → validate → load → refresh). Example DAG snippet:

```python
with DAG('spark_etl_pipeline', schedule_interval='@daily') as dag:

    ingest = BashOperator(...)
    spark_etl = SparkSubmitOperator(...)
    validate = PythonOperator(...)
    load_warehouse = BashOperator(...)
```

### 3️⃣ Apache Spark ETL — Transform Phase

Spark performs null/anomaly removal, type conversions, feature engineering, joins, aggregations and writes parquet output to `/processed` and analytics-ready tables to `/warehouse`.

Example:

```python
df = spark.read.csv("datalake/raw/sales.csv", header=True, inferSchema=True)
cleaned = df.dropna().withColumn("total", df.qty * df.price)
cleaned.write.mode("overwrite").parquet("datalake/processed/sales")
```

### 4️⃣ Data Warehouse — Load Phase

Processed datasets are stored as parquet files or optionally as SQL tables (SQLite/Postgres) for Superset connectivity.

### 5️⃣ Apache Superset — Dashboard Layer

Typical dashboards:

* Revenue by month
* Sales trend analysis
* Region-wise sales map
* Top products by revenue
* Customer distribution by location
* Return/Refund analysis

---

## 🎯 Final Deliverables

* ✅ Complete On-Prem Data Lake
* ✅ Spark ETL PySpark Scripts
* ✅ Airflow DAG (scheduler + operators)
* ✅ Structured Warehouse (Parquet / SQL)
* ✅ Superset Dashboards (6–10 charts)
* ✅ Architecture Diagram + Documentation

---

## 🚀 Quick Start — Add this README to your repository

> **Important:** The steps below will initialize a git repo locally and push **only** to your personal branch. **Do not push to `main`**.

```bash
# Initialize a new Git repository (if not initialized)
git init

# Add all project files
git add .

# Add remote GitHub repository (one-time)
git remote add origin https://github.com/rv-ethereal/Data_Mining_LAB.git

# Commit your changes
git commit -m "msa24021 repo push"

# Create and switch to your own branch (replace with your enrollment number)
git checkout -b msa24021

# Push ONLY to your own branch — NOT to main
git push origin msa24021
```

> If `origin` already exists and you need to update it, run:

```bash
git remote set-url origin https://github.com/rv-ethereal/Data_Mining_LAB.git
```

---



## 👥 Contributors

<p align="center">
  <img src="https://contrib.rocks/image?repo=rv-ethereal/Data_Mining_LAB" alt="Contributors"/>
</p>

[![Contributors](https://img.shields.io/github/contributors/rv-ethereal/Data_Mining_LAB?style=for-the-badge)](https://github.com/rv-ethereal/Data_Mining_LAB/graphs/contributors)



---

## 📞 Contact

**Student:** *Add your name here*

**Enrollment no:** *Add your name here*

**Instructor:** Prof. Sandeep Kumar Srivastava

**Repository:** [https://github.com/rv-ethereal/Data_Mining_LAB](https://github.com/rv-ethereal/Data_Mining_LAB)

---

<p align="right">(<a href="#readme-top">back to top</a>)</p>
