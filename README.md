
<a name="top"></a>

<div align="center">

<h1>On-Premise Data Lake Platform</h1>

<h3>Apache Spark ETL · Apache Airflow Orchestration · Apache Superset Analytics</h3>

<p><b>Data Mining Laboratory Project</b><br/>
Under the guidance of <b>Prof. Sandeep Kumar Srivastava</b></p>

<p>
  <b>Student:</b> Kuldeep Kumar Mishra &nbsp;|&nbsp;
  <b>Enrollment No:</b> MSD24006
</p>

<br/>

<p>
  <img src="https://img.shields.io/badge/Python-3.x-blue?style=for-the-badge&logo=python" />
  <img src="https://img.shields.io/badge/Apache%20Spark-ETL-orange?style=for-the-badge&logo=apachespark" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow" />
  <img src="https://img.shields.io/badge/Apache%20Superset-Analytics-181717?style=for-the-badge&logo=apache" />
</p>

<p>
  <img src="https://img.shields.io/github/stars/rv-ethereal/Data_Mining_LAB?style=flat-square" />
  <img src="https://img.shields.io/github/forks/rv-ethereal/Data_Mining_LAB?style=flat-square" />
  <img src="https://img.shields.io/github/issues/rv-ethereal/Data_Mining_LAB?style=flat-square" />
  <img src="https://img.shields.io/github/license/rv-ethereal/Data_Mining_LAB?style=flat-square" />
</p>

<br/>

<p>
  <img src="https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExbXN0MjFtc2tjNWliOGpwbjlsc250NnJ2dHdlcjNiMXRmcGluOHl2byZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/WTO8QA0mX2Cfw5vhkp/giphy.gif" 
       alt="Data Engineering" width="650" />
</p>

</div>

---

## 1. Overview

This repository implements an **on-premise data lake platform** that simulates a production-style enterprise data workflow on a single machine (no cloud dependency).

The architecture covers the complete lifecycle:

- Ingestion of structured data into a local **data lake**
- Distributed ETL using **Apache Spark (PySpark)**
- Workflow scheduling and dependency management via **Apache Airflow**
- Storage in a local **analytics warehouse** (Parquet / SQLite / PostgreSQL)
- Self-service BI dashboards using **Apache Superset**

The project is developed as part of the **Data Mining Laboratory** course and reflects typical responsibilities of a data engineer in an analytics ecosystem.

---

## 2. Architecture

```text
                          Data Sources
                    (CSV / JSON / API Dumps)
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │   On-Premise Data Lake   │
                    │  raw / staging / processed
                    └──────────────┬───────────┘
                                   │
                                   ▼
                 ┌────────────────────────────────┐
                 │     Apache Airflow Scheduler   │
                 │   DAG-driven ETL Orchestration │
                 └──────────────┬─────────────────┘
                                │
                                ▼
                 ┌────────────────────────────────┐
                 │       Apache Spark ETL         │
                 │  cleaning | joins | aggregations
                 └──────────────┬─────────────────┘
                                │
                                ▼
        ┌─────────────────────────────────────────────────┐
        │     Data Warehouse (Parquet / SQLite / PGSQL)   │
        └──────────────┬──────────────────────────────────┘
                       │
                       ▼
             ┌──────────────────────────────┐
             │        Apache Superset       │
             │   Dashboards & Analytics     │
             └──────────────────────────────┘
````

---

## 3. Data Lake Layout

```text
datalake/
    ├── raw/
    │     ├── sales.csv
    │     ├── products.json
    │     └── customers.csv
    ├── staging/
    ├── processed/
    └── warehouse/
```

* **raw/**: Direct dumps from source systems
* **staging/**: Intermediate cleaned/validated data
* **processed/**: Curated, analytics-ready datasets
* **warehouse/**: Final fact/dimension-style tables

---

## 4. Technology Stack

<div align="center">

| Layer             | Technology                   |
| ----------------- | ---------------------------- |
| Data Lake Storage | Local File System            |
| Distributed ETL   | Apache Spark (PySpark)       |
| Orchestration     | Apache Airflow (DAGs)        |
| Warehouse         | Parquet, SQLite / PostgreSQL |
| Analytics / BI    | Apache Superset              |
| Language          | Python                       |

</div>

---

## 5. Workflow Summary

### 5.1 Ingestion (Raw Zone)

* Data is exported from operational systems as **CSV / JSON / API dumps**
* Files are landed into `datalake/raw/` for downstream processing

### 5.2 Orchestration (Apache Airflow)

* Airflow DAG defines the pipeline: **ingest → transform → validate → load**
* Typical pattern:

```python
with DAG(
    dag_id="spark_etl_pipeline",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    ingest = BashOperator(...)
    spark_etl = SparkSubmitOperator(...)
    validate = PythonOperator(...)
    load_warehouse = BashOperator(...)
```

### 5.3 Transformation (Apache Spark)

* Spark handles:

  * Null handling, type casting, basic data quality filters
  * Business logic calculations (e.g., revenue, metrics)
  * Aggregations and joins across datasets

```python
df = spark.read.csv("datalake/raw/sales.csv", header=True, inferSchema=True)
cleaned = (
    df.dropna()
      .withColumn("total_amount", df.qty * df.price)
)
cleaned.write.mode("overwrite").parquet("datalake/processed/sales")
```

### 5.4 Warehouse & Analytics

* Curated outputs are exported as:

  * **Parquet files** in `datalake/warehouse/`, and/or
  * Tables in **SQLite / PostgreSQL** for Superset
* Superset connects to the warehouse to build dashboards such as:

  * Monthly revenue trend
  * Top products by revenue
  * Region-wise performance
  * Customer segmentation views

---

## 6. Repository Usage

> The commands below assume you are pushing to your **own branch**, not `main`.

```bash
# Initialize git (if not already)
git init

# Stage project files
git add .

# Configure remote
git remote add origin https://github.com/rv-ethereal/Data_Mining_LAB.git

# Commit with your identifier
git commit -m "msa24006 project submission"

# Create and push to your personal branch
git checkout -b msa24006
git push origin msa24006
```

If the remote already exists:

```bash
git remote set-url origin https://github.com/rv-ethereal/Data_Mining_LAB.git
```

---

## 📞 Contact

**Student:** *Soumita Chatterjee*

**Enrollment no:** *MSA24006*

**Instructor:** Prof. Sandeep Kumar Srivastava

**Repository:** [https://github.com/rv-ethereal/Data_Mining_LAB](https://github.com/rv-ethereal/Data_Mining_LAB)

---

<p align="right"><a href="#top">Back to top</a></p>

