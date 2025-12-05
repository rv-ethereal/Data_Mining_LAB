# **Retail Data lake  Pipeline**

*A complete end-to-end Data Engineering project using Apache Airflow, Apache Spark, MinIO Data Lake, Docker, and Apache Superset.*

---

## 🚀 **Project Overview**

This project demonstrates a real-world **Retail Analytics Data Engineering Pipeline**.
It covers everything from **raw data ingestion → ETL processing → data warehousing → orchestrated workflows → analytics dashboard**.

The architecture is built using:

* **Apache Airflow** for workflow orchestration
* **Apache Spark** for ETL processing
* **MinIO (S3 compatible)** as the Data Lake
* **Docker Compose** to run everything seamlessly
* **Apache Superset** for BI dashboards and reporting

This project showcases strong practical skills in ETL, data pipelines, scheduling, monitoring, and analytics.

##  **Architecture Diagram**

The pipeline follows a structured multi-zone Data Lake approach:

```
Raw → ETL → Warehouse → Analytics → Dashboard
```

Each layer has its own folder inside the `datalake/` directory.

* **raw/** — raw CSV and JSON files
* **warehouse/** — processed & cleaned Parquet files
* **analytics/** — ready-to-use analytics datasets

---

## 📂 **Project Structure**

```
MSD24011-Data_Mining_LAB/
│
├── airflow/
│   ├── dags/
│   │   └── retail_etl_dag.py
│
├── spark/
│   └── spark_etl.py
│
├── datalake/
│   ├── raw/
│   │   ├── customers.csv
│   │   ├── products.json
│   │   └── sales.csv
│   │
│   ├── warehouse/sales_fact/
│   │   └── *.parquet
│   │
│   └── analytics/sales_fact/
│       └── *.parquet
│
├── superset/
│   └── superset_config.py
│
├── Result_images/
│   ├── Airflow_Dag.jpg
│   ├── Airflow_UI.jpg
│   ├── dashboard.jpg
│   ├── Spark_master_UI.jpg
│   ├── Spark_worker_UI.jpg
│   └── More Screenshots...
│
├── docker-compose.yml
├── dashboard.py
├── .env
└── README.md  (this one)
```

---

## 🔄 **Pipeline Workflow**

### **1️⃣ Raw Data Ingestion**

Raw files stored in:

```
/datalake/raw/customers.csv
/datalake/raw/products.json
/datalake/raw/sales.csv
```

These represent customer info, product master data, and sales transactions.

---

### **2️⃣ ETL using Apache Spark**

The script:
`/spark/spark_etl.py`

Performs:

* Loading CSV & JSON files from Raw zone
* Data type cleaning, date parsing
* Joining customer, product & sales data
* Creating a **Sales Fact table**
* Saving output as Parquet into:

```
/datalake/warehouse/sales_fact
```

---

### **3️⃣ Workflow Orchestration using Apache Airflow**

DAG file:
`airflow/dags/retail_etl_dag.py`

The DAG includes:

* Spark ETL trigger
* Validations
* Timestamped logs
* Automated daily scheduling

Images included:

* ✔ Airflow DAG Screenshot
* ✔ Airflow UI
* ✔ Task Graph view

---

### **4️⃣ Analytics Layer**

Spark creates analytics-friendly datasets:

```
/datalake/analytics/sales_fact/
```

Used later for BI dashboards.

---

### **5️⃣ BI Dashboard using Apache Superset**

Config file:
`/superset/superset_config.py`

Dashboard built on:

* Total Sales
* Top Customers
* Product Performance
* Monthly Trends
* Interactive Filters


---

## 🐳 **Docker Compose Setup**

The `docker-compose.yml` file creates:

* Airflow Scheduler & WebServer
* Spark Master & Workers
* MinIO S3 Bucket
* Superset Dashboard

To run the complete setup:

```bash
docker-compose up -d
```

---

## 📊 **Screenshots**

Screenshots included in `Result_images/` folder:

* Airflow DAG
* Airflow UI
* Spark Master UI
* Spark Worker UI
* Superset Dashboard
* Results Preview

