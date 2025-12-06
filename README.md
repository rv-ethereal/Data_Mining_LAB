# 🏢 Enterprise On-Premise Data Lake Platform

<p align="center">
  <img src="https://cdn-icons-png.flaticon.com/512/3135/3135715.png" alt="Data Lake Logo" width="140" height="140">
</p>

<div align="center">

## Complete Data Engineering Stack with Apache Spark, Airflow & Superset

**Data Mining Laboratory - Enterprise-Grade Solution**

[Project Overview](#-project-overview) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Screenshots](#-screenshots) • [Documentation](#-documentation)

</div>

---

<div align="center">

[![Platform Badge](https://img.shields.io/badge/Platform-On--Premise%20Data%20Lake-blueviolet?style=for-the-badge&logo=apache)](.)
[![Status Badge](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=for-the-badge&logo=github)](.)
[![Python Badge](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python)](.)
[![Spark Badge](https://img.shields.io/badge/Spark-3.5+-orange?style=for-the-badge&logo=apache-spark)](.)
[![Docker Badge](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker)](.)

</div>

---

## 📋 Project Overview

This project implements a **complete on-premise data lake platform** that demonstrates real-world enterprise data engineering workflows on a local machine. It showcases the full lifecycle of modern data platforms: **ingestion → transformation → orchestration → warehousing → analytics**.

<blockquote>
<p align="center">
<strong>Build enterprise-grade data infrastructure without cloud dependencies</strong><br/>
<em>Raw Data → Spark ETL → Parquet → SQLite Warehouse → Superset Dashboards</em>
</p>
</blockquote>

### 🎯 Key Capabilities

 **3-Layer Data Lake** - Raw → Processed → Warehouse architecture  
 **PySpark ETL Pipeline** - Scalable distributed data transformations  
 **Parquet Storage** - Columnar format optimized for analytics  
 **SQLite Warehouse** - Query-optimized structured database  
 **Docker Integration** - Containerized Apache Superset deployment  
 **Interactive Dashboards** - Real-time business intelligence  
 **Airflow Ready** - Scheduled workflow orchestration  

---

## 🏗️ Architecture & Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│                   DATA LAKE PIPELINE                          │
└──────────────────────────────────────────────────────────────┘

 RAW DATA LAYER (datalake/raw/)
    │  CSV/JSON files stored as-is
    │  Single source of truth
    │
    ▼
 SPARK ETL PROCESSING (spark/spark_etl.py)
    │  ├─► Read raw data using PySpark
    │  ├─► Data Cleaning (nulls, duplicates)
    │  ├─► Transformations (calculations, derivations)
    │  ├─► Validation (type checking, quality assurance)
    │  └─► Output to Parquet format
    ▼
 PROCESSED LAYER (datalake/processed/)
    │  Parquet files (columnar, compressed)
    │  Analytics-optimized format
    │
    ▼
 WAREHOUSE BUILDER (tools/parquet_to_sqlite.py)
    │  Converts Parquet → SQLite tables
    │
    ▼
 DATA WAREHOUSE (datalake/warehouse/warehouse.db)
    │  SQLite database with star-schema
    │  Query-optimized structured tables
    │
    ▼
 DOCKER CONTAINER
    │  ├─► Apache Superset (Port 8088)
    │  ├─► Volume mount: datalake/ → /app/datalake/
    │  └─► Connects to warehouse.db
    ▼
 BUSINESS INTELLIGENCE LAYER
    │  Interactive charts & dashboards
    │  Real-time data exploration
    └─► SQL queries & visualizations
```

---

## 📂 Project Structure

```
ONPREM-DATALAKE/
│
├── .vscode/                        # VS Code settings
│
├── airflow/                        # Workflow Orchestration
│   └── dags/
│       └── spark_etl_dag.py       # Airflow DAG for scheduled ETL
│
├── datalake/                       # 3-Layer Data Lake
│   ├── raw/                       # Layer 1: Raw incoming data
│   ├── processed/                 # Layer 2: Cleaned Parquet files
│   └── warehouse/                 # Layer 3: SQLite warehouse
│       └── warehouse.db          # Analytics-ready database
│
├── spark/                          # ETL Processing Engine
│   └── spark_etl.py               # Main PySpark transformation script
│
├── tools/                          # Utility Scripts
│   └── parquet_to_sqlite.py       # Parquet → SQLite converter
│
├── venv/                           # Python virtual environment
│
├── .gitignore                      # Git ignore rules
├── app.py                          # Application entry point
├── create_dashboard.py             # Automated dashboard creation
├── docker-compose.yml              # Docker orchestration config
├── docker-entrypoint.sh            # Container initialization script
├── Dockerfile                      # Custom Superset image definition
├── install_log.txt                 # Installation logs
├── requirements.txt                # Python dependencies
├── run.txt                         # Runtime commands reference
├── run_superset.ps1                # Windows PowerShell launcher
├── setup_superset_demo.py          # Demo data setup script
├── superset_config.py              # Superset configuration file
│
└── README.md                       # This file
```

---

## 📁 Component Explanation

### 🗂️ Data Lake Layers

**datalake/raw/**
- Stores original CSV/JSON files without modification
- Serves as single source of truth
- Preserves data lineage and audit trail

**datalake/processed/**
- Contains cleaned and transformed Parquet files
- Columnar storage format for efficient analytics
- Compressed and optimized for query performance

**datalake/warehouse/**
- SQLite database (`warehouse.db`) with star-schema design
- Fact tables: transaction-level data
- Dimension tables: reference/lookup data
- Ready for BI tool connections

### ⚙️ Processing Components

**spark/spark_etl.py**
- Main ETL pipeline implementation
- Reads from raw layer using PySpark
- Applies cleaning, transformations, validations
- Outputs to both processed (Parquet) and warehouse (SQLite)

**tools/parquet_to_sqlite.py**
- Utility for Parquet → SQLite conversion
- Refreshes warehouse tables from processed layer
- Handles schema creation and data loading

### 🐳 Docker Setup

**Dockerfile**
- Defines custom Apache Superset image
- Base: `python:3.9-slim`
- Includes all necessary dependencies
- Copies configuration files

**docker-compose.yml**
- Orchestrates Superset container
- Maps port 8088 (host) → 8088 (container)
- Volume mount: host's datalake/ → container's /app/datalake/
- Enables live data access from ETL to Superset

**docker-entrypoint.sh**
- Container initialization script
- Runs on startup:
  - `superset db upgrade` - Database migrations
  - `superset fab create-admin` - Admin user creation
  - `superset init` - Default roles/permissions
  - `superset run -p 8088` - Web server start

**superset_config.py**
```python
# Allow SQLite file-based connections
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Enable advanced features
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}
```

### 🌪️ Airflow Integration

**airflow/dags/spark_etl_dag.py**
- Defines scheduled workflow
- Uses SparkSubmitOperator or PythonOperator
- Triggers `spark_etl.py` on schedule
- Includes retry logic and dependencies

---

## 🚀 Quick Start Guide

### Prerequisites

- **Python 3.10+**
- **Java 11+** (required for PySpark)
- **Docker & Docker Compose** (for Superset)
- **8GB+ RAM** recommended
- **Windows/Linux/MacOS**

---


## 📸 Screenshots

### 🔐 1. Superset Login Interface
![Superset Login](outputs/Screenshot%202025-12-06%20115941.png)


**Features:**
- Clean authentication interface
- Secure login credentials
- Default credentials: admin/admin

---

### 📊 2. Charts Gallery
![Charts List](outputs/Screenshot%202025-12-06%20220600.png)

**Available Visualizations:**
-  Revenue by Region (Pie Chart)
-  Monthly Sales Trend (Line Chart)
-  Revenue by Product (Area Chart)
-  Customer Distribution Analytics
-  Payment Method Breakdown

---

### 📈 3. Chart Builder Interface
![Chart Builder](outputs/Screenshot%202025-12-06%20124527.png)

**Components:**
- **Left Panel:** Metrics & Columns selector
- **Center Panel:** Chart configuration
  - X-axis, Y-axis settings
  - Metrics aggregations
  - Filters and grouping
- **Right Panel:** Live chart preview
- **Bottom Panel:** SQL query results

---

### 🎭 4. Visualization Types Library
![Visualization Types](outputs/Screenshot%202025-12-06%20220712.png)
![Visualization Types](outputs/Screenshot%202025-12-06%20220800.png)

**40+ Chart Types:**

**Evolution:**
- Generic Chart, Line Chart, Area Chart
- Smooth Line, Stepped Line

**Distribution:**
- Pie Chart, Bar Chart, Histogram
- Box Plot, Violin Plot

**Flow:**
- Sunburst Chart, Sankey Diagram
- Tree Chart, Treemap



---


**Use Cases:**
- Sales funnel analysis
- Customer journey mapping
- Product category breakdown
- Regional performance analysis

---


## 💡 Key Metrics & Analytics

### Generated Business Metrics

| Metric | Formula | Business Purpose |
|--------|---------|------------------|
| **Total Revenue** | `SUM(final_amount)` | Financial KPI tracking |
| **Average Order Value** | `AVG(final_amount)` | Customer value analysis |
| **Unit Sales Volume** | `SUM(quantity)` | Inventory management |
| **Customer Count** | `COUNT(DISTINCT customer_id)` | Market size estimation |
| **Monthly Growth Rate** | `(current - previous) / previous * 100` | Trend analysis |
| **Regional Performance** | `SUM(revenue) GROUP BY region` | Geographic strategy |
| **Product Performance** | `Revenue × Volume × Margin` | Product prioritization |

---

### Dashboard Examples

**📈 Revenue Analytics Dashboard**
- Monthly revenue trends (line chart)
- Revenue breakdown by product (area chart)
- Geographic revenue heatmap
- Year-over-year comparison

**👥 Customer Analytics Dashboard**
- Customer distribution by region (pie chart)
- Customer lifetime value (histogram)
- Repeat purchase rate (KPI card)
- Customer segmentation analysis

**💳 Payment Analytics Dashboard**
- Payment method distribution (pie chart)
- Transaction volume by method (bar chart)
- Payment success rate tracking
- Average transaction value trends

**📦 Operations Dashboard**
- Order status breakdown (pie chart)
- Processing time trends (line chart)
- Fulfillment rate monitoring
- Inventory level alerts

---

## 📈 Performance Benchmarks

| Operation | Typical Duration | Data Volume |
|-----------|------------------|-------------|
| **Data Ingestion** | 5-10 seconds | 100,000+ records |
| **ETL Transformation** | 15-30 seconds | Complete dataset |
| **Aggregations** | 10-20 seconds | Multiple tables |
| **Parquet Write** | 5-10 seconds | Compressed output |
| **SQLite Insert** | 10-15 seconds | All tables |
| **Dashboard Load** | < 1 second | Live queries |

---


## 📞 Support & Contact

<div align="center">

**Instructor:** Prof. Sandeep Kumar Srivastava

**Repository:** [https://github.com/rv-ethereal/Data_Mining_LAB](https://github.com/rv-ethereal/Data_Mining_LAB)

**Branch:** MSA24009

**Status:**  Active Development

**Institution:** Indian Institute of Information Technology, Lucknow (IIIT Lucknow)

</div>

---

## 📄 License

This project is part of the Data Mining Laboratory curriculum and follows academic usage guidelines.


<div align="center">

## 🎓 Learning Outcomes

After completing this project, you will understand:

**Data Lake Architecture** - Multi-layer design patterns  
**ETL Pipeline Development** - PySpark transformations  
**Workflow Orchestration** - Airflow DAG concepts  
**Data Warehousing** - Star-schema modeling  
**Business Intelligence** - Dashboard creation  
**Columnar Storage** - Parquet optimization  
**Container Orchestration** - Docker/Docker Compose  
**Production Deployment** - Scalability strategies  

---
