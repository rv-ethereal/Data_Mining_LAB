<style>
  .header-container {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 40px 20px;
    border-radius: 10px;
    color: white;
    text-align: center;
    margin-bottom: 30px;
    box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
  }
  .header-container h1 {
    margin: 0;
    font-size: 2.5em;
    font-weight: 700;
  }
  .tagline {
    font-size: 1.1em;
    opacity: 0.95;
    margin-top: 10px;
    font-weight: 300;
  }
  .feature-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 20px;
    margin: 40px 0;
  }
  .feature-card {
    background: linear-gradient(135deg, #f5f7fa 0%, #ffffff 100%);
    border-left: 5px solid #667eea;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    transition: transform 0.3s, box-shadow 0.3s;
  }
  .feature-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 4px 16px rgba(102, 126, 234, 0.15);
  }
  .feature-card strong {
    color: #667eea;
    font-size: 1.1em;
  }
  .badge-container {
    display: flex;
    justify-content: center;
    gap: 12px;
    flex-wrap: wrap;
    margin: 30px 0;
  }
  .badge {
    background: #667eea;
    color: white;
    padding: 8px 16px;
    border-radius: 25px;
    font-size: 0.9em;
    font-weight: 500;
  }
</style>

<div class="header-container">
<h1>🚀 Enterprise Data Lake Platform</h1>
<p class="tagline">Production-grade on-premise data engineering with Apache Spark, Airflow & Superset</p>
</div>

<div class="badge-container">
<span class="badge">Python 3.10+</span>
<span class="badge">Spark 3.5+</span>
<span class="badge">Airflow 2.6.3</span>
<span class="badge">Production Ready</span>
</div>

## Overview

A complete, enterprise-grade data engineering platform demonstrating the full lifecycle: **ingestion → transformation → orchestration → warehousing → analytics**. Built for local deployment without cloud dependencies.

<div class="feature-grid">
<div class="feature-card">
<strong>⚡ ETL Pipeline</strong><br/>
Distributed Apache Spark processing with data validation, cleansing, and transformation
</div>
<div class="feature-card">
<strong>🔄 Orchestration</strong><br/>
Apache Airflow automation with scheduling, dependency management, and monitoring
</div>
<div class="feature-card">
<strong>📊 Analytics</strong><br/>
Pre-built warehouse views for revenue, customer, and operational metrics
</div>
<div class="feature-card">
<strong>🎨 Dashboards</strong><br/>
Interactive Superset visualizations with filtering and drill-down capabilities
</div>
</div>

## 📂 Project Structure

```
onprem-datalake-msd24014/
├── spark/spark_etl.py          ⚡ ETL pipeline
├── airflow/dags/               🔄 DAG orchestration
├── tools/parquet_to_sqlite.py  🔧 Export utility
├── datalake/
│   ├── raw/                    📥 Source data
│   ├── processed/              🔄 Cleansed data
│   └── warehouse/              💾 Analytics tables
├── app.py                      🎨 Analytics app
└── requirements.txt            📦 Dependencies
```

## 🛠️ Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Processing** | Apache Spark | 3.5+ | Distributed ETL |
| **Orchestration** | Apache Airflow | 2.6.3 | Workflow automation |
| **Visualization** | Apache Superset | Latest | Dashboards & BI |
| **Language** | Python | 3.10+ | Core programming |
| **Storage** | Parquet/SQLite | Latest | Data warehouse |

## 📊 Analytical Tables

| Table | Description |
|-------|-------------|
| **revenue_by_product** | Product performance & pricing metrics |
| **revenue_by_region** | Geographic revenue distribution |
| **payment_analysis** | Payment method adoption & volume |
| **customer_summary** | Customer lifetime value & behavior |
| **status_summary** | Order status distribution & tracking |
| **monthly_sales** | Temporal trends & seasonal analysis |

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Java 11+ (for Spark)
- 8GB RAM minimum
- 20GB disk space

### Installation

```bash
# 1. Clone repository
git clone https://github.com/rv-ethereal/Data_Mining_LAB.git
cd onprem-datalake-msd24014

# 2. Create virtual environment
python -m venv myeve

# 3. Activate (Windows)
.\myeve\Scripts\Activate.ps1

# 4. Install dependencies
pip install -r requirements.txt

# 5. Run ETL pipeline
python spark/spark_etl.py

# 6. Export to SQLite
python tools/parquet_to_sqlite.py

# 7. Start analytics platform
python app.py
```

**Access dashboards:** http://localhost:8088

## 📈 Data Pipeline

```
Raw Data → Validation → Cleaning → Transformation → Aggregation → Warehouse → Dashboards
```

**Key Operations:**
- Data ingestion from CSV/JSON sources
- Null/duplicate removal and type conversion
- Dataset joins and feature engineering
- Automatic aggregation and warehouse loading
- Real-time dashboard updates

## 💡 Key Metrics

- **Total Revenue** - SUM(final_amount)
- **Average Order Value** - AVG(final_amount)
- **Customer Count** - COUNT(DISTINCT cust_id)
- **Unit Sales** - SUM(qty)
- **Regional Performance** - Revenue per region

## 🎯 Use Cases

| Domain | Applications |
|--------|--------------|
| **Business Intelligence** | KPI tracking, trend analysis, decision support |
| **Financial Analysis** | Revenue tracking, profitability, forecasting |
| **Operations** | Process efficiency, quality metrics, resource utilization |
| **Customer Analytics** | Segmentation, lifetime value, churn prediction |
| **Product Management** | Performance metrics, feature adoption, prioritization |

## 📈 Performance

| Operation | Duration | Data Volume |
|-----------|----------|------------|
| Data Ingestion | 5-10s | 100K+ records |
| ETL Transformation | 15-30s | All data |
| Aggregations | 10-20s | 6 tables |
| Dashboard Query | <1s | Full datasets |

## 🔧 Configuration

**Environment Variables** (auto-configured):
- `SUPERSET_HOME` - Superset configuration directory
- `SUPERSET_SECRET_KEY` - Encryption key
- `FLASK_APP` - Application entry point
- `PYTHONPATH` - Python module path

**Feature Flags** (in `superset_config.py`):
- Template Processing
- Adhoc Subqueries
- CSRF Protection

## 🛡️ Security

**Development:**
- Local filesystem storage
- SQLite database
- Admin: admin/admin

**Production:**
- PostgreSQL backend
- HTTPS/SSL encryption
- Strong password policies
- Role-based access control
- Audit logging

## 📞 Contact & Support

- **Instructor:** Prof. Sandeep Kumar Srivastava
- **Repository:** [https://github.com/rv-ethereal/Data_Mining_LAB](https://github.com/rv-ethereal/Data_Mining_LAB)
- **Branch:** msd24014
- **Status:** Active Development

## 📄 License

Academic usage - part of Data Mining Laboratory curriculum

## 🙏 Credits

- Apache Foundation (Spark, Airflow, Superset)
- Prof. Sandeep Kumar Srivastava
- Data Mining Laboratory (IIT-BHU)

---

**Version:** 1.0 | **Status:** ✅ Production Ready | **Updated:** December 2025
