# 🚀 Data Lakehouse with Medallion Architecture


A production-ready end-to-end data lakehouse implementation using modern data stack technologies. This project demonstrates a medallion architecture (Bronze → Silver → Gold) with automated ETL pipelines, distributed query engine, and interactive dashboards.

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────┐     ┌────────┐     ┌───────┐     ┌──────────┐
│   MinIO     │────▶│  Spark   │────▶│ Delta  │────▶│ Trino │────▶│ Superset │
│  (Storage)  │     │  (ETL)   │     │  Lake  │     │(Query)│     │  (Viz)   │
└─────────────┘     └──────────┘     └────────┘     └───────┘     └──────────┘
       ▲                   ▲
       │                   │
       └───────────────────┴─── Orchestrated by Apache Airflow
```

## 🚀 Quick Start

### ⚡ One-Click Execution (Windows)

We have provided a PowerShell script to automate the entire process:

```powershell
./execute_pipeline.ps1
```

This script will:
1. Start all Docker services
2. Wait for Airflow to be ready
3. Generate sample data
4. Upload data to MinIO
5. Trigger the Airflow pipeline
6. Monitor the execution status

### 🐳 Manual Setup

### Step 1: Clone and Setup

```bash
git clone <repository-url>
cd MSD24001-L1

# Copy environment template
cp .env.example .env

# Optional: Customize credentials in .env
# nano .env
```

### Step 2: Start Services

```bash
# Start all containers
docker-compose up -d

# Wait for services to initialize (~2-3 minutes)
docker-compose ps
```

### Step 3: Access Web Interfaces

| Service   | URL                      | Username | Password |
|-----------|--------------------------|----------|----------|
| Airflow   | http://localhost:8080    | admin    | admin    |
| Superset  | http://localhost:8088    | admin    | admin    |
| MinIO     | http://localhost:9001    | admin    | minio123 |
| Trino     | http://localhost:8081    | -        | -        |

### Step 4: Run Sample Pipeline

1. **Generate Sample Data**
   ```bash
   # Generate sample sales data
   python data/sample/generator.py
   ```

2. **Upload to MinIO**
   ```bash
   # Install dependencies
   pip install minio pandas

   # Upload to bronze bucket
   python scripts/upload_to_minio.py
   ```

3. **Trigger Airflow DAG**
   - Go to http://localhost:8080
   - Enable the `lakehouse_etl` DAG
   - Click "Trigger DAG" button
   - Monitor task execution in the Graph view

4. **View Data in Trino**
   ```bash
   docker exec -it trino trino
   
   # In Trino CLI:
   SHOW SCHEMAS FROM delta;
   USE delta.gold;
   SHOW TABLES;
   SELECT * FROM daily_sales LIMIT 10;
   ```

5. **Create Superset Dashboard**
   - Go to http://localhost:8088
   - Settings → Database Connections → + Database
   - URI: `trino://admin@trino:8080/delta`
   - Test connection → Connect
   - Create datasets from `gold` schema tables
   - Build visualizations and dashboards

## 📚 Tech Stack

| Component       | Technology              | Purpose                          |
|-----------------|-------------------------|----------------------------------|
| Storage         | MinIO                   | S3-compatible object storage     |
| Metadata Format | Delta Lake 2.4.0        | ACID transactions, time travel   |
| Processing      | Apache Spark 3.4.1      | Distributed data transformations |
| Orchestration   | Apache Airflow 2.8.0    | Workflow scheduling & monitoring |
| Query Engine    | Trino (latest)          | Fast distributed SQL queries     |
| Visualization   | Apache Superset (latest)| Interactive dashboards           |
| Metadata DB     | PostgreSQL 15           | Airflow & Superset metadata      |

## 📂 Project Structure

```
.
├── airflow/
│   └── dags/
│       └── lakehouse_dag.py          # Main ETL pipeline DAG
├── spark/
│   ├── jobs/
│   │   ├── bronze_ingestion.py       # Raw data ingestion
│   │   ├── silver_transformation.py  # Data quality & cleaning
│   │   └── gold_aggregation.py       # Business metrics
│   ├── Dockerfile                    # Custom Spark image
│   └── jars/                         # Delta Lake dependencies
├── trino/
│   └── catalog/
│       └── delta.properties          # Delta Lake connector config
├── superset/
│   └── Dockerfile                    # Custom Superset with Trino driver
├── data/
│   └── sample/
│       └── generator.py              # Sample data generator
├── scripts/
│   └── upload_to_minio.py            # Data upload utility
├── docker-compose.yml                # Container orchestration
├── .env.example                      # Environment template
└── README.md                         # This file
```

## 🔧 Configuration

### Environment Variables

Edit `.env` file to customize:

```ini
# MinIO credentials
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=minio123

# Airflow admin
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin

# Superset admin
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin
SUPERSET_SECRET_KEY=change_this_in_production
```

### Spark Configuration

Modify `spark/jobs/*.py` for custom transformations. Delta Lake packages are automatically downloaded via Maven:

```python
--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4
```

### Trino Catalog

The Delta Lake catalog is configured in `trino/catalog/delta.properties`:

```properties
connector.name=delta_lake
hive.metastore=file
hive.metastore.catalog.dir=s3://gold/metastore
s3.endpoint=http://minio:9000
s3.path-style-access=true
```

## 🛠️ Troubleshooting

### Services Won't Start

```bash
# Check logs
docker-compose logs -f [service-name]

# Restart specific service
docker-compose restart [service-name]

# Reset everything
docker-compose down -v
docker-compose up -d
```

### Airflow Tasks Failing

1. **Check task logs**: Airflow UI → DAGs → lakehouse_etl → Graph → Click task → Logs
2. **Common issues**:
   - Missing packages: Verify Spark `--packages` in DAG
   - Docker socket access: Ensure `/var/run/docker.sock` is mounted
   - Network issues: All containers must be on `lakehouse-network`

### Trino Connection Errors

```bash
# Test Trino connectivity from Superset
docker exec superset curl http://trino:8080/v1/info

# Check catalog loading
docker logs trino | grep delta
```

### MinIO Access Issues

```bash
# Verify buckets exist
docker exec lakehouse-minio mc ls minio/

# Create missing buckets
docker exec lakehouse-minio mc mb minio/bronze
docker exec lakehouse-minio mc mb minio/silver
docker exec lakehouse-minio mc mb minio/gold
```

## 🧪 Development

### Adding New Transformations

1. Create Spark job in `spark/jobs/`
2. Add task to `airflow/dags/lakehouse_dag.py`
3. Set task dependencies: `bronze >> silver >> gold >> new_task`
4. Test locally: `docker exec spark /opt/spark/bin/spark-submit ...`

### Custom Superset Charts

1. Install additional viz plugins in `superset/Dockerfile`
2. Rebuild: `docker-compose up -d --build superset`

## 📊 Sample Queries

### Daily Sales Analysis

```sql
SELECT 
    date,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT customer_id) as unique_customers,
    AVG(order_value) as avg_order_value
FROM delta.gold.daily_sales
WHERE date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY date
ORDER BY date DESC;
```

### Product Performance

```sql
SELECT 
    product_category,
    SUM(quantity_sold) as units_sold,
    SUM(revenue) as total_revenue,
    AVG(profit_margin) as avg_margin
FROM delta.gold.product_performance
GROUP BY product_category
ORDER BY total_revenue DESC
LIMIT 10;
```

## 🚦 Health Checks

Monitor service health:

```bash
# Airflow
curl http://localhost:8080/health

# Superset  
curl http://localhost:8088/health

# Trino
curl http://localhost:8081/v1/info

# MinIO
curl http://localhost:9000/minio/health/live
```

## 📝 License

This project is for educational purposes as part of the MSD24001-L1 Data Mining course.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📧 Support

For issues or questions:
- Check [Troubleshooting](#-troubleshooting) section
- Review container logs: `docker-compose logs -f`
- Open an issue on the repository

---

**Note**: This is a development setup. For production deployment, implement proper security (secrets management, network isolation, authentication), scalability (cluster mode for Spark/Trino), and monitoring (Prometheus + Grafana).
