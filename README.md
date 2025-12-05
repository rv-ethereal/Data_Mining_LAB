# Data Lakehouse Project

End-to-end data lakehouse using Apache Spark, Airflow, MinIO, Trino, and Superset.

## Quick Start

```bash
# 1. Start all services
docker-compose up -d

# 2. Generate sample data  
python data/sample/generator.py

# 3. Upload to MinIO
pip install minio pandas
python scripts/upload_to_minio.py

# 4. Trigger pipeline
# Go to http://localhost:8082 (Airflow)
# Enable and trigger 'lakehouse_pipeline' DAG

# 5. Create dashboards
# Go to http://localhost:8088 (Superset)
```

**Gold Layer**: Business aggregations for dashboards  

## Tech Stack

- **Storage**: MinIO (S3-compatible)
- **Compute**: Apache Spark 3.4.0
- **Format**: Delta Lake 2.4.0
- **Orchestration**: Apache Airflow 2.8
- **Query**: Trino
- **Visualization**: Apache Superset
