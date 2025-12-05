"""
Data Lakehouse Pipeline DAG
Orchestrates Bronze → Silver → Gold ETL using Spark
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# DAG configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='End-to-end data lakehouse: Bronze → Silver → Gold',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'spark', 'delta-lake'],
)

# JAR paths for Delta Lake support
JARS = "/opt/spark/extra-jars/delta-core_2.12-2.4.0.jar,/opt/spark/extra-jars/delta-storage-2.4.0.jar,/opt/spark/extra-jars/hadoop-aws-3.3.4.jar,/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar"

# Bronze Layer Ingestion
bronze_ingestion = BashOperator(
    task_id='bronze_layer_ingestion',
    bash_command=f"""
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars {JARS} \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark-apps/jobs/bronze_ingestion.py
    """,
    dag=dag,
)

# Silver Layer Transformation
silver_transformation = BashOperator(
    task_id='silver_layer_transformation',
    bash_command=f"""
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars {JARS} \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark-apps/jobs/silver_transformation.py
    """,
    dag=dag,
)

# Gold Layer Aggregation
gold_aggregation = BashOperator(
    task_id='gold_layer_aggregation',
    bash_command=f"""
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars {JARS} \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark-apps/jobs/gold_aggregation.py
    """,
    dag=dag,
)

# Success notification
success = BashOperator(
    task_id='pipeline_complete',
    bash_command='echo "Data Lakehouse pipeline completed successfully!"',
    dag=dag,
)

# Pipeline flow
bronze_ingestion >> silver_transformation >> gold_aggregation >> success
