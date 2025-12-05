"""
Apache Airflow DAG for Data Lake ETL Pipeline Orchestration
Orchestrates ingestion, transformation, validation, and loading
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import os

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'spark_etl_pipeline',
    default_args=default_args,
    description='On-Premise Data Lake ETL with Spark',
    schedule_interval='@daily',
    catchup=False,
    tags=['data_mining', 'etl', 'spark'],
)

def validate_raw_data():
    """Python operator to validate raw data exists"""
    raw_path = "/home/claude/data_mining_lab/datalake/raw"
    files = ['sales.csv', 'products.json', 'customers.csv']
    
    for file in files:
        file_path = os.path.join(raw_path, file)
        if not os.path.exists(file_path):
            raise ValueError(f"Raw data file not found: {file_path}")
    
    print(f"[INFO] All raw data files validated successfully")
    return True

def post_etl_validation():
    """Python operator to validate ETL output"""
    warehouse_path = "/home/claude/data_mining_lab/datalake/warehouse"
    tables = ['enriched_sales', 'revenue_by_month', 'revenue_by_region', 'top_products']
    
    for table in tables:
        table_path = os.path.join(warehouse_path, table)
        if not os.path.exists(table_path):
            raise ValueError(f"Warehouse table not found: {table_path}")
    
    print(f"[INFO] All warehouse tables validated successfully")
    return True

# Task 1: Start pipeline
task_start = BashOperator(
    task_id='pipeline_start',
    bash_command='echo "Starting ETL Pipeline at $(date)"',
    dag=dag,
)

# Task 2: Validate raw data
task_validate_raw = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data,
    dag=dag,
)

# Task 3: Run Spark ETL
task_spark_etl = BashOperator(
    task_id='run_spark_etl',
    bash_command='''
    cd /home/claude/data_mining_lab && \
    python spark_etl/etl_pipeline.py
    ''',
    dag=dag,
)

# Task 4: Validate ETL output
task_validate_etl = PythonOperator(
    task_id='validate_etl_output',
    python_callable=post_etl_validation,
    dag=dag,
)

# Task 5: End pipeline
task_end = BashOperator(
    task_id='pipeline_end',
    bash_command='echo "ETL Pipeline completed successfully at $(date)"',
    dag=dag,
)

# Define task dependencies
task_start >> task_validate_raw >> task_spark_etl >> task_validate_etl >> task_end