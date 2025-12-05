"""
Apache Airflow DAG for Data Lake ETL Pipeline
Orchestrates the complete ETL workflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

# Default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Project paths
PROJECT_ROOT = 'c:/Users/tb619/Videos/Dm assignment'
SPARK_SCRIPT = os.path.join(PROJECT_ROOT, 'spark', 'etl_pipeline.py')
VALIDATION_SCRIPT = os.path.join(PROJECT_ROOT, 'scripts', 'validate_pipeline.py')

def check_raw_data():
    """Check if raw data files exist"""
    import os
    
    raw_path = os.path.join(PROJECT_ROOT, 'datalake', 'raw')
    required_files = ['sales.csv', 'products.json', 'customers.csv']
    
    for file in required_files:
        file_path = os.path.join(raw_path, file)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Required file not found: {file_path}")
    
    print("✓ All required raw data files found")
    return True

def validate_processed_data():
    """Validate processed data quality"""
    import os
    
    processed_path = os.path.join(PROJECT_ROOT, 'datalake', 'processed')
    warehouse_path = os.path.join(PROJECT_ROOT, 'datalake', 'warehouse')
    
    # Check if processed directories exist
    required_dirs = [
        os.path.join(processed_path, 'sales'),
        os.path.join(processed_path, 'products'),
        os.path.join(processed_path, 'customers'),
        os.path.join(warehouse_path, 'fact_sales'),
        os.path.join(warehouse_path, 'dim_products'),
        os.path.join(warehouse_path, 'dim_customers'),
    ]
    
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            raise FileNotFoundError(f"Expected directory not found: {dir_path}")
    
    print("✓ All processed data directories exist")
    return True

def send_success_notification():
    """Send success notification"""
    print("=" * 60)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Timestamp: {datetime.now()}")
    print("All tasks completed successfully!")
    return True

# Create DAG
with DAG(
    'spark_etl_pipeline',
    default_args=default_args,
    description='Data Lake ETL Pipeline with Apache Spark',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['data-lake', 'etl', 'spark'],
) as dag:
    
    # Task 1: Check for raw data files
    check_data = PythonOperator(
        task_id='check_raw_data',
        python_callable=check_raw_data,
    )
    
    # Task 2: Run Spark ETL Pipeline
    run_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command=f'python {SPARK_SCRIPT}',
    )
    
    # Alternative: Use SparkSubmitOperator (requires Spark installation)
    # run_spark_etl = SparkSubmitOperator(
    #     task_id='run_spark_etl',
    #     application=SPARK_SCRIPT,
    #     name='data_lake_etl',
    #     conn_id='spark_default',
    #     verbose=True,
    # )
    
    # Task 3: Validate processed data
    validate_data = PythonOperator(
        task_id='validate_processed_data',
        python_callable=validate_processed_data,
    )
    
    # Task 4: Send success notification
    notify_success = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
    )
    
    # Define task dependencies
    check_data >> run_spark_etl >> validate_data >> notify_success
