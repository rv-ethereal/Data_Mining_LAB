"""
ETL Pipeline DAG for Local Data Engineering Project
Orchestrates the data flow from raw to warehouse
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import shutil
import pandas as pd
import json

# Define default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Get base path (parent of dags folder)
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATALAKE_PATH = os.path.join(BASE_PATH, 'datalake')

def validate_raw_files():
    """Validate that raw data files exist and are readable"""
    raw_path = os.path.join(DATALAKE_PATH, 'raw')
    required_files = ['sales.csv', 'products.json', 'customers.csv']
    
    print(f"Validating raw files in: {raw_path}")
    
    for file in required_files:
        file_path = os.path.join(raw_path, file)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Required file not found: {file}")
        
        # Check if file is readable and not empty
        if os.path.getsize(file_path) == 0:
            raise ValueError(f"File is empty: {file}")
        
        print(f"✓ Validated: {file}")
    
    # Additional validation: check CSV structure
    sales_df = pd.read_csv(os.path.join(raw_path, 'sales.csv'))
    customers_df = pd.read_csv(os.path.join(raw_path, 'customers.csv'))
    
    print(f"✓ Sales records: {len(sales_df)}")
    print(f"✓ Customer records: {len(customers_df)}")
    
    # Check products JSON
    with open(os.path.join(raw_path, 'products.json'), 'r') as f:
        products = json.load(f)
        print(f"✓ Product records: {len(products)}")
    
    print("✓ All raw files validated successfully")

def stage_raw_data():
    """Copy raw files to staging area with basic validation"""
    raw_path = os.path.join(DATALAKE_PATH, 'raw')
    staging_path = os.path.join(DATALAKE_PATH, 'staging')
    
    print(f"Staging data from {raw_path} to {staging_path}")
    
    # Clear staging area
    if os.path.exists(staging_path):
        for file in os.listdir(staging_path):
            os.remove(os.path.join(staging_path, file))
    
    # Copy files to staging
    files_to_stage = ['sales.csv', 'products.json', 'customers.csv']
    
    for file in files_to_stage:
        src = os.path.join(raw_path, file)
        dst = os.path.join(staging_path, file)
        shutil.copy2(src, dst)
        print(f"✓ Staged: {file}")
    
    print("✓ Staging complete")

def verify_spark_output():
    """Verify that Spark ETL produced expected output files"""
    processed_path = os.path.join(DATALAKE_PATH, 'processed')
    warehouse_path = os.path.join(DATALAKE_PATH, 'warehouse')
    
    print("Verifying Spark ETL output...")
    
    # Check processed zone
    if not os.path.exists(processed_path) or not os.listdir(processed_path):
        raise FileNotFoundError("Processed data not found")
    
    # Check warehouse zone for dimension and fact tables
    required_tables = ['fact_sales.parquet', 'dim_products.parquet', 'dim_customers.parquet']
    
    for table in required_tables:
        table_path = os.path.join(warehouse_path, table)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Warehouse table not found: {table}")
        print(f"✓ Found: {table}")
    
    # Read and validate data
    fact_sales = pd.read_parquet(os.path.join(warehouse_path, 'fact_sales.parquet'))
    dim_products = pd.read_parquet(os.path.join(warehouse_path, 'dim_products.parquet'))
    dim_customers = pd.read_parquet(os.path.join(warehouse_path, 'dim_customers.parquet'))
    
    print(f"✓ Fact Sales records: {len(fact_sales)}")
    print(f"✓ Dimension Products records: {len(dim_products)}")
    print(f"✓ Dimension Customers records: {len(dim_customers)}")
    print("✓ Spark output verified successfully")

# Create the DAG
with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline from raw data to warehouse',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,  # Don't run for past dates
    tags=['etl', 'data-engineering'],
) as dag:
    
    # Task 1: Validate raw files
    task_validate = PythonOperator(
        task_id='validate_raw_files',
        python_callable=validate_raw_files,
    )
    
    # Task 2: Stage raw data
    task_stage = PythonOperator(
        task_id='stage_raw_data',
        python_callable=stage_raw_data,
    )
    
    # Task 3: Run Spark ETL
    task_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command=f'cd {BASE_PATH} && python spark/etl_job.py',
    )
    
    # Task 4: Verify Spark output
    task_verify = PythonOperator(
        task_id='verify_spark_output',
        python_callable=verify_spark_output,
    )
    
    # Task 5: Load warehouse
    task_load_warehouse = BashOperator(
        task_id='load_warehouse',
        bash_command=f'cd {BASE_PATH} && python warehouse/load_data.py',
    )
    
    # Define task dependencies
    task_validate >> task_stage >> task_spark_etl >> task_verify >> task_load_warehouse

# This allows the DAG to be tested standalone
if __name__ == "__main__":
    print("Testing DAG structure...")
    print(f"DAG ID: {dag.dag_id}")
    print(f"Tasks: {[task.task_id for task in dag.tasks]}")
    print("✓ DAG structure is valid")
