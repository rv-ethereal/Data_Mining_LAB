from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# getting project path
PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

def check_files():
    # just checking if files exist before running spark
    raw_path = os.path.join(PROJECT_PATH, "datalake/raw")
    sales_file = os.path.join(raw_path, "sales.csv")
    customers_file = os.path.join(raw_path, "customers.csv")
    
    if not os.path.exists(sales_file):
        raise FileNotFoundError(f"sales file not found at {sales_file}")
    if not os.path.exists(customers_file):
        raise FileNotFoundError(f"customers file not found at {customers_file}")
    
    print(f"all files found, ready to run spark")

def validate_output():
    # checking if spark created the output files
    warehouse_path = os.path.join(PROJECT_PATH, "datalake/warehouse")
    
    expected_tables = [
        "revenue_by_product",
        "revenue_by_region",
        "payment_analysis",
        "status_summary",
        "customer_summary",
        "monthly_sales"
    ]
    
    for table in expected_tables:
        table_path = os.path.join(warehouse_path, table)
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"expected table {table} not found")
    
    print("all warehouse tables created successfully")

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="data_lake_etl_pipeline",
    default_args=default_args,
    description="runs spark etl for data lake processing",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "etl", "data-lake"]
) as dag:
    
    # task 1: check if input files exist
    check_input = PythonOperator(
        task_id="check_input_files",
        python_callable=check_files
    )
    
    # task 2: run spark etl job
    run_spark = BashOperator(
        task_id="run_spark_etl",
        bash_command=f"spark-submit {PROJECT_PATH}/spark/spark_etl.py"
    )
    
    # task 3: validate output
    validate = PythonOperator(
        task_id="validate_warehouse_output",
        python_callable=validate_output
    )
    
    # task 4: print success message
    success = BashOperator(
        task_id="print_success",
        bash_command="echo 'etl pipeline completed successfully!'"
    )
    
    # setting up task dependencies
    check_input >> run_spark >> validate >> success
