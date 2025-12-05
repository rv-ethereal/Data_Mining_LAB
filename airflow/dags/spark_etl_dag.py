"""Data Pipeline Orchestration - Airflow DAG for ETL Processing"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

PROJECT_DIRECTORY = Path(__file__).parent.parent.parent

def validate_input_artifacts():
    """Validate presence of input data artifacts."""
    data_dir = PROJECT_DIRECTORY / "datalake/raw"
    required_files = ["sales.csv", "customers.csv"]
    
    missing = [f for f in required_files if not (data_dir / f).exists()]
    if missing:
        raise FileNotFoundError(f"Missing data files: {', '.join(missing)}")
    logger.info("All input artifacts validated")

def validate_output_artifacts():
    """Verify warehouse layer output generation."""
    warehouse_dir = PROJECT_DIRECTORY / "datalake/warehouse"
    expected_tables = [
        "revenue_by_product", "revenue_by_region", "payment_analysis",
        "status_summary", "customer_summary", "monthly_sales"
    ]
    
    missing = [t for t in expected_tables if not (warehouse_dir / t).exists()]
    if missing:
        raise FileNotFoundError(f"Missing warehouse tables: {', '.join(missing)}")
    logger.info("All warehouse artifacts validated")

default_configs = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="enterprise_etl_pipeline",
    default_args=default_configs,
    description="Data Lake ETL Pipeline Orchestration",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "data-processing", "analytics"]
) as dag:
    
    validate_inputs = PythonOperator(
        task_id="validate_input_artifacts",
        python_callable=validate_input_artifacts,
        doc="Verify raw data layer contains required input files"
    )
    
    execute_etl = BashOperator(
        task_id="execute_spark_etl",
        bash_command=f"python {PROJECT_DIRECTORY}/spark/spark_etl.py",
        doc="Execute Spark-based ETL transformation pipeline"
    )
    
    validate_outputs = PythonOperator(
        task_id="validate_output_artifacts",
        python_callable=validate_output_artifacts,
        doc="Verify warehouse layer table creation"
    )
    
    notify_completion = BashOperator(
        task_id="notify_completion",
        bash_command="echo 'ETL Pipeline Execution Completed Successfully'",
        doc="Completion notification"
    )
    
    # Task dependencies
    validate_inputs >> execute_etl >> validate_outputs >> notify_completion

