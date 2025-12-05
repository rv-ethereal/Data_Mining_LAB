from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'bhawana_dhaka',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='spark_etl_pipeline_msd24002',
    default_args=default_args,
    description='Spark ETL Pipeline - MSD24002',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'etl', 'msd24002'],
)

validate_raw = BashOperator(
    task_id='validate_raw_data',
    bash_command='ls -lh /opt/airflow/datalake/raw',
    dag=dag,
)

run_etl = BashOperator(
    task_id='run_spark_etl',
    bash_command='cd /opt/airflow && python3 spark_jobs/etl_pipeline.py',
    dag=dag,
)

validate_processed = BashOperator(
    task_id='validate_processed',
    bash_command='ls -lh /opt/airflow/datalake/processed',
    dag=dag,
)

success = BashOperator(
    task_id='success',
    bash_command='echo "✓ Pipeline Complete - MSD24002"',
    dag=dag,
)

validate_raw >> run_etl >> validate_processed >> success
