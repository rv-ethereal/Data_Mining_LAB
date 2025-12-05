from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "praveen",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="retail_etl_dag",
    default_args=default_args,
    schedule_interval=None,   # No auto schedule (manual run)
    catchup=False,
) as dag:

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="docker exec spark-master /opt/spark/bin/spark-submit /opt/spark-apps/spark_etl.py"
    )
