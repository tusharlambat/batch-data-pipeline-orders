from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "tushar",
    "retries": 1
}

with DAG(
    dag_id="orders_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",   # runs daily
    catchup=False
) as dag:

    run_pipeline = BashOperator(
        task_id="run_etl_pipeline",
        bash_command="bash ~/Batch_data_pipeline_orders_project1/scripts/run_pipeline.sh"
    )