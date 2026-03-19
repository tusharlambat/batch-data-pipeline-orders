from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "tushar",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="orders_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "orders"]
) as dag:

    run_pipeline = BashOperator(
        task_id="run_orders_pipeline",
        bash_command="""
        export PYTHONPATH=/home/tusharlmbt/Batch_data_pipeline_orders_project1
        python /home/tusharlmbt/Batch_data_pipeline_orders_project1/src/pipelines/orders_pipeline.py
        """
    )