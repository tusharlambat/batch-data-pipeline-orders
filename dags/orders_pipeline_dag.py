import sys
sys.path.append("/home/tusharlmbt/Batch_data_pipeline_orders_project1")

from datetime import datetime

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utils.config import SNOWFLAKE_CONFIG

default_args = {
    "owner": "tushar",
    "retries": 1,
    "email": ["tusharpheonix@gmail.com"],
    "email_on_failure": True,
}


def _run_snowflake_sql(sql):
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
    )
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        conn.commit()
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()


def validate_row_count():
    result = _run_snowflake_sql("""
        SELECT COUNT(*) 
        FROM TUSHAR_DB.PUBLIC.ORDERS;
    """)

    row_count = result[0]

    if row_count == 0:
        raise ValueError("❌ Data validation failed: No rows loaded in ORDERS table")
    
    print(f"Row count check passed: {row_count} rows")


def run_orders_pipeline():
    from src.pipelines.orders_pipeline import run_pipeline

    run_pipeline()


def create_clean_table():
    _run_snowflake_sql("""
        CREATE OR REPLACE TABLE TUSHAR_DB.PUBLIC.ORDERS_CLEAN AS
        SELECT *
        FROM TUSHAR_DB.PUBLIC.ORDERS
        WHERE order_id IS NOT NULL;
    """)


def create_customer_summary():
    _run_snowflake_sql("""
        CREATE OR REPLACE TABLE TUSHAR_DB.PUBLIC.CUSTOMER_SUMMARY AS
        SELECT 
            segment,
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_quantity,
            SUM("List Price" * quantity) AS total_revenue
        FROM TUSHAR_DB.PUBLIC.ORDERS_CLEAN
        GROUP BY segment;
    """)


with DAG(
    dag_id="orders_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    run_pipeline_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_orders_pipeline
    )

    validate_data = PythonOperator(
        task_id="validate_row_count",
        python_callable=validate_row_count
    )

    create_clean_table_task = PythonOperator(
        task_id="create_clean_table",
        python_callable=create_clean_table,
    )

    create_customer_summary_task = PythonOperator(
        task_id="create_customer_summary",
        python_callable=create_customer_summary,
    )

    run_pipeline_task >> validate_data >> create_clean_table_task >> create_customer_summary_task
