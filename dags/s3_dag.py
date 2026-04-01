from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3


def test_s3_connection():
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()

        print("✅ Connected to S3")
        for bucket in response['Buckets']:
            print(bucket['Name'])

    except Exception as e:
        print("❌ Error:", str(e))
        raise e


with DAG(
    dag_id='test_s3_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    s3_task = PythonOperator(
        task_id='test_s3_connection',
        python_callable=test_s3_connection
    )

