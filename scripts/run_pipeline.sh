#!/bin/bash

echo "Starting ETL Pipeline..."

cd ~/Batch_data_pipeline_orders_project1

source airflow_env/bin/activate

python src/pipeline/orders_pipeline.py

echo "Pipeline Completed!"