import os
from pathlib import Path
from dotenv import load_dotenv

# ==============================
# 🔹 LOAD ENV FILE BASED ON ENV
# ==============================

ENV = os.getenv("ENV", "dev")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / f".env.{ENV}"

# Resolve the env file from the project root so Airflow can load it
# regardless of the process working directory.
load_dotenv(dotenv_path=ENV_FILE)

BUCKET_NAME = os.getenv("S3_BUCKET")

# 🔥 For Spark (read/write)
RAW_DATA_PATH = f"s3a://{BUCKET_NAME}/{ENV}/raw_data/orders.csv"
CLEANED_DATA_PATH = f"s3a://{BUCKET_NAME}/{ENV}/cleaned_data/"

# 🔥 For Snowflake (important)
SNOWFLAKE_S3_PATH = f"s3://{BUCKET_NAME}/{ENV}/cleaned_data/"

# ==============================
# 🔹 SNOWFLAKE CONFIG
# ==============================

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "table": "ORDERS"
}

# ==============================
# 🔹 SPARK CONFIG
# ==============================

APP_NAME = f"Batch_ETL_{ENV}"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
