import os
from dotenv import load_dotenv

# ==============================
# 🔹 LOAD ENV FILE BASED ON ENV
# ==============================

ENV = os.getenv("ENV", "dev")

if ENV == "prod":
    load_dotenv(".env.prod")
else:
    load_dotenv(".env.dev")

# ==============================
# 🔹 S3 CONFIG
# ==============================

BUCKET_NAME = os.getenv("S3_BUCKET")

RAW_DATA_PATH = f"s3://{BUCKET_NAME}/{ENV}/raw_data/orders.csv"
CLEANED_DATA_PATH = f"s3://{BUCKET_NAME}/{ENV}/cleaned_data/"

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