import snowflake.connector
from botocore.session import Session

from src.utils.config import SNOWFLAKE_CONFIG, SNOWFLAKE_S3_PATH
from src.utils.logger import get_logger

logger = get_logger(__name__)

SNOWFLAKE_TABLE_DDL = """
CREATE OR REPLACE TABLE {table} (
    order_id NUMBER,
    order_date DATE,
    ship_mode VARCHAR,
    segment VARCHAR,
    country VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    region VARCHAR,
    category VARCHAR,
    sub_category VARCHAR,
    product_id VARCHAR,
    cost_price FLOAT,
    list_price FLOAT,
    quantity NUMBER,
    discount_percent FLOAT,
    final_price FLOAT
)
"""


def _resolve_aws_credentials():
    session = Session()
    credentials = session.get_credentials()

    if credentials is None:
        raise RuntimeError(
            "AWS credentials were not found. Snowflake cannot load from S3 "
            "without AWS access to the cleaned parquet path."
        )

    frozen = credentials.get_frozen_credentials()
    if not frozen.access_key or not frozen.secret_key:
        raise RuntimeError(
            "Incomplete AWS credentials resolved. Both access key and secret key are required."
        )

    return frozen


def _build_copy_into_sql(table_name, s3_path, aws_credentials):
    credentials_clause = (
        f"AWS_KEY_ID='{aws_credentials.access_key}' "
        f"AWS_SECRET_KEY='{aws_credentials.secret_key}'"
    )

    if aws_credentials.token:
        credentials_clause += f" AWS_TOKEN='{aws_credentials.token}'"

    return f"""
COPY INTO {table_name}
FROM '{s3_path}'
CREDENTIALS=({credentials_clause})
FILE_FORMAT=(TYPE=PARQUET)
PATTERN='.*[.]parquet'
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
FORCE=TRUE
"""


def load_to_snowflake(_df=None):
    logger.info("Connecting to Snowflake...")

    s3_path = SNOWFLAKE_S3_PATH
    if not s3_path.startswith("s3://"):
        raise ValueError(
            f"Snowflake load requires an S3 path, got {s3_path!r}. "
            "Make sure the cleaned dataset is written to S3 before loading."
        )

    aws_credentials = _resolve_aws_credentials()
    table_name = SNOWFLAKE_CONFIG["table"]

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
        logger.info("Connection established. Preparing Snowflake table...")
        cursor.execute(SNOWFLAKE_TABLE_DDL.format(table=table_name))

        logger.info("Starting Snowflake COPY INTO from S3 path: %s", s3_path)
        copy_sql = _build_copy_into_sql(table_name, s3_path, aws_credentials)
        cursor.execute(copy_sql)

        result = cursor.fetchone()
        conn.commit()

        logger.info("Snowflake load finished. COPY INTO result: %s", result)
        return result
    finally:
        cursor.close()
        conn.close()
