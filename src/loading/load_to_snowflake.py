import snowflake.connector
from src.utils.logger import logger


def load_to_snowflake():

    logger.info("Connecting to Snowflake")

    conn = snowflake.connector.connect(
        user="YOUR_USER",
        password="YOUR_PASSWORD",
        account="YOUR_ACCOUNT"
    )

    cursor = conn.cursor()

    cursor.execute("COPY INTO orders_table FROM @s3_stage")

    logger.info("Snowflake load completed")

    conn.close()