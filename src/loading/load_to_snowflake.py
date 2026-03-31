import snowflake.connector
from src.utils.config import SNOWFLAKE_CONFIG
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_to_snowflake(df):
    """
    Load data into Snowflake table
    """

    logger.info("Connecting to Snowflake...")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )

    cursor = conn.cursor()

    logger.info("Connection established. Loading data...")

    # ⚠️ Basic approach (for learning)
    for row in df.collect():
        cursor.execute(f"""
            INSERT INTO {SNOWFLAKE_CONFIG['table']}
            VALUES {tuple(row)}
        """)

    logger.info("Data successfully loaded into Snowflake")

    cursor.close()
    conn.close()