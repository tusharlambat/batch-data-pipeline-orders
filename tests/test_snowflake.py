from dotenv import load_dotenv
import os

# ✅ Absolute path (important fix)
load_dotenv(dotenv_path=os.path.abspath(".env.dev"))

from src.utils.config import SNOWFLAKE_CONFIG
import snowflake.connector


def test_snowflake_connection():
    print("DEBUG:", SNOWFLAKE_CONFIG)  # 👈 add this

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT CURRENT_DATABASE();")
    result = cur.fetchone()

    assert result is not None