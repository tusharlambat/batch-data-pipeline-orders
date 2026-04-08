from dotenv import load_dotenv
import os
import pytest
from pyspark.sql import SparkSession

load_dotenv(dotenv_path=os.path.abspath(".env.dev"))


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    try:
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("orders-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Spark is unavailable in this environment: {exc}")

    yield session
    session.stop()
