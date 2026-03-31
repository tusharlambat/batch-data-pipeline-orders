from src.utils.spark_session import get_spark_session
from src.utils.config import RAW_DATA_PATH
from src.utils.logger import get_logger
from pyspark.sql.functions import col, count

logger = get_logger(__name__)

def observe_orders():
    """
    Perform exploratory data analysis on raw dataset
    """

    logger.info("Starting data observation...")

    spark = get_spark_session()

    # Load data from S3
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_DATA_PATH)

    # ==============================
    # 🔹 Basic Info
    # ==============================
    logger.info(f"Total rows: {df.count()}")
    logger.info(f"Total columns: {len(df.columns)}")

    # ==============================
    # 🔹 Schema
    # ==============================
    df.printSchema()

    # ==============================
    # 🔹 Sample Data
    # ==============================
    df.show(5)

    # ==============================
    # 🔹 Null Check
    # ==============================
    null_df = df.select([
        count(col(c)).alias(c) for c in df.columns
    ])
    null_df.show()

    # ==============================
    # 🔹 Duplicate Check
    # ==============================
    total_count = df.count()
    distinct_count = df.dropDuplicates().count()

    logger.info(f"Total rows: {total_count}")
    logger.info(f"Distinct rows: {distinct_count}")

    logger.info("Observation completed successfully")


if __name__ == "__main__":
    observe_orders()