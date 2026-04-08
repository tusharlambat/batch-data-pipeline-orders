from src.utils.spark_session import get_spark_session
from src.utils.config import RAW_DATA_PATH
from src.utils.logger import get_logger

logger = get_logger(__name__)

def extract_orders():
    """
    Extract raw orders data from S3 (CSV) and return Spark DataFrame.
    """
    logger.info("🚀 Starting extraction...")

    # Create Spark session
    spark = get_spark_session()

    # RAW_DATA_PATH is configured as s3a://<bucket>/<env>/raw_data/orders.csv
    # Keep extraction strictly on S3 bucket storage.
    logger.info(f"Reading raw data from S3 path: {RAW_DATA_PATH}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .option("escape", '"')
        .option("quote", '"')
        .csv(RAW_DATA_PATH)
    )

    logger.info("Schema after extraction:")
    df.printSchema()

    logger.info("Extraction completed successfully")

    return df
