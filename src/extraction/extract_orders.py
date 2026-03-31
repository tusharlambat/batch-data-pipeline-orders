from src.utils.spark_session import get_spark_session
from src.utils.config import RAW_DATA_PATH

def extract_orders():
    """
    Extract data from S3 (CSV) and return Spark DataFrame
    """

    # Create Spark session
    spark = get_spark_session()

    # Read CSV from S3
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(RAW_DATA_PATH)

    return df