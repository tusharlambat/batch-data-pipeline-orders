from src.utils.config import CLEANED_DATA_PATH
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_to_s3(df):
    """
    Save transformed data to S3 in Parquet format
    """

    logger.info("Starting load to S3...")

    df.write \
        .mode("overwrite") \
        .parquet(CLEANED_DATA_PATH)

    logger.info(f"Data successfully written to S3 at {CLEANED_DATA_PATH}")