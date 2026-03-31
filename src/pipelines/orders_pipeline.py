from datetime import datetime

from src.extraction.extract_orders import extract_orders
from src.transformation.transform_orders import transform_orders
from src.validation.validate_orders import validate_orders
from src.loading.load_to_s3 import load_to_s3
from src.loading.load_to_snowflake import load_to_snowflake

from src.utils.logger import get_logger
from src.utils.report_generator import generate_report

logger = get_logger(__name__)


def run_pipeline():
    """
    Main ETL pipeline:
    Extract → Transform → Validate → Load → Report
    """

    start_time = datetime.now()
    logger.info("🚀 Pipeline started")

    try:
        # ==============================
        # 🔹 EXTRACT
        # ==============================
        logger.info("Starting extraction...")
        df = extract_orders()

        # ==============================
        # 🔹 TRANSFORM
        # ==============================
        logger.info("Starting transformation...")
        df_transformed = transform_orders(df)

        # ==============================
        # 🔹 VALIDATE
        # ==============================
        logger.info("Starting validation...")
        validate_orders(df_transformed)

        # ==============================
        # 🔹 LOAD TO S3
        # ==============================
        logger.info("Loading data to S3...")
        load_to_s3(df_transformed)

        # ==============================
        # 🔹 LOAD TO SNOWFLAKE
        # ==============================
        logger.info("Loading data to Snowflake...")
        load_to_snowflake(df_transformed)

        # ==============================
        # 🔹 REPORT
        # ==============================
        end_time = datetime.now()
        row_count = df_transformed.count()

        generate_report(start_time, end_time, row_count)

        logger.info("✅ Pipeline completed successfully")

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    run_pipeline()