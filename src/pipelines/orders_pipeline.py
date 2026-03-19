from pyspark.sql import SparkSession
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from config.config import CONFIG
from src.extraction.extract_orders import extract_orders
from src.transformation.transform_orders import transform_orders
from src.loading.load_orders import load_orders
from src.utils.logger import logger


def main():
    logger.info("Pipeline started")

    # ✅ Create Spark session
    spark = SparkSession.builder \
        .appName("Order Pipeline") \
        .getOrCreate()

    # ✅ Environment-based config
    env = os.getenv("ENV", "dev")  # default = dev

    raw_path = CONFIG["paths"][env]["raw_data"]
    clean_path = CONFIG["paths"][env]["clean_data"]

    try:
        # 🔹 Extract
        df = extract_orders(spark, raw_path)

        # 🔹 Transform
        df_transformed = transform_orders(df)

        # 🔹 Load
        load_orders(df_transformed, clean_path)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()