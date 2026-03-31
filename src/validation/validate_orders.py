from src.utils.logger import get_logger
from pyspark.sql.functions import col, count

logger = get_logger(__name__)

def validate_orders(df):
    """
    Validate data before loading
    Raises exception if validation fails
    """

    logger.info("Starting data validation...")

    # ==============================
    # 🔹 Rule 1: Check empty dataset
    # ==============================
    row_count = df.count()
    if row_count == 0:
        raise Exception("Validation Failed: Dataset is empty")

    logger.info(f"Row count check passed: {row_count} rows")

    # ==============================
    # 🔹 Rule 2: Null check (critical columns)
    # ==============================
    null_count = df.filter(col("order_id").isNull()).count()

    if null_count > 0:
        raise Exception(f"Validation Failed: order_id has {null_count} null values")

    logger.info("Null check passed for order_id")

    # ==============================
    # 🔹 Rule 3: Duplicate check
    # ==============================
    total_count = df.count()
    distinct_count = df.dropDuplicates(["order_id"]).count()

    if total_count != distinct_count:
        raise Exception("Validation Failed: Duplicate order_id found")

    logger.info("Duplicate check passed")

    # ==============================
    # 🔹 Rule 4: Business rule
    # ==============================
    invalid_quantity = df.filter(col("quantity") <= 0).count()

    if invalid_quantity > 0:
        raise Exception("Validation Failed: quantity should be > 0")

    logger.info("Business rule check passed")

    logger.info("All validations passed successfully")

    return True