from pyspark.sql.functions import col, coalesce, regexp_replace, to_date
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _normalize_columns(df):
    """
    Normalize incoming column names to lower_snake_case so source header
    differences like 'Cost Price' vs 'cost price' do not break transforms.
    """
    normalized = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df.toDF(*normalized)


def transform_orders(df):

    logger.info("Starting transformation step...")

    # Normalize headers from source CSV/S3 to stable snake_case names.
    df = _normalize_columns(df)
    logger.info("Columns normalized successfully")

    # ==============================
    # 🔹 Clean Dirty Data (FIX)
    # ==============================
    df = df.withColumn("quantity", regexp_replace(col("quantity"), "[^0-9]", "")) \
           .withColumn("cost_price", regexp_replace(col("cost_price"), "[^0-9.]", "")) \
           .withColumn("list_price", regexp_replace(col("list_price"), "[^0-9.]", "")) \
           .withColumn("discount_percent", regexp_replace(col("discount_percent"), "[^0-9.]", ""))

    logger.info("Dirty data cleaned")

    # ==============================
    # 🔹 Convert Data Types
    # ==============================
    # Support both yyyy-MM-dd (production CSV) and dd/MM/yyyy (older test data).
    df = df.withColumn(
        "order_date",
        coalesce(
            to_date(col("order_date"), "yyyy-MM-dd"),
            to_date(col("order_date"), "dd/MM/yyyy"),
        ),
    ).withColumn("cost_price", col("cost_price").cast("double")) \
     .withColumn("list_price", col("list_price").cast("double")) \
     .withColumn("quantity", col("quantity").cast("int")) \
     .withColumn("discount_percent", col("discount_percent").cast("double"))

    logger.info("Data types converted")

    # ==============================
    # 🔹 Business Logic
    # ==============================
    df = df.withColumn(
        "final_price",
        col("list_price") * (1 - col("discount_percent") / 100)
    )

    logger.info("Business column 'final_price' created")
    logger.info("🔥 CLEANING STEP APPLIED")
    logger.info("Transformation completed successfully")

    return df
