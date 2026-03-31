from pyspark.sql.functions import col, to_date
from src.utils.logger import get_logger

logger = get_logger(__name__)

def transform_orders(df):
    """
    Transform raw orders data:
    - Rename columns
    - Change data types
    - Create new business columns
    """

    logger.info("Starting transformation step...")

    # ==============================
    # 🔹 Rename Columns
    # ==============================
    df = df.withColumnRenamed("Order Id", "order_id") \
           .withColumnRenamed("Order Date", "order_date") \
           .withColumnRenamed("Ship Mode", "ship_mode") \
           .withColumnRenamed("Segment", "segment") \
           .withColumnRenamed("Country", "country") \
           .withColumnRenamed("City", "city") \
           .withColumnRenamed("State", "state") \
           .withColumnRenamed("Postal Code", "postal_code") \
           .withColumnRenamed("Region", "region") \
           .withColumnRenamed("Category", "category") \
           .withColumnRenamed("Sub Category", "sub_category") \
           .withColumnRenamed("Product Id", "product_id") \
           .withColumnRenamed("Cost Price", "cost_price") \
           .withColumnRenamed("List Price", "list_price") \
           .withColumnRenamed("Quantity", "quantity") \
           .withColumnRenamed("Discount Percent", "discount_percent")

    logger.info("Columns renamed successfully")

    # ==============================
    # 🔹 Convert Data Types
    # ==============================
    df = df.withColumn("order_date", to_date(col("order_date"), "MM/dd/yyyy"))

    df = df.withColumn("cost_price", col("cost_price").cast("double")) \
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

    logger.info("Transformation completed successfully")

    return df