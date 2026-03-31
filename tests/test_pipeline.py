from pyspark.sql import SparkSession
from src.transformation.transform_orders import transform_orders

# create spark session
spark = SparkSession.builder.getOrCreate()

def test_pipeline():

    # Step 1: Create dummy data (instead of S3)
    data = [
        
        ("1", "01/03/2023", "Second Class", "Consumer", "USA", "NY",
     "NY", "10001", "East", "Furniture", "Chairs", "P1",
     "240", "260", "2", "10"),
        ("2", "02/03/2023", "First Class", "Corporate", "USA", "LA",
     "CA", "90001", "West", "Office Supplies", "Paper", "P2",
     "200", "250", "3", "5")
    ]
    columns = [    "Order Id",
    "Order Date",
    "Ship Mode",
    "Segment",
    "Country",
    "City",
    "State",
    "Postal Code",
    "Region",
    "Category",
    "Sub Category",
    "Product Id",
    "Cost Price",
    "List Price",
    "Quantity",
    "Discount Percent"]

    df = spark.createDataFrame(data, columns)

    # Step 2: Apply transformation
    df_transformed = transform_orders(df)

    # Step 3: Simulate load (no Snowflake)
    result_count = df_transformed.count()

    # Step 4: Assertion
    assert result_count == 2