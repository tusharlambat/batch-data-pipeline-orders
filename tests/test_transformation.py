from pyspark.sql import SparkSession
from src.transformation.transform_orders import transform_orders

# create spark session
spark = SparkSession.builder.getOrCreate()


def test_transform_orders():
    # ✅ Dummy data (same as raw schema)
    data = [
        ("1", "01/03/2023", "Second Class", "Consumer", "USA", "NY",
         "NY", "10001", "East", "Furniture", "Chairs", "P1",
         "240", "260", "2", "10"),

        ("2", "02/03/2023", "First Class", "Corporate", "USA", "LA",
         "CA", "90001", "West", "Office Supplies", "Paper", "P2",
         "200", "250", "3", "5")
    ]

    columns = [
        "Order Id",
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
        "Discount Percent"
    ]

    # create dataframe
    df = spark.createDataFrame(data, columns)

    # apply transformation
    df_transformed = transform_orders(df)

    # ✅ assertions
    assert df_transformed.count() == 2
    assert "order_id" in df_transformed.columns
    assert "cost_price" in df_transformed.columns
    assert "list_price" in df_transformed.columns
    assert "quantity" in df_transformed.columns