from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def test_extract_orders():
    data = [("1", "100")]
    columns = ["order_id", "amount"]

    df = spark.createDataFrame(data, columns)

    assert df.count() == 1