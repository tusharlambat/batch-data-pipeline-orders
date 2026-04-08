def test_extract_orders(spark):
    data = [("1", "100")]
    columns = ["order_id", "amount"]

    df = spark.createDataFrame(data, columns)

    assert df.count() == 1
