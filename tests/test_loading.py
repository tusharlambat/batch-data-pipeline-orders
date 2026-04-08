def test_load_to_s3(spark):
    data = [("1", "100")]
    columns = ["order_id", "amount"]

    df = spark.createDataFrame(data, columns)

    assert df.count() > 0
