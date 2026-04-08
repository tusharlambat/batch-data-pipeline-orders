from pyspark.sql import SparkSession
from src.utils.config import APP_NAME, HADOOP_AWS_PACKAGE

def get_spark_session():

    # Set explicit, numeric S3A timeout values to avoid runtime parsing issues
    # from external defaults (e.g. invalid values like "60s").
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.jars.packages", HADOOP_AWS_PACKAGE)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "200000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
        .config("spark.hadoop.fs.s3a.connection.ttl", "300000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        # Avoid duration-string parsing issues like "24h" in some runtime combos.
        .config("spark.hadoop.fs.s3a.multipart.purge", "true")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .getOrCreate()
    )

    hadoop_conf = spark._jsc.hadoopConfiguration()

    # Use default AWS credential chain (env vars, instance profile, etc.)
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.timeout", "200000")
    hadoop_conf.set("fs.s3a.socket.timeout", "200000")
    hadoop_conf.set("fs.s3a.connection.request.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.idle.time", "60000")
    hadoop_conf.set("fs.s3a.connection.ttl", "300000")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60")
    hadoop_conf.set("fs.s3a.multipart.purge", "true")
    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400")

    return spark
