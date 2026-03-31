from pyspark.sql import SparkSession
from src.utils.config import APP_NAME, HADOOP_AWS_PACKAGE

def get_spark_session():
    """
    Creates and returns a Spark session configured for S3 access
    """

    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.jars.packages", HADOOP_AWS_PACKAGE) \
        .getOrCreate()

    return spark