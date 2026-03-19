from pyspark.sql import SparkSession

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Orders Pipeline") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026"
        ) \
        .getOrCreate()

    # AWS Credentials
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAR67SV44FR6JMRJJY")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "AKIAR67SV44FR6JMRJJY")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

    return spark
