from src.utils.logger import logger


def load_to_s3(df, path):

    logger.info("Loading data to S3")

    df.write.mode("overwrite").csv(path)

    logger.info("Data successfully written to S3")