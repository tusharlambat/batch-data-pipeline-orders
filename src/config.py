CONFIG = {
    # 🌍 Environment
    "env": "dev",

    # 📂 Environment-based paths
    "paths": {
        "dev": {
            "raw_data": "s3://batchpipelineproject1/dev/raw_data/orders.csv",
            "clean_data": "s3://batchpipelineproject1/dev/cleaned_data/"
        },
        "prod": {
            "raw_data": "s3://batchpipelineproject1/prod/raw_data/orders.csv",
            "clean_data": "s3://batchpipelineproject1/prod/cleaned_data/"
        }
    },

    # 📦 File format
    "file_format": "csv",

    # 🧩 Partitioning (for next step)
    "partition_column": "order_date",

    # 🔐 AWS Credentials (learning purpose)
    "aws": {
        "access_key": "AKIAR67SV44FR6JMRJJY",
        "secret_key": "AKIAR67SV44FR6JMRJJY",
        "endpoint": "s3.amazonaws.com"
    }
}