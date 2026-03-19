# extract_orders.py

import pandas as pd
from src.config import CONFIG


def extract():
    try:
        # Get environment (dev/prod)
        env = CONFIG["env"]

        # Get S3 raw data path
        raw_path = CONFIG["paths"][env]["raw_data"]

        print(f"[EXTRACT] Reading data from: {raw_path}")

        # Read CSV from S3
        df = pd.read_csv(raw_path)

        print("[EXTRACT] Data read successfully")
        print(df.head())

        return df

    except Exception as e:
        print(f"[EXTRACT] Error: {e}")
        raise