# transform_orders.py

import pandas as pd
from src.config import CONFIG


def transform(df):
    try:
        print("[TRANSFORM] Starting transformation...")

        # 1. Remove duplicates
        df = df.drop_duplicates()

        # 2. Handle missing values
        df = df.dropna()

        # 3. Convert order_date to datetime
        df["order_date"] = pd.to_datetime(df["order_date"])

        # 4. Add new column (example)
        df["order_year"] = df["order_date"].dt.year

        print("[TRANSFORM] Transformation complete")
        print(df.head())

        return df

    except Exception as e:
        print(f"[TRANSFORM] Error: {e}")
        raise