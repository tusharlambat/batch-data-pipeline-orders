from src.extraction.extract_orders import extract
from src.transformation.transform_orders import transform

if __name__ == "__main__":
    df = extract()
    df_transformed = transform(df)

    print("Final rows:", len(df_transformed))