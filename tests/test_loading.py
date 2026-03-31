from src.extraction.extract_orders import extract_orders
from src.transformation.transform_orders import transform_orders
from src.loading.load_to_s3 import load_to_s3

def test_load_to_s3():
    df = extract_orders()
    df_transformed = transform_orders(df)

    # Just check it runs without error
    load_to_s3(df_transformed)

    assert True