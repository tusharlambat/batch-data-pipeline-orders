from src.extraction.extract_orders import extract_orders
from src.transformation.transform_orders import transform_orders

def test_transform_orders():
    df = extract_orders()
    df_transformed = transform_orders(df)

    # Check column rename
    assert "order_id" in df_transformed.columns

    # Check new column created
    assert "final_price" in df_transformed.columns