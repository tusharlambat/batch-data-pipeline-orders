from src.extraction.extract_orders import extract_orders

def test_extract_orders():
    df = extract_orders()

    # Check dataframe is not empty
    assert df.count() > 0

    # Check columns exist
    assert len(df.columns) > 0