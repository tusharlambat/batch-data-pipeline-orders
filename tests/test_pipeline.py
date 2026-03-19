import pandas as pd
from src.transformation.transform_orders import transform_orders


def test_duplicates():

    data = {"A": [1,1,2]}

    df = pd.DataFrame(data)

    df_clean = transform_orders(df)

    assert len(df_clean) == 2