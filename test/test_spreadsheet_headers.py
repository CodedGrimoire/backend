import pandas as pd

from app.services.spreadsheets.dynamic_tables import clean_spreadsheet, normalize_columns


def test_messy_headers_sanitized():
    df = pd.DataFrame(
        [[1, 2, 3, 4, 5]],
        columns=[650, False, "", "Total Sales", "Total Sales"],
    )

    df_clean = clean_spreadsheet(df)
    df_norm, _ = normalize_columns(df_clean)

    assert list(df_norm.columns) == ["col_650", "col_false", "column_2", "total_sales", "total_sales_2"]
