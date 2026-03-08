import io
import pandas as pd


def read_spreadsheet(file_bytes: bytes, filename: str, sheet_name: str | None = None) -> pd.DataFrame:
    """Load CSV or XLSX into DataFrame."""
    buf = io.BytesIO(file_bytes)
    if filename.lower().endswith(".csv"):
        return pd.read_csv(buf)
    return pd.read_excel(buf, sheet_name=sheet_name or 0)
