import io
import pandas as pd


def read_spreadsheet(file_bytes: bytes, filename: str, sheet_name: str | None = None) -> pd.DataFrame:
    """Load CSV or a specific sheet from XLSX into DataFrame."""
    buf = io.BytesIO(file_bytes)
    if filename.lower().endswith(".csv"):
        return pd.read_csv(buf)
    return pd.read_excel(buf, sheet_name=sheet_name or 0)


def read_all_sheets(file_bytes: bytes, filename: str) -> dict[str, pd.DataFrame]:
    """Load all sheets from an Excel file; for CSV return single pseudo-sheet."""
    buf = io.BytesIO(file_bytes)
    if filename.lower().endswith(".csv"):
        return {"Sheet1": pd.read_csv(buf)}
    xls = pd.ExcelFile(buf)
    return {name: xls.parse(name) for name in xls.sheet_names}
