import datetime
import logging
import numpy as np
import pandas as pd
import re

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils.identifiers import sanitize_identifier, short_hash


def infer_types(df: pd.DataFrame) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for col, dtype in df.dtypes.items():
        safe_col = sanitize_identifier(col)
        if pd.api.types.is_integer_dtype(dtype):
            mapping[safe_col] = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            mapping[safe_col] = "DOUBLE PRECISION"
        elif pd.api.types.is_bool_dtype(dtype):
            mapping[safe_col] = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            mapping[safe_col] = "TIMESTAMP"
        else:
            mapping[safe_col] = "TEXT"
    return mapping


async def _table_exists(session: AsyncSession, name: str) -> bool:
    res = await session.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = :name)"), {"name": name}
    )
    return res.scalar()


def build_table_name(dataset_id: str, suffix: int | None = None) -> str:
    base = f"data_{sanitize_identifier(dataset_id)}_{short_hash(dataset_id)}"
    return f"{base}_{suffix}" if suffix is not None else base


async def generate_unique_table_name(session: AsyncSession, dataset_id: str) -> str:
    candidate = build_table_name(dataset_id)
    idx = 1
    while await _table_exists(session, candidate):
        candidate = build_table_name(dataset_id, idx)
        idx += 1
    return candidate


def normalize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    mapping: dict[str, str] = {}
    used: set[str] = set()
    renamed = {}
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    original_cols = list(df.columns)
    for col in original_cols:
        base = sanitize_identifier(col)
        if not base:
            base = "col"
        candidate = base[:63]
        idx = 1
        while candidate in used:
            candidate = f"{base[:55]}_{idx}"
            idx += 1
        used.add(candidate)
        mapping[col] = candidate
        renamed[col] = candidate
    return df.rename(columns=renamed), mapping


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a DataFrame for asyncpg by ensuring only native Python types are present.
    - NaN/NaT -> None
    - datetimes -> Python datetime
    - objects -> strings (unless already basic scalar)
    """
    if df.empty:
        return df
    df_norm = df.copy()
    for col in df_norm.columns:
        series = df_norm[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            series = pd.to_datetime(series, errors="coerce")
            # keep object dtype so None stays None, not NaT
            series = series.astype(object).where(pd.notnull(series), None)
            series = series.apply(
                lambda v: v.to_pydatetime()
                if isinstance(v, pd.Timestamp)
                else (v if isinstance(v, datetime.datetime) else None)
            )
        else:
            series = series.astype(object).where(pd.notnull(series), None)
            # Convert generic objects to string, keep basic scalars untouched
            series = series.apply(
                lambda v: v
                if v is None or isinstance(v, (str, int, float, bool, datetime.datetime))
                else str(v)
            )
        df_norm[col] = series
    return df_norm


def normalize_dataframe_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final sanitation step before sending values to asyncpg:
    - force datetime columns through to_datetime (errors coerced to NaT)
    - cast to object dtype so None is preserved
    - replace NaN/NaT with None
    """
    if df.empty:
        return df
    df_norm = df.copy()
    for col in df_norm.columns:
        if pd.api.types.is_datetime64_any_dtype(df_norm[col]):
            df_norm[col] = pd.to_datetime(df_norm[col], errors="coerce")
    df_norm = df_norm.astype(object)
    df_norm = df_norm.replace({np.nan: None, pd.NaT: None})
    df_norm = df_norm.where(pd.notnull(df_norm), None)
    return df_norm


def normalize_dataframe_to_sql_types(df: pd.DataFrame, column_types: dict[str, str]) -> pd.DataFrame:
    """
    Align dataframe values with inferred SQL column types.
    - TEXT columns: cast any non-null value to string.
    Other column types are left as-is because they already map to native scalars.
    """
    if df.empty:
        return df
    df_norm = df.copy()
    for col in df_norm.columns:
        if column_types.get(col) == "TEXT":
            df_norm[col] = df_norm[col].apply(lambda v: None if v is None else str(v))
    return df_norm


def detect_and_fix_header(df: pd.DataFrame) -> pd.DataFrame:
    """
    Heuristic: if current headers look corrupted (unnamed/numeric/empty),
    assume first row is the real header and promote it.
    """
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    bad_count = 0
    for c in cols:
        if pd.isna(c):
            bad_count += 1
            continue
        s = str(c).strip()
        if s == "" or s.lower().startswith("unnamed") or s.isdigit() or isinstance(c, (bool, int, float)) or len(s) <= 1:
            bad_count += 1
    if bad_count / max(len(cols), 1) > 0.5:
        new_header = [str(c) for c in df.iloc[0]]
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = new_header
        logging.getLogger(__name__).info("Spreadsheet header corrected: first row promoted to header.")
    else:
        df.columns = [str(c) for c in df.columns]
    return df


def clean_spreadsheet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Robust preprocessing for messy Excel inputs:
    - stringifies headers, drops 'unnamed' placeholder columns
    - normalizes header characters, dedupes with suffixes
    - replaces NaN/NaT with None and casts to object
    - coerces mostly-numeric columns to numbers, mostly-bool to bool, otherwise to str
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    df = df.loc[:, ~df.columns.str.lower().str.startswith("unnamed")]

    def norm_name(name: str, idx: int) -> str:
        raw = str(name).strip().lower()
        if raw in {"", "nan", "none"}:
            raw = f"column_{idx}"
        raw = re.sub(r"\s+", "_", raw)
        raw = re.sub(r"[^a-z0-9_]+", "_", raw)
        raw = re.sub(r"_+", "_", raw).strip("_")
        if raw == "":
            raw = f"column_{idx}"
        if raw.isdigit() or raw in {"true", "false", "null"}:
            raw = f"col_{raw}"
        if not re.match(r"^[a-z]", raw):
            raw = f"col_{raw}"
        return raw

    seen: dict[str, int] = {}
    new_cols: list[str] = []
    for idx, col in enumerate(df.columns):
        base = norm_name(col, idx)
        count = seen.get(base, 0)
        name = base if count == 0 else f"{base}_{count+1}"
        seen[base] = count + 1
        new_cols.append(name)
    df.columns = new_cols

    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        df = df.drop(columns=empty_cols)
        logging.getLogger(__name__).info("Dropped empty columns: %s", empty_cols)

    df = df.astype(object)
    df = df.replace({np.nan: None, pd.NaT: None})

    def is_number(v) -> bool:
        return isinstance(v, (int, float, np.number)) and not isinstance(v, bool)

    def try_parse_bool(v):
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            lv = v.strip().lower()
            if lv in {"true", "1", "yes", "y"}:
                return True
            if lv in {"false", "0", "no", "n"}:
                return False
        return None

    for col in df.columns:
        series = df[col]
        non_null = [v for v in series if v is not None]
        if not non_null:
            df[col] = series
            continue
        num_ratio = sum(is_number(v) for v in non_null) / len(non_null)
        bool_candidates = [v for v in non_null if try_parse_bool(v) is not None]
        bool_ratio = len(bool_candidates) / len(non_null)

        if num_ratio >= 0.8:
            converted = pd.to_numeric(series, errors="coerce")
            converted = converted.astype(object).replace({np.nan: None})
            df[col] = converted
        elif bool_ratio >= 0.8:
            df[col] = series.apply(lambda v: try_parse_bool(v) if v is not None else None)
        else:
            df[col] = series.apply(lambda v: None if v is None else str(v))

    return df


async def create_table_from_df(session: AsyncSession, dataset_id: str, df: pd.DataFrame, table_name: str | None = None) -> str:
    df_fixed_header = detect_and_fix_header(df)
    df_clean = clean_spreadsheet(df_fixed_header)
    df_norm, mapping = normalize_columns(df_clean)
    if len(df_norm.columns) != len(mapping):
        logging.getLogger(__name__).warning(
            "Column mapping length mismatch: normalized=%s mapping=%s", len(df_norm.columns), len(mapping)
        )
    columns = infer_types(df_norm)
    table_name = table_name or await generate_unique_table_name(session, dataset_id)
    # avoid duplicate "id" when source data already has an id column
    pk_name = "id" if "id" not in columns else "pk_id"
    cols_sql = ", ".join([f'"{c}" {t}' for c, t in columns.items()])
    create_sql = f'CREATE TABLE "{table_name}" ({pk_name} BIGSERIAL PRIMARY KEY, {cols_sql});'
    await session.execute(text(create_sql))
    df_for_insert = normalize_dataframe(df_norm)
    df_for_insert = normalize_dataframe_for_sql(df_for_insert)
    df_for_insert = normalize_dataframe_to_sql_types(df_for_insert, columns)
    await bulk_insert(session, table_name, df_for_insert)
    return table_name


async def bulk_insert(session: AsyncSession, table: str, df: pd.DataFrame, chunk_size: int = 1000) -> int:
    if df.empty:
        return 0
    # Ensure any caller-provided df is safe for asyncpg
    df = normalize_dataframe_for_sql(df)
    col_names = [sanitize_identifier(c) for c in df.columns]
    quoted_cols = [f'"{c}"' for c in col_names]
    param_keys = [f"c{i}" for i in range(len(col_names))]
    placeholders = ", ".join([f":{k}" for k in param_keys])
    stmt = text(f'INSERT INTO "{table}" ({", ".join(quoted_cols)}) VALUES ({placeholders})')
    total = 0
    records = df.to_dict(orient="records")
    rows = [
        tuple(rec[col] for col in df.columns)
        for rec in records
    ]
    for start in range(0, len(rows), chunk_size):
        batch = rows[start : start + chunk_size]
        params = [
            {f"c{i}": value for i, value in enumerate(row)}
            for row in batch
        ]
        await session.execute(stmt, params)
        total += len(batch)
    return total
