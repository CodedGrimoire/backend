import pandas as pd
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
    for col in df.columns:
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


async def create_table_from_df(session: AsyncSession, dataset_id: str, df: pd.DataFrame, table_name: str | None = None) -> str:
    df_norm, mapping = normalize_columns(df)
    columns = infer_types(df_norm)
    table_name = table_name or await generate_unique_table_name(session, dataset_id)
    cols_sql = ", ".join([f'"{c}" {t}' for c, t in columns.items()])
    create_sql = f'CREATE TABLE "{table_name}" (id BIGSERIAL PRIMARY KEY, {cols_sql});'
    await session.execute(text(create_sql))
    await bulk_insert(session, table_name, df_norm)
    # restore original column names on the df for upstream mapping use
    df.columns = list(mapping.keys())
    return table_name


async def bulk_insert(session: AsyncSession, table: str, df: pd.DataFrame, chunk_size: int = 1000) -> int:
    if df.empty:
        return 0
    col_names = [sanitize_identifier(c) for c in df.columns]
    quoted_cols = [f'"{c}"' for c in col_names]
    param_keys = [f"c{i}" for i in range(len(col_names))]
    placeholders = ", ".join([f":{k}" for k in param_keys])
    stmt = text(f'INSERT INTO "{table}" ({", ".join(quoted_cols)}) VALUES ({placeholders})')
    total = 0
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    for start in range(0, len(rows), chunk_size):
        batch = rows[start : start + chunk_size]
        params = [
            {f"c{i}": value for i, value in enumerate(row)}
            for row in batch
        ]
        await session.execute(stmt, params)
        total += len(batch)
    return total
