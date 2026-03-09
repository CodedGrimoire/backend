import hashlib
import logging
import os
import uuid
from typing import Any, Dict
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, Query
from sqlalchemy import select, text, delete
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
import sqlglot

from app.api.deps import assert_dataset_owner, get_current_user
from app.core.db.session import get_session
from app.models.dataset import Dataset, DatasetColumn, DatasetMetric, DatasetChart, DatasetInsight
from app.models.user import User
from app.schemas.auth import CurrentUser
from app.schemas.datasets import (
    DashboardResponse,
    DashboardStatusResponse,
    DatasetDetail,
    DatasetOut,
    EditCellRequest,
    EditCellResponse,
    QueryRequest,
    QueryResponse,
    UploadResponse,
    ActionsRequest,
    ActionsResponse,
)
from app.services.audit.service import log_action
from app.services.spreadsheets import parser, dynamic_tables
from app.services.sql.safety import (
    is_safe_sql,
    ensure_limit,
    is_safe_expression_sql,
    extract_columns,
    normalize_column,
)
from app.services.dashboards.service import rebuild_dashboard_stub, fetch_dashboard
from app.services.datasets.service import get_schema_metadata
from app.services.datasets.schema_context import (
    build_schema_context,
    build_full_schema_context,
    build_single_table_context,
    build_multi_table_context,
)
from app.services.datasets.profiling import profile_dataset
from app.services.datasets.optimizer import analyze_and_optimize
from app.services.llm.client import llm_client, generate_sql, generate_answer, repair_sql, generate_suggestions
from sqlglot import exp

router = APIRouter()
logger = logging.getLogger(__name__)
_query_cache: Dict[str, Dict[str, Any]] = {}
_suggestion_cache: Dict[str, list[str]] = {}
_metadata_cache: Dict[str, Dict[str, Any]] = {}


def _unique_dataset_name(base_name: str, existing: set[str]) -> str:
    """Return a dataset name that does not collide for the user.

    Keeps the original extension and appends ``(n)`` like macOS if needed.
    """
    if base_name not in existing:
        return base_name

    stem, ext = os.path.splitext(base_name)
    counter = 1
    while True:
        candidate = f"{stem} ({counter}){ext}"
        if candidate not in existing:
            return candidate
        counter += 1


def _jaccard_similarity(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    return len(a & b) / max(1, len(a | b))


def _profile_df_columns(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        stats[col] = {
            "distinct_count": int(non_null.nunique()),
            "null_count": int(series.isna().sum()),
            "sample_values": [v if pd.notna(v) else None for v in non_null.head(5).tolist()],
        }
    return stats


def _merge_similar_sheets(sheets: dict[str, pd.DataFrame]) -> list[tuple[str, pd.DataFrame]]:
    merged: list[tuple[str, pd.DataFrame]] = []
    for name, df in sheets.items():
        cols = {c.lower() for c in df.columns}
        placed = False
        for idx, (mname, mdf) in enumerate(merged):
            sim = _jaccard_similarity(cols, {c.lower() for c in mdf.columns})
            if sim >= 0.9:
                # align columns union
                all_cols = list({*mdf.columns, *df.columns})
                mdf = mdf.reindex(columns=all_cols)
                df_aligned = df.reindex(columns=all_cols)
                merged[idx] = (f"{mname}+{name}", pd.concat([mdf, df_aligned], ignore_index=True))
                placed = True
                break
        if not placed:
            merged.append((name, df))
    return merged


def _detect_relationships(table_dfs: dict[str, pd.DataFrame]) -> list[dict[str, str]]:
    """Detect simple relationships between tables based on shared columns and value subsets."""
    edges: list[dict[str, str]] = []
    tables = list(table_dfs.items())
    for i in range(len(tables)):
        t1, df1 = tables[i]
        for j in range(i + 1, len(tables)):
            t2, df2 = tables[j]
            common_cols = set(df1.columns) & set(df2.columns)
            for col in common_cols:
                vals1 = set(df1[col].dropna().astype(str).unique()[:500])
                vals2 = set(df2[col].dropna().astype(str).unique()[:500])
                if not vals1 or not vals2:
                    continue
                if vals1.issubset(vals2):
                    edges.append({"from": t1, "to": t2, "column": col, "type": "many-to-one"})
                elif vals2.issubset(vals1):
                    edges.append({"from": t2, "to": t1, "column": col, "type": "many-to-one"})
    return edges


async def _dataset_tables(session: AsyncSession, dataset_id: str) -> list[str]:
    prefix = dynamic_tables.build_table_name(dataset_id)
    res = await session.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name LIKE :pat "
            "ORDER BY table_name"
        ),
        {"pat": f"{prefix}%"},
    )
    return [r[0] for r in res.fetchall()]


async def _table_columns(session: AsyncSession, tables: list[str]) -> dict[str, list[dict[str, str]]]:
    if not tables:
        return {}
    res = await session.execute(
        text(
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = ANY(:tables)"
        ),
        {"tables": tables},
    )
    mapping: dict[str, list[dict[str, str]]] = {}
    for t, col, dtype in res.fetchall():
        mapping.setdefault(t, []).append({"name": col, "type": dtype})
    return mapping


def _normalize_question(question: str) -> str:
    return " ".join(question.strip().lower().split())


def _classify_result(sql: str, rows: list[dict[str, Any]], columns: list[dict[str, str]]) -> str:
    sql_lower = sql.lower()
    row_count = len(rows)
    col_count = len(columns)
    has_group = "group by" in sql_lower
    has_order = "order by" in sql_lower
    if row_count == 1 and col_count == 1:
        return "scalar"
    if has_group:
        return "aggregation"
    if has_order:
        return "ranking"
    return "table"


def _projection_aliases(sql: str) -> set[str]:
    """Return lower-cased aliases defined in SELECT list (used to allow ORDER BY alias)."""
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return set()
    aliases: set[str] = set()
    for expr in parsed.expressions or []:
        if isinstance(expr, sqlglot.exp.Alias):
            aliases.add(expr.alias.lower())
    return aliases


def _make_string_filters_case_insensitive(sql: str, col_types: dict[str, str]) -> str:
    """Rewrite string equality filters to ILIKE with wildcards for flexibility."""
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql

    text_like = {"character varying", "text", "varchar", "char"}

    def is_text_col(col_name: str) -> bool:
        t = col_types.get(col_name.lower(), "")
        return any(k in t for k in text_like)

    changed = False

    def transform(node: exp.Expression):
        nonlocal changed
        if isinstance(node, exp.EQ):
            left, right = node.left, node.right
            # Column = 'literal'
            if isinstance(left, exp.Column) and isinstance(right, exp.Literal) and right.is_string:
                col_name = left.name.lower()
                if is_text_col(col_name):
                    val = right.this
                    new = exp.ILike(this=left.copy(), expression=exp.Literal.string(f"%{val}%"))
                    changed = True
                    return new
            # 'literal' = Column
            if isinstance(right, exp.Column) and isinstance(left, exp.Literal) and left.is_string:
                col_name = right.name.lower()
                if is_text_col(col_name):
                    val = left.this
                    new = exp.ILike(this=right.copy(), expression=exp.Literal.string(f"%{val}%"))
                    changed = True
                    return new
        return node

    transformed = parsed.transform(transform)
    return transformed.sql(dialect="postgres") if changed else sql


def _extract_tables(sql: str) -> set[str]:
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return set()
    tables = set()
    for tbl in parsed.find_all(exp.Table):
        if tbl.name:
            tables.add(tbl.name.lower())
    return tables


async def _get_metadata(session: AsyncSession, dataset_id: str, table_name: str) -> Dict[str, Any]:
    if dataset_id in _metadata_cache:
        return _metadata_cache[dataset_id]
    tables = await _dataset_tables(session, dataset_id)
    if not tables:
        tables = [table_name]
    meta = {"tables": tables, "relationships": [], "metrics": [], "dimensions": [], "time_columns": [], "synonyms": {}}
    try:
        # best-effort: detect relationships live
        table_cols = await _table_columns(session, tables)
        # collect small samples to infer relationships
        dfs = {}
        for t in tables:
            cols = [c["name"] for c in table_cols.get(t, [])]
            if not cols:
                continue
            quoted_cols = ", ".join([f'"{c}"' for c in cols])
            sample_stmt = text(f'SELECT {quoted_cols} FROM "{t}" LIMIT 500')
            res = await session.execute(sample_stmt)
            dfs[t] = pd.DataFrame(res.fetchall(), columns=cols)
        if dfs:
            meta["relationships"] = _detect_relationships(dfs)
        # gather semantic roles per table
        metrics = set()
        dimensions = set()
        time_cols = set()
        synonyms = {}
        for tbl in tables:
            tbl_columns = []
            for t, cols in table_cols.items():
                if t == tbl:
                    for c in cols:
                        tbl_columns.append(DatasetColumn(name=c["name"], db_type=c["type"], sample_values={"table": t}))
            meta_tbl = await analyze_and_optimize(session, uuid.UUID(dataset_id), tbl, columns=tbl_columns)
            metrics.update(meta_tbl.get("metrics", []))
            dimensions.update(meta_tbl.get("dimensions", []))
            time_cols.update(meta_tbl.get("time_columns", []))
            synonyms.update(meta_tbl.get("synonyms", {}))
        meta["metrics"] = list(metrics)
        meta["dimensions"] = list(dimensions)
        meta["time_columns"] = list(time_cols)
        meta["synonyms"] = synonyms
    except Exception:
        pass
    _metadata_cache[dataset_id] = meta
    return meta


def _sql_from_intent(intent: dict, table_name: str, allowed_cols: set[str]) -> str:
    metric = intent.get("metric")
    agg = intent.get("aggregation", "SUM").upper()
    group_by = intent.get("group_by")
    filters = intent.get("filters", []) or []
    order_by = intent.get("order_by")
    limit = intent.get("limit", 100)

    select_parts = []
    if metric:
        select_parts.append(f"{agg}({metric}) AS {metric if agg != 'COUNT' else 'count'}")
    else:
        select_parts.append("*")

    if group_by:
        select_parts.insert(0, group_by)

    where_clauses = []
    for f in filters:
        col = f.get("column")
        op = f.get("operator", "=")
        val = f.get("value")
        if not col or col not in allowed_cols:
            continue
        if isinstance(val, str):
            where_clauses.append(f'LOWER("{col}") {op} LOWER(:{col})')
        else:
            where_clauses.append(f'"{col}" {op} :{col}')
    params = {f.get("column"): f.get("value") for f in filters if f.get("column") in allowed_cols}

    sql = f'SELECT {", ".join(select_parts)} FROM "{table_name}"'
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    if group_by:
        sql += f' GROUP BY "{group_by}"'
    if order_by and metric:
        direction = "DESC" if order_by.upper() == "DESC" else "ASC"
        sql += f' ORDER BY "{metric if agg != "COUNT" else "count"}" {direction}'
    sql += f" LIMIT {limit}"
    return sql, params


def _validate_sql_against_schema(
    sql: str,
    allowed_tables: set[str],
    table_columns: dict[str, list[dict[str, str]]],
    allowed_pairs: set[frozenset[str]] | None = None,
) -> str | None:
    """Return None if valid, else an error string describing the first violation."""
    tables_set = _extract_tables(sql)
    if not tables_set:
        return "Could not parse SQL tables"
    tables = list(tables_set)
    if any(t not in allowed_tables for t in tables):
        return f"Invalid table referenced: {tables}"

    sql_lower = sql.lower()
    # if joins present ensure they are allowed
    if len(tables) > 1 or " join " in sql_lower:
        if not allowed_pairs:
            return "Joins are not allowed"
        for i in range(len(tables)):
            for j in range(i + 1, len(tables)):
                if frozenset({tables[i], tables[j]}) not in allowed_pairs:
                    return f"Join between {tables[i]} and {tables[j]} is not allowed"

    # block unsafe ops
    forbidden = [" drop ", " delete ", " truncate ", " alter ", " insert ", " update "]
    if any(f in sql_lower for f in forbidden):
        return "Unsafe SQL operation detected"

    # build table->set columns map
    table_cols_map: dict[str, set[str]] = {t.lower(): {c["name"].lower() for c in cols} for t, cols in table_columns.items()}
    select_aliases = _projection_aliases(sql)
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return "SQL parse error"
    for col in parsed.find_all(exp.Column):
        name = (col.name or "").lower()
        if name in ("", "*"):
            continue
        if name in select_aliases:
            continue
        tbl = col.table.lower() if col.table else None
        if tbl:
            if tbl not in allowed_tables:
                return f"Invalid table referenced: {tbl}"
            if name not in table_cols_map.get(tbl, set()):
                return f"Invalid column referenced: {tbl}.{name}"
        else:
            # unqualified column must be unique across all tables in query
            matches = [t for t in tables if name in table_cols_map.get(t, set())]
            if not matches:
                return f"Invalid column referenced: {name}"
            if len(tables) > 1 and len(matches) > 1:
                return f"Ambiguous column '{name}', qualify with table name"
    return None


async def _schema_fingerprint(dataset_id: uuid.UUID, columns: list[DatasetColumn]) -> tuple[str, set[str]]:
    """Return dataset-specific schema hash and allowed column set."""
    allowed_cols = {c.name.lower() for c in columns}
    fingerprint_str = "|".join(f"{dataset_id}:{c.name}:{c.db_type}" for c in columns)
    schema_hash = hashlib.sha1(fingerprint_str.encode()).hexdigest() if fingerprint_str else ""
    return schema_hash, allowed_cols


def _build_column_metadata(result) -> list[dict[str, str]]:
    cursor = getattr(result, "cursor", None)
    if cursor and getattr(cursor, "description", None):
        cols = []
        for col in cursor.description:
            name = getattr(col, "name", None) or (col[0] if isinstance(col, tuple) else None)
            type_code = getattr(col, "type_code", None)
            if type_code is None and isinstance(col, tuple) and len(col) > 1:
                type_code = col[1]
            cols.append({"name": str(name), "type": str(type_code) if type_code is not None else "UNKNOWN"})
        return cols
    # fallback to SQLAlchemy keys
    return [{"name": key, "type": "UNKNOWN"} for key in result.keys()]


@router.post("/upload", response_model=UploadResponse)
async def upload_dataset(
    file: UploadFile = File(...),
    sheet_name: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    content = await file.read()
    sheets = parser.read_all_sheets(content, file.filename) if sheet_name is None else {sheet_name: parser.read_spreadsheet(content, file.filename, sheet_name=sheet_name)}
    sheets = {k: v for k, v in sheets.items() if not v.empty}
    if not sheets:
        raise HTTPException(status_code=400, detail="File is empty")
    merged_sheets = _merge_similar_sheets(sheets)

    total_rows = sum(len(df) for _, df in merged_sheets)

    # attach owner and pick a unique name for this user (avoid IntegrityError on duplicate filename)
    user_stmt = select(User).where(User.firebase_uid == current_user.firebase_uid)
    res = await session.execute(user_stmt)
    user = res.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    existing_names_res = await session.execute(select(Dataset.name).where(Dataset.owner_id == user.id))
    existing_names = set(existing_names_res.scalars().all())
    dataset_name = _unique_dataset_name(file.filename, existing_names)

    dataset = Dataset(owner_id=user.id, name=dataset_name, table_name="pending", row_count=total_rows, status="processing")

    session.add(dataset)
    await session.flush()

    table_dfs: dict[str, pd.DataFrame] = {}
    columns_records: list[DatasetColumn] = []

    for idx, (sheet, df) in enumerate(merged_sheets):
        # normalize + create table + insert
        table_name = dynamic_tables.build_table_name(str(dataset.id), idx if idx > 0 else None)
        await dynamic_tables.create_table_from_df(session, str(dataset.id), df, table_name=table_name)
        if idx == 0:
            dataset.table_name = table_name
        # profile columns
        df_norm, mapping = dynamic_tables.normalize_columns(df)
        inferred_types = dynamic_tables.infer_types(df_norm)
        col_profiles = _profile_df_columns(df_norm)
        for order, orig in enumerate(df.columns):
            db_col = mapping[orig]
            db_type = inferred_types[db_col]
            profile = col_profiles.get(db_col) or col_profiles.get(orig) or {}
            profile["table"] = table_name
            columns_records.append(
                DatasetColumn(
                    dataset_id=dataset.id,
                    original_name=orig,
                    name=db_col,
                    db_type=db_type,
                    order=order,
                    is_nullable=True,
                    sample_values=profile,
                )
            )
        table_dfs[table_name] = df_norm

    dataset.status = "ready"

    session.add_all(columns_records)
    await session.flush()

    # Profile first table for backwards compatibility
    await profile_dataset(session, dataset.id, dataset.table_name)
    relationships = _detect_relationships(table_dfs)
    meta = {
        "tables": list(table_dfs.keys()),
        "relationships": relationships,
        "metrics": [],
        "dimensions": [],
        "time_columns": [],
        "synonyms": {},
    }
    _metadata_cache[str(dataset.id)] = meta

    await session.commit()

    await log_action(session, dataset.id, current_user.id, "upload", {"rows": dataset.row_count})
    return UploadResponse(dataset_id=str(dataset.id), rows=dataset.row_count)


@router.post("/dev-seed", response_model=UploadResponse)
async def dev_seed_dataset(
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    # if already exists for this user, return it
    existing_q = select(Dataset).where(
        Dataset.owner_id == select(User.id).where(User.firebase_uid == current_user.firebase_uid).scalar_subquery(),
        Dataset.name == "Dev Seed",
    )
    res_existing = await session.execute(existing_q)
    existing_ds = res_existing.scalar_one_or_none()
    if existing_ds:
        return UploadResponse(dataset_id=str(existing_ds.id), rows=existing_ds.row_count)

    data = [
        {"month": "Jan", "revenue": 10000, "expenses": 7000, "profit": 3000, "customers": 120},
        {"month": "Feb", "revenue": 12000, "expenses": 8000, "profit": 4000, "customers": 140},
        {"month": "Mar", "revenue": 9000, "expenses": 6000, "profit": 3000, "customers": 110},
        {"month": "Apr", "revenue": 15000, "expenses": 9000, "profit": 6000, "customers": 160},
        {"month": "May", "revenue": 18000, "expenses": 10000, "profit": 8000, "customers": 170},
        {"month": "Jun", "revenue": 16000, "expenses": 9500, "profit": 6500, "customers": 165},
        {"month": "Jul", "revenue": 20000, "expenses": 12000, "profit": 8000, "customers": 200},
        {"month": "Aug", "revenue": 21000, "expenses": 13000, "profit": 8000, "customers": 210},
        {"month": "Sep", "revenue": 19000, "expenses": 11000, "profit": 8000, "customers": 195},
        {"month": "Oct", "revenue": 22000, "expenses": 14000, "profit": 8000, "customers": 220},
        {"month": "Nov", "revenue": 25000, "expenses": 15000, "profit": 10000, "customers": 240},
        {"month": "Dec", "revenue": 30000, "expenses": 18000, "profit": 12000, "customers": 300},
    ]
    df = pd.DataFrame(data)

    dataset = Dataset(owner_id=None, name="Dev Seed", table_name="pending", row_count=len(df), status="processing")
    user_stmt = select(User).where(User.firebase_uid == current_user.firebase_uid)
    res = await session.execute(user_stmt)
    user = res.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    dataset.owner_id = user.id

    session.add(dataset)
    await session.flush()

    table_name = await dynamic_tables.create_table_from_df(session, str(dataset.id), df)
    dataset.table_name = table_name
    dataset.status = "ready"
    dataset.row_count = len(df)

    df_norm, mapping = dynamic_tables.normalize_columns(df)
    inferred_types = dynamic_tables.infer_types(df_norm)
    cols = []
    for idx, orig in enumerate(df.columns):
        db_col = mapping[orig]
        db_type = inferred_types[db_col]
        cols.append(
            DatasetColumn(
                dataset_id=dataset.id,
                original_name=orig,
                name=db_col,
                db_type=db_type,
                order=idx,
                is_nullable=True,
                sample_values=None,
            )
        )
    session.add_all(cols)
    await session.flush()

    await profile_dataset(session, dataset.id, table_name)
    meta = await analyze_and_optimize(session, dataset.id, table_name)
    _metadata_cache[str(dataset.id)] = meta
    await session.commit()

    return UploadResponse(dataset_id=str(dataset.id), rows=dataset.row_count)


@router.get("", response_model=list[DatasetOut])
async def list_datasets(
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    stmt = select(Dataset).join(User, Dataset.owner_id == User.id).where(
        User.firebase_uid == current_user.firebase_uid
    )
    result = await session.execute(stmt)
    datasets = result.scalars().all()
    return [DatasetOut(id=str(ds.id), name=ds.name, status=ds.status) for ds in datasets]


@router.delete("/{dataset_id}", status_code=204)
async def delete_dataset(
    dataset_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    tables = await _dataset_tables(session, dataset_id)
    for t in tables:
        await session.execute(text(f'DROP TABLE IF EXISTS "{t}" CASCADE'))
    # log before removing the dataset row to avoid FK issues; ignore if dataset vanished
    try:
        await log_action(session, ds.id, current_user.id, "delete", {"tables_dropped": tables})
    except Exception:
        pass
    await session.execute(delete(Dataset).where(Dataset.id == ds.id))
    _suggestion_cache.pop(dataset_id, None)
    _metadata_cache.pop(dataset_id, None)
    await session.commit()
    return


@router.get("/{dataset_id}", response_model=DatasetDetail)
async def get_dataset(
    dataset_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    return DatasetDetail(id=str(ds.id), name=ds.name, table=ds.table_name, status=ds.status, row_count=ds.row_count)


@router.get("/{dataset_id}/suggestions")
async def dataset_suggestions(
    dataset_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    if dataset_id in _suggestion_cache:
        return {"suggestions": _suggestion_cache[dataset_id]}
    tables = await _dataset_tables(session, dataset_id)
    if not tables:
        tables = [ds.table_name]
    schema_ctx = await build_multi_table_context(session, ds.id, tables)
    meta = await _get_metadata(session, dataset_id, ds.table_name)
    relationships = meta.get("relationships", [])
    if relationships:
        schema_ctx = schema_ctx + "\nRelationships:\n" + "\n".join(
            [f"- {r.get('from')}.{r.get('column')} -> {r.get('to')}.{r.get('column')}" for r in relationships]
        )
    _dataset_obj, columns_meta = await get_schema_metadata(session, dataset_id)
    col_types = {c.name.lower(): c.db_type for c in columns_meta}

    # derive roles
    def _is_numeric(t: str) -> bool:
        t = t.lower()
        return any(k in t for k in ["int", "numeric", "decimal", "double", "real", "float", "number"])

    def _is_time(t: str) -> bool:
        t = t.lower()
        return any(k in t for k in ["date", "time", "timestamp"])

    metrics = [c.original_name or c.name for c in columns_meta if _is_numeric(c.db_type)]
    dimensions = [c.original_name or c.name for c in columns_meta if not _is_numeric(c.db_type) and not _is_time(c.db_type)]
    time_cols = [c.original_name or c.name for c in columns_meta if _is_time(c.db_type)]

    # rule-based suggestions
    rule_suggestions: list[str] = []
    metric = metrics[0] if metrics else None
    dimension = dimensions[0] if dimensions else None
    if metric and dimension:
        rule_suggestions.append(f"What is the total {metric} by {dimension}?")
        rule_suggestions.append(f"Which {dimension} has the highest {metric}?")
    if metric and time_cols:
        rule_suggestions.append(f"How does {metric} change over time?")

    llm_suggestions = await generate_suggestions(schema_ctx, n=5)

    merged = rule_suggestions + llm_suggestions
    # dedup case-insensitive, preserve order preferring rule first (already first)
    seen = set()
    final: list[str] = []
    for q in merged:
        key = q.lower().strip()
        if key and key not in seen:
            seen.add(key)
            final.append(q)
        if len(final) >= 3:
            break

    _suggestion_cache[dataset_id] = final
    await log_action(session, ds.id, current_user.id, "suggestions", {"count": len(final)})
    return {"suggestions": final}


@router.get("/{dataset_id}/preview")
async def preview_dataset(
    dataset_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    table_name = ds.table_name
    stmt = text(f'SELECT * FROM "{table_name}" LIMIT 100')
    result = await session.execute(stmt)
    columns = _build_column_metadata(result)
    rows = [dict(r) for r in result.mappings().all()]
    return {"columns": columns, "rows": rows}


@router.get("/{dataset_id}/rows")
async def list_dataset_rows(
    dataset_id: str,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
    sort_by: str | None = Query(default=None),
    sort_order: str | None = Query(default="asc"),
    filter_column: str | None = Query(default=None),
    filter_value: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    table_name = ds.table_name
    offset = (page - 1) * limit

    # allowed columns to avoid injection
    col_meta_stmt = text(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = :table"
    )
    col_res = await session.execute(col_meta_stmt, {"table": table_name})
    col_rows = col_res.fetchall()
    allowed_cols = {r[0] for r in col_rows}
    col_types = {r[0]: r[1] for r in col_rows}

    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    where_clause = ""
    if filter_column and filter_value and filter_column in allowed_cols:
        col_type = col_types.get(filter_column, "")
        cast_prefix = ""
        # if numeric, cast to text for ILIKE
        if any(k in col_type.lower() for k in ["int", "numeric", "decimal", "double", "real", "float"]):
            cast_prefix = "CAST"
            where_clause = f' WHERE CAST("{filter_column}" AS TEXT) ILIKE :fval'
        else:
            where_clause = f' WHERE "{filter_column}" ILIKE :fval'
        params["fval"] = f"%{filter_value}%"

    order_clause = ""
    if sort_by and sort_by in allowed_cols:
        direction = "DESC" if (sort_order or "").lower() == "desc" else "ASC"
        order_clause = f' ORDER BY "{sort_by}" {direction}'

    count_stmt = text(f'SELECT COUNT(*) FROM "{table_name}"{where_clause}')
    count_res = await session.execute(count_stmt, params if where_clause else {})
    total_rows = count_res.scalar() or 0

    data_stmt = text(f'SELECT * FROM "{table_name}"{where_clause}{order_clause} LIMIT :limit OFFSET :offset')
    result = await session.execute(data_stmt, params)
    columns = _build_column_metadata(result)
    rows = [dict(r) for r in result.mappings().all()]

    return {
        "columns": columns,
        "rows": rows,
        "page": page,
        "limit": limit,
        "total_rows": total_rows,
    }


@router.post("/{dataset_id}/query", response_model=QueryResponse)
async def query_dataset(
    dataset_id: str,
    body: QueryRequest,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)

    schema_meta = await get_schema_metadata(session, dataset_id)
    if not schema_meta:
        raise HTTPException(status_code=404, detail="Dataset not found")
    _dataset_obj, columns_meta = schema_meta

    tables = await _dataset_tables(session, dataset_id)
    if not tables:
        tables = [ds.table_name]
    allowed_tables = {t.lower() for t in tables}
    table_columns = await _table_columns(session, tables)
    col_types = {c["name"].lower(): c["type"] for cols in table_columns.values() for c in cols}
    schema_hash, allowed_cols = await _schema_fingerprint(ds.id, columns_meta)
    allowed_cols.update({c["name"].lower() for cols in table_columns.values() for c in cols})

    cache_key = None
    if body.question:
        cache_key = f"{_normalize_question(body.question)}::{schema_hash}"
        cached = _query_cache.get(cache_key)
        if cached:
            await log_action(
                session,
                ds.id,
                current_user.id,
                "query",
                {"question": body.question, "sql": cached["sql"], "row_count": cached["row_count"], "cache": True},
            )
            return QueryResponse(**cached)

    # determine SQL
    intent_params: Dict[str, Any] = {}
    if not (body.sql or body.question):
        raise HTTPException(status_code=400, detail="SQL or question required")

    meta = await _get_metadata(session, dataset_id, ds.table_name)
    relationships = meta.get("relationships", [])
    tables = meta.get("tables") or tables
    schema_ctx_str = await build_multi_table_context(session, ds.id, tables)
    relationship_lines = []
    if relationships:
        relationship_lines.append("Relationships (joins allowed only on these):")
        for rel in relationships:
            relationship_lines.append(f"- {rel.get('from')}.{rel.get('column')} -> {rel.get('to')}.{rel.get('column')}")
    schema_ctx_str = "\n".join([schema_ctx_str, *relationship_lines])

    sql_text = body.sql or ""
    attempts = 0
    max_attempts = 2
    validation_error = None
    allowed_pairs = {frozenset({rel.get("from", "").lower(), rel.get("to", "").lower()}) for rel in relationships if rel.get("from") and rel.get("to")}

    while attempts < max_attempts:
        if not sql_text and body.question:
            extra_hint = ""
            if validation_error:
                extra_hint = (
                    f"\nPrevious SQL was invalid ({validation_error}). "
                    f"Use ONLY the tables {', '.join(sorted(allowed_tables))} with their columns. "
                    "Do NOT use other tables. Joins are only allowed when a relationship exists."
                )
            sql_text = await generate_sql(schema_context=schema_ctx_str, question=body.question + extra_hint)
        sql_text = sql_text.replace("`sql", "").replace("`", "").strip()
        validation_error = _validate_sql_against_schema(sql_text, allowed_tables, table_columns, allowed_pairs)
        if not validation_error:
            break
        attempts += 1
        sql_text = ""

    if validation_error:
        raise HTTPException(status_code=400, detail=f"Invalid SQL: {validation_error}")

    if not is_safe_sql(sql_text):
        print(f"Rejected unsafe SQL: {sql_text}")
        raise HTTPException(status_code=400, detail=f"Unsafe SQL: {sql_text}")

    # validate columns against live schema
    cols_in_sql = extract_columns(sql_text)
    select_aliases = _projection_aliases(sql_text)
    tables_in_sql = _extract_tables(sql_text)
    # reject tables not in schema
    for t in tables_in_sql:
        if t not in allowed_tables:
            raise HTTPException(status_code=400, detail=f"Invalid table referenced: {t} | SQL: {sql_text}")

    for col in cols_in_sql:
        normalized = normalize_column(col)
        if normalized == "*" or not normalized:
            continue
        if normalized in select_aliases:
            continue  # allow ORDER BY aliases, etc.
        if normalized not in allowed_cols:
            raise HTTPException(status_code=400, detail=f"Invalid column referenced: {normalized} | SQL: {sql_text}")
    print(f"LLM question: {body.question}")
    print(f"Generated SQL: {sql_text}")
    logger.info(f"Generated SQL: {sql_text}")
    try:
        sql = ensure_limit(sql_text, limit=100).replace("{{table}}", f'"{ds.table_name}"')
        sql = _make_string_filters_case_insensitive(sql, col_types)
        # if joins exist but all columns are from main table, strip joins by using only main table
        tables = _extract_tables(sql_text)
        if len(tables) > 1:
            cols_tables = set()
            try:
                parsed = sqlglot.parse_one(sql_text, read="postgres")
                for c in parsed.find_all(exp.Column):
                    if c.table:
                        cols_tables.add(c.table.lower())
            except Exception:
                cols_tables = set()
            if cols_tables and cols_tables.issubset({ds.table_name.lower()}):
                sql = sqlglot.parse_one(sql_text, read="postgres")
                # replace all table names with main table
                for tbl in sql.find_all(exp.Table):
                    tbl.set("this", sqlglot.exp.to_identifier(ds.table_name))
                sql = ensure_limit(sql.sql(dialect="postgres"), limit=100)
                sql = _make_string_filters_case_insensitive(sql, col_types)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"{exc} | SQL: {sql_text}")
    stmt = text(sql)

    try:
        result = await session.execute(stmt, intent_params or {})
    except Exception as exc:
        repaired = await repair_sql(sql, str(exc))
        if not repaired:
            raise HTTPException(status_code=400, detail=str(exc))
        try:
            if not is_safe_sql(repaired):
                raise HTTPException(status_code=400, detail="Repaired SQL considered unsafe")
            repaired_cols = extract_columns(repaired)
            for col in repaired_cols:
                normalized = normalize_column(col)
                if normalized == "*" or not normalized:
                    continue
                if normalized not in allowed_cols:
                    raise HTTPException(
                        status_code=400, detail=f"Invalid column referenced after repair: {normalized} | SQL: {repaired}"
                    )
            sql = ensure_limit(repaired, limit=100).replace("{{table}}", f'"{ds.table_name}"')
            stmt = text(sql)
            result = await session.execute(stmt)
        except Exception as exc2:
            raise HTTPException(status_code=400, detail=f"SQL failed after repair: {exc2}")

    if result.returns_rows:
        columns = _build_column_metadata(result)
        rows = [dict(r) for r in result.mappings().all()]
        row_count = len(rows)
    else:
        row_count = result.rowcount or 0
        rows = []
        columns = []
        await session.commit()

    result_type = _classify_result(sql, rows, columns)

    answer = None
    if body.question:
        answer = await generate_answer(body.question, sql, rows)

    response_payload = {
        "type": result_type,
        "sql": sql,
        "rows": rows,
        "columns": columns,
        "row_count": row_count,
        "answer": answer,
    }

    if cache_key:
        _query_cache[cache_key] = response_payload

    await log_action(
        session, ds.id, current_user.id, "query", {"question": body.question, "sql": sql, "row_count": row_count}
    )
    return QueryResponse(**response_payload)


@router.patch("/{dataset_id}/cell", response_model=EditCellResponse)
async def edit_cell(
    dataset_id: str,
    body: EditCellRequest,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    # validate column exists
    col_stmt = select(DatasetColumn).where(DatasetColumn.dataset_id == ds.id, DatasetColumn.name == body.column)
    col_res = await session.execute(col_stmt)
    col = col_res.scalar_one_or_none()
    if not col:
        raise HTTPException(status_code=400, detail="Column not found")
    stmt = text(f'UPDATE "{ds.table_name}" SET "{body.column}" = :value WHERE id = :pk')
    await session.execute(stmt, {"value": body.value, "pk": body.id})
    await log_action(session, ds.id, current_user.id, "edit_cell", {"row_id": body.id, "column": body.column})
    await session.commit()
    return EditCellResponse(ok=True)


@router.post("/{dataset_id}/dashboard/rebuild", response_model=DashboardStatusResponse)
async def rebuild_dashboard(
    dataset_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    await rebuild_dashboard_stub(session, ds)
    await log_action(session, ds.id, current_user.id, "dashboard_rebuild", {})
    return DashboardStatusResponse(status="queued")


@router.get("/{dataset_id}/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    dataset_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    payload = await fetch_dashboard(session, ds.id)
    await log_action(session, ds.id, current_user.id, "dashboard_fetch", {})
    return payload


@router.post("/{dataset_id}/actions", response_model=ActionsResponse)
async def apply_actions(
    dataset_id: str,
    body: ActionsRequest,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    schema_meta = await get_schema_metadata(session, dataset_id)
    if not schema_meta:
        raise HTTPException(status_code=404, detail="Dataset not found")
    dataset_obj, columns_meta = schema_meta
    actions = await llm_client.generate_actions(session, dataset_obj.id, body.message, {"table": dataset_obj.table_name, "columns": [{"name": c.name, "type": c.db_type} for c in columns_meta]})

    if not actions:
        return ActionsResponse(actions=[], applied=False, notes="No actions generated")

    try:
        async with session.begin():
            for action in actions:
                atype = action.get("type")
                if atype == "write_cell":
                    col = action["column"]
                    col_stmt = select(DatasetColumn).where(DatasetColumn.dataset_id == ds.id, DatasetColumn.name == col)
                    col_res = await session.execute(col_stmt)
                    if not col_res.scalar_one_or_none():
                        raise HTTPException(status_code=400, detail=f"Column {col} not found")
                    stmt = text(f'UPDATE "{ds.table_name}" SET "{col}" = :value WHERE id = :pk')
                    await session.execute(stmt, {"value": action["value"], "pk": action["row_id"]})
                elif atype == "add_column":
                    name = action["name"]
                    db_type = action["db_type"]
                    add_sql = text(f'ALTER TABLE "{ds.table_name}" ADD COLUMN "{name}" {db_type}')
                    await session.execute(add_sql)
                    col = DatasetColumn(
                        dataset_id=ds.id,
                        original_name=name,
                        name=name,
                        db_type=db_type,
                        order=len(schema["columns"]) + 1,
                        is_nullable=True,
                    )
                    session.add(col)
                    if "fill" in action and action["fill"]:
                        expr = action["fill"].get("formula_sql")
                        if expr and not is_safe_expression_sql(expr):
                            raise HTTPException(status_code=400, detail="Unsafe fill expression")
                        if expr:
                            fill_sql = text(f'UPDATE "{ds.table_name}" SET "{name}" = ({expr})')
                            await session.execute(fill_sql)
                elif atype == "fill_column":
                    expr = action["expression_sql"]
                    col = action["column"]
                    if not is_safe_expression_sql(expr):
                        raise HTTPException(status_code=400, detail="Unsafe expression")
                    fill_sql = text(f'UPDATE "{ds.table_name}" SET "{col}" = ({expr})')
                    await session.execute(fill_sql)
                elif atype == "add_summary_row":
                    values = action["values"]
                    cols = [f'"{k}"' for k in values.keys()]
                    params = {k: v for k, v in values.items()}
                    placeholders = ", ".join([f":{k}" for k in values.keys()])
                    ins_sql = text(f'INSERT INTO "{ds.table_name}" ({", ".join(cols)}) VALUES ({placeholders})')
                    await session.execute(ins_sql, params)
                else:
                    raise HTTPException(status_code=400, detail=f"Unknown action {atype}")
    except HTTPException:
        await session.rollback()
        raise
    except Exception as exc:
        await session.rollback()
        raise HTTPException(status_code=400, detail=str(exc))

    await log_action(session, ds.id, current_user.id, "actions", {"actions": actions})
    return ActionsResponse(actions=actions, applied=True, notes="Applied", preview=None)
