import hashlib
import logging
from typing import Any, Dict
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd

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
from app.services.datasets.schema_context import build_schema_context, build_full_schema_context
from app.services.datasets.profiling import profile_dataset
from app.services.llm.client import llm_client, generate_sql, generate_answer, repair_sql

router = APIRouter()
logger = logging.getLogger(__name__)
_query_cache: Dict[str, Dict[str, Any]] = {}


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


async def _schema_fingerprint(session: AsyncSession) -> tuple[str, set[str]]:
    """Return schema hash and allowed column set derived from information_schema."""
    res = await session.execute(
        text(
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "ORDER BY table_name, ordinal_position"
        )
    )
    rows = res.fetchall()
    allowed_cols = {r._mapping["column_name"].lower() for r in rows}
    fingerprint_str = "|".join(
        f"{r._mapping['table_name']}:{r._mapping['column_name']}:{r._mapping['data_type']}" for r in rows
    )
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
    df = parser.read_spreadsheet(content, file.filename, sheet_name=sheet_name)
    if df.empty:
        raise HTTPException(status_code=400, detail="File is empty")

    dataset = Dataset(owner_id=None, name=file.filename, table_name="pending", row_count=len(df), status="processing")
    # attach owner
    user_stmt = select(User).where(User.firebase_uid == current_user.firebase_uid)
    res = await session.execute(user_stmt)
    user = res.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    dataset.owner_id = user.id

    session.add(dataset)
    await session.flush()

    # normalize + create table + insert
    table_name = await dynamic_tables.create_table_from_df(session, str(dataset.id), df)
    dataset.table_name = table_name
    dataset.status = "ready"

    # Save column metadata with original_name
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

    # Profile dataset once
    await profile_dataset(session, dataset.id, table_name)

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


@router.get("/{dataset_id}", response_model=DatasetDetail)
async def get_dataset(
    dataset_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: CurrentUser = Depends(get_current_user),
):
    ds = await assert_dataset_owner(dataset_id, current_user.firebase_uid, session)
    return DatasetDetail(id=str(ds.id), name=ds.name, table=ds.table_name, status=ds.status, row_count=ds.row_count)


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
    _dataset_obj, _columns_meta = schema_meta
    schema_hash, allowed_cols = await _schema_fingerprint(session)

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
    if body.sql:
        sql_text = body.sql
    elif body.question:
        # build full DB schema context to allow joins
        schema_ctx_str = await build_full_schema_context(session)
        sql_text = await generate_sql(schema_context=schema_ctx_str, question=body.question)
    else:
        raise HTTPException(status_code=400, detail="SQL or question required")

    sql_text = sql_text.replace("`sql", "").replace("`", "").strip()

    if not is_safe_sql(sql_text):
        print(f"Rejected unsafe SQL: {sql_text}")
        raise HTTPException(status_code=400, detail=f"Unsafe SQL: {sql_text}")

    # validate columns against live schema
    cols_in_sql = extract_columns(sql_text)
    for col in cols_in_sql:
        normalized = normalize_column(col)
        if normalized == "*" or not normalized:
            continue
        if normalized not in allowed_cols:
            raise HTTPException(status_code=400, detail=f"Invalid column referenced: {normalized} | SQL: {sql_text}")
    print(f"LLM question: {body.question}")
    print(f"Generated SQL: {sql_text}")
    logger.info(f"Generated SQL: {sql_text}")
    try:
        sql = ensure_limit(sql_text, limit=100).replace("{{table}}", f'"{ds.table_name}"')
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"{exc} | SQL: {sql_text}")
    stmt = text(sql)

    try:
        result = await session.execute(stmt)
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
        rows = [dict(r._mapping) for r in result.mappings().all()]
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
