import uuid
import logging
from typing import List, Any

from sqlalchemy import text, select, inspect
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dataset import DatasetColumn, DatasetMetric
import pandas as pd
import numpy as np
import datetime


NUMERIC_TYPES = {"bigint", "double precision", "integer", "numeric", "real", "smallint"}
DATE_TYPES = {"date", "timestamp", "timestamp without time zone", "timestamp with time zone"}
logger = logging.getLogger(__name__)


def json_safe(value: Any):
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    return value


async def _fetch_scalar(session: AsyncSession, sql: str, params: dict) -> any:
    res = await session.execute(text(sql), params)
    return res.scalar()


async def profile_dataset(session: AsyncSession, dataset_id: uuid.UUID, table_name: str) -> None:
    async with session.bind.connect() as conn:
        table_exists = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).has_table(table_name)
        )
        if not table_exists:
            logger.warning("Profiling skipped because table %s does not exist yet", table_name)
            return
        columns = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_columns(table_name)
        )
    column_names = [c["name"] for c in columns]
    if not column_names:
        logger.warning("No columns found for table %s, skipping profiling", table_name)
        return
    valid_columns = set(column_names)

    # fetch columns metadata
    cols_res = await session.execute(
        select(DatasetColumn).where(DatasetColumn.dataset_id == dataset_id).order_by(DatasetColumn.order)
    )
    columns: List[DatasetColumn] = cols_res.scalars().all()

    for col in columns:
        col_name = col.name
        if not col_name or col_name not in valid_columns:
            logger.warning("Skipping invalid column during profiling: %s", col_name)
            continue
        # null count (use FILTER to avoid arithmetic issues)
        null_count = await _fetch_scalar(
            session,
            f'SELECT COUNT(*) FILTER (WHERE "{col_name}" IS NULL) FROM "{table_name}"',
            {},
        )
        col.sample_values = col.sample_values or {}
        col.sample_values["null_count"] = null_count

        # distinct sample
        sample_res = await session.execute(
            text(f'SELECT DISTINCT "{col_name}" FROM "{table_name}" WHERE "{col_name}" IS NOT NULL LIMIT 5')
        )
        samples = [json_safe(r[0]) for r in sample_res.fetchall()]
        col.sample_values["sample_values"] = samples

        # distinct count (approx)
        distinct_count = await _fetch_scalar(
            session,
            f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}"',
            {},
        )
        col.sample_values["distinct_count"] = json_safe(distinct_count)

        dbt = col.db_type.lower()
        if dbt in NUMERIC_TYPES or dbt in DATE_TYPES:
            min_v = await _fetch_scalar(session, f'SELECT MIN("{col_name}") FROM "{table_name}"', {})
            max_v = await _fetch_scalar(session, f'SELECT MAX("{col_name}") FROM "{table_name}"', {})
            col.sample_values["min"] = json_safe(min_v)
            col.sample_values["max"] = json_safe(max_v)

    # detect financial patterns -> metrics
    col_names = {c.name.lower() for c in columns}
    existing_metrics = await session.execute(
        select(DatasetMetric.name).where(DatasetMetric.dataset_id == dataset_id)
    )
    existing = {m[0].lower() for m in existing_metrics.fetchall()}

    def add_metric_if(name: str, expression: str, desc: str):
        if name.lower() in existing:
            return
        session.add(
            DatasetMetric(
                dataset_id=dataset_id,
                name=name,
                expression=expression,
                description=desc,
            )
        )

    if {"revenue", "expenses"}.issubset(col_names):
        add_metric_if("profit", "revenue - expenses", "Estimated profit metric")
    if {"price", "quantity"}.issubset(col_names):
        add_metric_if("total_sales", "price * quantity", "Estimated total sales")
    if {"cost", "sales"}.issubset(col_names):
        add_metric_if("margin", "sales - cost", "Estimated margin")

    await session.flush()
