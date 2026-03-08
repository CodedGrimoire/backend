import uuid
from typing import List

from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dataset import DatasetColumn, DatasetMetric


NUMERIC_TYPES = {"bigint", "double precision", "integer", "numeric", "real", "smallint"}
DATE_TYPES = {"date", "timestamp", "timestamp without time zone", "timestamp with time zone"}


async def _fetch_scalar(session: AsyncSession, sql: str, params: dict) -> any:
    res = await session.execute(text(sql), params)
    return res.scalar()


async def profile_dataset(session: AsyncSession, dataset_id: uuid.UUID, table_name: str) -> None:
    # fetch columns metadata
    cols_res = await session.execute(
        select(DatasetColumn).where(DatasetColumn.dataset_id == dataset_id).order_by(DatasetColumn.order)
    )
    columns: List[DatasetColumn] = cols_res.scalars().all()

    for col in columns:
        col_name = col.name
        # null count
        null_count = await _fetch_scalar(
            session,
            f'SELECT COUNT(*) - COUNT("{col_name}") FROM "{table_name}"',
            {},
        )
        col.sample_values = col.sample_values or {}
        col.sample_values["null_count"] = null_count

        # distinct sample
        sample_res = await session.execute(
            text(f'SELECT DISTINCT "{col_name}" FROM "{table_name}" WHERE "{col_name}" IS NOT NULL LIMIT 5')
        )
        samples = [r[0] for r in sample_res.fetchall()]
        col.sample_values["sample_values"] = samples

        # distinct count (approx)
        distinct_count = await _fetch_scalar(
            session,
            f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}"',
            {},
        )
        col.sample_values["distinct_count"] = distinct_count

        dbt = col.db_type.lower()
        if dbt in NUMERIC_TYPES or dbt in DATE_TYPES:
            min_v = await _fetch_scalar(session, f'SELECT MIN("{col_name}") FROM "{table_name}"', {})
            max_v = await _fetch_scalar(session, f'SELECT MAX("{col_name}") FROM "{table_name}"', {})
            col.sample_values["min"] = min_v
            col.sample_values["max"] = max_v

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
