import re
import uuid
from typing import Dict, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dataset import DatasetColumn
from app.services.llm.client import generate_synonyms

NUMERIC_TYPES = {"bigint", "double precision", "integer", "numeric", "real", "smallint"}
DATE_TYPES = {"date", "timestamp", "timestamp without time zone", "timestamp with time zone"}


async def analyze_and_optimize(
    session: AsyncSession, dataset_id: uuid.UUID, table_name: str, columns: List[DatasetColumn] | None = None
) -> Dict:
    """Classify columns, create helpful indexes, and return metadata."""
    if columns is None:
        cols_res = await session.execute(
            text(
                """
                SELECT id, name, original_name, db_type, sample_values
                FROM dataset_columns
                WHERE dataset_id = :ds
                ORDER BY "order"
                """
            ),
            {"ds": dataset_id},
        )
        columns = []
        for row in cols_res.fetchall():
            dc = DatasetColumn(
                id=row.id,
                name=row.name,
                original_name=row.original_name,
                db_type=row.db_type,
                sample_values=row.sample_values,
            )
            columns.append(dc)

    # filter to the given table if table hint present in sample_values
    filtered = []
    for c in columns:
        tbl = None
        if isinstance(c.sample_values, dict):
            tbl = c.sample_values.get("table")
        if tbl and tbl != table_name:
            continue
        filtered.append(c)
    columns = filtered or columns

    metrics = []
    dimensions = []
    time_cols = []
    identifiers = []

    for col in columns:
        dbt = (col.db_type or "").lower()
        distinct_count = None
        if col.sample_values and col.sample_values.get("distinct_count") is not None:
            distinct_count = col.sample_values.get("distinct_count")
        # role detection
        if dbt in NUMERIC_TYPES:
            metrics.append(col.name)
        elif dbt in DATE_TYPES or re.search(r"(date|day|month|year)", col.name, re.IGNORECASE):
            time_cols.append(col.name)
        else:
            # text
            if distinct_count is not None and distinct_count <= 100:
                dimensions.append(col.name)
            elif distinct_count is not None and distinct_count > 1000:
                identifiers.append(col.name)
            else:
                dimensions.append(col.name)

    # default metric aggregations
    metric_aggs = {}
    for m in metrics:
        if re.search(r"(price|cost)", m, re.IGNORECASE):
            metric_aggs[m] = "AVG"
        else:
            metric_aggs[m] = "SUM"

    # filter values for low-card dimensions
    filter_values = {}
    for col in columns:
        if col.name in dimensions and col.sample_values:
            distinct_count = col.sample_values.get("distinct_count")
            samples = col.sample_values.get("sample_values") or []
            if distinct_count is not None and distinct_count <= 50:
                filter_values[col.name] = samples

    # create indexes for dimensions and time columns
    index_targets = set(dimensions + time_cols)
    for col in index_targets:
        idx_name = f'idx_{table_name.replace(".", "_")}_{col}'
        await session.execute(text(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ("{col}")'))

    # LLM enrichment (best-effort)
    schema_brief_lines = [f"{c.name} ({c.db_type})" for c in columns]
    schema_brief = "\n".join(schema_brief_lines)
    synonyms = await generate_synonyms(schema_brief) or {}

    metadata = {
        "table": table_name,
        "metrics": metrics,
        "dimensions": dimensions,
        "time_columns": time_cols,
        "indexed_columns": list(index_targets),
        "identifiers": identifiers,
        "metric_aggregations": metric_aggs,
        "filter_values": filter_values,
        "synonyms": synonyms,
    }
    return metadata
