import uuid
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dataset import Dataset, DatasetColumn


async def build_schema_context(session: AsyncSession, dataset_id: uuid.UUID) -> str:
    ds_res = await session.execute(select(Dataset).where(Dataset.id == dataset_id))
    dataset = ds_res.scalar_one_or_none()
    if not dataset:
        return ""
    cols_res = await session.execute(
        select(DatasetColumn).where(DatasetColumn.dataset_id == dataset_id).order_by(DatasetColumn.order)
    )
    lines = [f"Dataset table: {dataset.table_name}", "", "Columns:"]
    for col in cols_res.scalars().all():
        samples = ""
        if col.sample_values and col.sample_values.get("sample_values"):
            samples = f' example values: {", ".join([str(v) for v in col.sample_values.get("sample_values")[:3]])}'
        lines.append(f"- {col.name} ({col.db_type}){samples}")
    lines.append("")
    lines.append(f"Notes:\n- Rows: {dataset.row_count}")
    return "\n".join(lines)


async def build_full_schema_context(session: AsyncSession) -> str:
    sql = """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position
    """
    res = await session.execute(text(sql))
    schema = {}
    for table, col in res.fetchall():
        schema.setdefault(table, []).append(col)
    lines = []
    for table, cols in schema.items():
        cols_join = ", ".join(cols)
        lines.append(f"{table}({cols_join})")
    return "\n".join(lines)
