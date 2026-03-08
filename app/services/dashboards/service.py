from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dataset import DatasetChart, DatasetMetric, DatasetInsight, Dataset


async def rebuild_dashboard_stub(session: AsyncSession, dataset: Dataset) -> None:
    # Placeholder: enqueue background work in future
    await session.execute(select(1))  # no-op to keep async signature


async def fetch_dashboard(session: AsyncSession, dataset_id) -> dict:
    metrics_res = await session.execute(select(DatasetMetric).where(DatasetMetric.dataset_id == dataset_id))
    charts_res = await session.execute(select(DatasetChart).where(DatasetChart.dataset_id == dataset_id))
    insights_res = await session.execute(select(DatasetInsight).where(DatasetInsight.dataset_id == dataset_id))
    return {
        "charts": [c.spec for c in charts_res.scalars().all()],
        "metrics": [{"name": m.name, "expression": m.expression, "description": m.description} for m in metrics_res.scalars().all()],
        "insights": [{"text": i.text, "created_at": i.created_at.isoformat()} for i in insights_res.scalars().all()],
    }
