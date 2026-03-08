from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dataset import Dataset, DatasetColumn
from app.models.user import User


async def get_schema_metadata(session: AsyncSession, dataset_id: str):
    ds_stmt = select(Dataset, User).join(User, Dataset.owner_id == User.id).where(Dataset.id == dataset_id)
    ds_res = await session.execute(ds_stmt)
    ds_row = ds_res.first()
    if not ds_row:
        return None
    dataset = ds_row[0]
    cols_res = await session.execute(
        select(DatasetColumn).where(DatasetColumn.dataset_id == dataset.id).order_by(DatasetColumn.order)
    )
    columns = list(cols_res.scalars().all())
    return dataset, columns
