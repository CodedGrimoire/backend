from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert

from app.models.dataset import AuditLog


async def log_action(session: AsyncSession, dataset_id, user_id, action: str, details: dict | None):
    stmt = insert(AuditLog).values(
        dataset_id=dataset_id,
        user_id=user_id,
        action=action,
        details=details or {},
    )
    await session.execute(stmt)
    await session.commit()
