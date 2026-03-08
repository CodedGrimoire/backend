from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.db.session import get_session

router = APIRouter()


@router.get("", tags=["health"])
async def health(session: AsyncSession = Depends(get_session)):
    try:
        await session.execute(text("SELECT 1"))
        db_status = "healthy"
        status = "healthy"
    except Exception as exc:
        db_status = f"unhealthy: {exc}"
        status = "unhealthy"
    return {"status": status, "services": {"database": db_status}}
