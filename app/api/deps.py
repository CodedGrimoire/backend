from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import uuid

from app.core.config import settings
from app.core.security.firebase import verify_token
from app.core.db.session import get_session
from app.models.user import User
from app.models.dataset import Dataset
from app.schemas.auth import CurrentUser


async def _ensure_dev_user(session: AsyncSession) -> User:
    # if any user exists return first
    res = await session.execute(select(User).limit(1))
    existing = res.scalar_one_or_none()
    if existing:
        return existing
    dev_id = uuid.UUID(settings.dev_user_id)
    dev_user = User(id=dev_id, firebase_uid="dev-user", email="dev@local")
    session.add(dev_user)
    await session.commit()
    await session.refresh(dev_user)
    return dev_user


async def get_current_user(session: AsyncSession = Depends(get_session)) -> CurrentUser:
    if settings.dev_mode:
        user = await _ensure_dev_user(session)
        return CurrentUser(id=str(user.id), firebase_uid=user.firebase_uid, email=user.email)

    # Future: Firebase path (kept for compatibility)
    token_claims = await verify_token("")  # placeholder; real header parsing when re-enabled
    stmt = select(User).where(User.firebase_uid == token_claims["uid"])
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()
    if not user:
        user = User(firebase_uid=token_claims["uid"], email=token_claims.get("email"))
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return CurrentUser(id=str(user.id), firebase_uid=user.firebase_uid, email=user.email)


async def assert_dataset_owner(
    dataset_id: str,
    user_uid: str,
    session: AsyncSession,
) -> Dataset:
    stmt = select(Dataset).join(User, Dataset.owner_id == User.id).where(
        Dataset.id == dataset_id, User.firebase_uid == user_uid
    )
    result = await session.execute(stmt)
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found or not owned")
    return ds
