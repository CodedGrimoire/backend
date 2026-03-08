from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from app.core.config import settings

# TEMP DEBUG: verify what the app is *actually* using
#print("DATABASE_URL (app):", settings.database_url)

engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_pre_ping=True,
    connect_args={"ssl": True},  # Neon needs TLS; asyncpg uses ssl=True
)

SessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

async def get_session() -> AsyncSession:
    async with SessionLocal() as session:
        yield session