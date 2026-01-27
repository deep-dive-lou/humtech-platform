import asyncpg
from .config import settings

_pool: asyncpg.Pool | None = None

async def init_db_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL is not set")
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=1,
            max_size=10,
            command_timeout=30,
        )
    return _pool

async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None

async def get_pool() -> asyncpg.Pool:
    if _pool is None:
        return await init_db_pool()
    return _pool
