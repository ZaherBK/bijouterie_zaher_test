"""
Database configuration and session factory.

This module reads the `DATABASE_URL` environment variable and sets up an
asynchronous SQLAlchemy engine. For asyncpg connections, it normalizes the URL
to ensure SSL is enabled and unsupported query parameters are removed. A
session generator is provided for FastAPI dependency injection.
"""
import os
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from fastapi import Depends  # <--- FIX: AJOUTÉ L'IMPORTATION MANQUANTE

def _normalize_asyncpg_url(url: str) -> tuple[str, dict]:
    if not url:
        return url, {}
    if url.startswith("postgresql+asyncpg://"):
        parts = urlparse(url)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q.pop("sslmode", None)
        q.pop("channel_binding", None)
        q.setdefault("ssl", "true")
        new_url = urlunparse((parts.scheme, parts.netloc, parts.path, "", urlencode(q), ""))
        return new_url, {"ssl": True}
    return url, {}

DATABASE_URL_RAW = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./hr.db")
DATABASE_URL, CONNECT_ARGS = _normalize_asyncpg_url(DATABASE_URL_RAW)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    connect_args=CONNECT_ARGS,
    pool_size=5,        # مناسب لـ Render Free
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
Base = declarative_base()

#
# --- DEBUT DE LA CORRECTION ---
#
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    (Correct) Yield an asynchronous session for use with FastAPI dependencies.
    This handles session creation, commit-on-success, rollback-on-error,
    and closing.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_db(session: AsyncSession = Depends(get_session)) -> AsyncSession:
    """
    FastAPI dependency that yields the database session from get_session.
    This is now just a simple wrapper.
    """
    yield session
#
# --- FIN DE LA CORRECTION ---
#
