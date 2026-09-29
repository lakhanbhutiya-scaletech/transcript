"""Async engine and session factory lifecycle."""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ...config import DatabaseSettings
from .models import Base

logger = logging.getLogger(__name__)


class Database:
    """Owns the engine; hands out sessions and closes cleanly on shutdown."""

    def __init__(self, settings: DatabaseSettings) -> None:
        self._engine: AsyncEngine = create_async_engine(
            settings.url,
            echo=settings.echo,
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            pool_pre_ping=True,
        )
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """A session scoped to one unit of work, committed or rolled back."""
        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def create_all(self) -> None:
        """Dev/test convenience. Production schema changes go through Alembic."""
        async with self._engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def ping(self) -> bool:
        from sqlalchemy import text

        try:
            async with self._engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        except Exception:
            logger.exception("Database ping failed")
            return False

    async def aclose(self) -> None:
        await self._engine.dispose()
