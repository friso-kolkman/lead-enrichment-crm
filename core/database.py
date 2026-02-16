"""Async PostgreSQL database setup with SQLAlchemy."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from config import settings
from core.models import Base

logger = logging.getLogger(__name__)


class Database:
    """Async database manager."""

    def __init__(self, url: str | None = None):
        self.url = url or settings.database.url
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            kwargs = {
                "echo": settings.database.echo,
                "pool_pre_ping": True,
            }
            # SQLite uses StaticPool, doesn't support pool_size
            if "sqlite" not in self.url:
                kwargs["pool_size"] = settings.database.pool_size
            self._engine = create_async_engine(self.url, **kwargs)
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        if self._session_factory is None:
            self._session_factory = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=False,
            )
        return self._session_factory

    async def create_tables(self) -> None:
        """Create all tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created")

    async def drop_tables(self) -> None:
        """Drop all tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        logger.info("Database tables dropped")

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get an async session context manager."""
        session = self.session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def close(self) -> None:
        """Close the database connection."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
        logger.info("Database connection closed")


# Global database instance
db = Database()


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for getting a database session."""
    async with db.session() as session:
        yield session


async def _run_migrations(engine: AsyncEngine) -> None:
    """Run lightweight schema migrations (idempotent).

    Tables are created by Base.metadata.create_all above, so these migrations
    only handle ALTER TABLE for columns added after initial schema.
    PostgreSQL supports IF NOT EXISTS; SQLite doesn't, so we catch errors.
    """
    from sqlalchemy import text, inspect

    is_sqlite = "sqlite" in str(engine.url)

    # ALTER TABLE migrations (columns added post-launch)
    alter_migrations = [
        ("contacts", "unsubscribed", "ALTER TABLE contacts ADD COLUMN unsubscribed BOOLEAN DEFAULT FALSE"),
        ("contacts", "unsubscribed_at", "ALTER TABLE contacts ADD COLUMN unsubscribed_at TIMESTAMP"),
    ]

    # Index migrations (work on both SQLite and PostgreSQL)
    index_migrations = [
        "CREATE INDEX IF NOT EXISTS ix_sequences_is_active ON sequences(is_active)",
        "CREATE INDEX IF NOT EXISTS ix_sequence_steps_sequence_id ON sequence_steps(sequence_id)",
        "CREATE INDEX IF NOT EXISTS ix_enrollments_status_next_send ON sequence_enrollments(status, next_send_at)",
        "CREATE INDEX IF NOT EXISTS ix_enrollments_lead_id ON sequence_enrollments(lead_id)",
    ]

    async with engine.begin() as conn:
        # Run ALTER TABLE migrations with column-existence check
        for table, column, sql in alter_migrations:
            try:
                # Check if column already exists
                result = await conn.run_sync(
                    lambda sync_conn: inspect(sync_conn).get_columns(table)
                )
                existing_cols = {c["name"] for c in result}
                if column not in existing_cols:
                    await conn.execute(text(sql))
            except Exception:
                pass  # Column already exists or table doesn't exist yet

        # Run index migrations
        for sql in index_migrations:
            try:
                await conn.execute(text(sql))
            except Exception:
                pass  # Index already exists

    logger.info("Schema migrations applied")


async def init_db() -> None:
    """Initialize database (create tables if needed)."""
    await db.create_tables()
    await _run_migrations(db.engine)


async def close_db() -> None:
    """Close database connection."""
    await db.close()
