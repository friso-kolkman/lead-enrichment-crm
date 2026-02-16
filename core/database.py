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
            self._engine = create_async_engine(
                self.url,
                echo=settings.database.echo,
                pool_size=settings.database.pool_size,
                pool_pre_ping=True,
            )
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
    """Run lightweight schema migrations (idempotent ALTER TABLE statements)."""
    migrations = [
        "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS unsubscribed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS unsubscribed_at TIMESTAMP",
        # Sequence tables
        """CREATE TABLE IF NOT EXISTS sequences (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            is_active BOOLEAN DEFAULT FALSE,
            is_paused BOOLEAN DEFAULT FALSE,
            target_tier VARCHAR(50),
            min_score INTEGER,
            max_score INTEGER,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS sequence_steps (
            id SERIAL PRIMARY KEY,
            sequence_id INTEGER NOT NULL REFERENCES sequences(id),
            step_number INTEGER NOT NULL,
            delay_days INTEGER DEFAULT 1,
            subject_template TEXT NOT NULL,
            body_template TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (sequence_id, step_number)
        )""",
        """CREATE TABLE IF NOT EXISTS sequence_enrollments (
            id SERIAL PRIMARY KEY,
            lead_id INTEGER NOT NULL REFERENCES leads(id),
            sequence_id INTEGER NOT NULL REFERENCES sequences(id),
            current_step INTEGER DEFAULT 0,
            status VARCHAR(50) DEFAULT 'active',
            enrolled_at TIMESTAMP DEFAULT NOW(),
            last_step_sent_at TIMESTAMP,
            next_send_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (lead_id, sequence_id)
        )""",
        "CREATE INDEX IF NOT EXISTS ix_sequences_is_active ON sequences(is_active)",
        "CREATE INDEX IF NOT EXISTS ix_sequence_steps_sequence_id ON sequence_steps(sequence_id)",
        "CREATE INDEX IF NOT EXISTS ix_enrollments_status_next_send ON sequence_enrollments(status, next_send_at)",
        "CREATE INDEX IF NOT EXISTS ix_enrollments_lead_id ON sequence_enrollments(lead_id)",
    ]
    async with engine.begin() as conn:
        for sql in migrations:
            await conn.execute(__import__("sqlalchemy").text(sql))
    logger.info("Schema migrations applied")


async def init_db() -> None:
    """Initialize database (create tables if needed)."""
    await db.create_tables()
    await _run_migrations(db.engine)


async def close_db() -> None:
    """Close database connection."""
    await db.close()
