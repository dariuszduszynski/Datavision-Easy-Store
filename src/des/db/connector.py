"""
Async PostgreSQL connector and models for DES.
"""
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import BigInteger, Date, DateTime, Integer, String, delete, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base declarative class."""


class ShardLock(Base):
    """
    Distributed shard lock.
    """

    __tablename__ = "des_shard_locks"

    shard_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    holder_id: Mapped[str] = mapped_column(String(128), nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="held")


class DesContainer(Base):
    """
    DES container metadata.
    """

    __tablename__ = "des_containers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    shard_id: Mapped[int] = mapped_column(Integer, nullable=False)
    day: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    s3_key: Mapped[str] = mapped_column(String(512), nullable=False)
    file_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    data_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finalized_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class DesDbConnector:
    """
    Async SQLAlchemy connector for DES metadata and shard locks.
    """

    def __init__(self, db_url: Optional[str] = None, echo: bool = False):
        self.db_url = db_url or os.getenv("DES_DB_URL")
        if not self.db_url:
            raise RuntimeError("DES_DB_URL env variable is required")

        self.engine: AsyncEngine = create_async_engine(self.db_url, echo=echo, future=True)
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine, expire_on_commit=False
        )

    async def init_models(self) -> None:
        """Create tables if they do not exist."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def try_acquire_shard_lock(self, shard_id: int, holder_id: str, ttl_seconds: int) -> bool:
        """
        Try to acquire shard lock via UPSERT guarded by expiration/holder check.
        """
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl_seconds)

        stmt = (
            insert(ShardLock)
            .values(
                shard_id=shard_id,
                holder_id=holder_id,
                acquired_at=now,
                heartbeat_at=now,
                expires_at=expires_at,
                state="held",
            )
            .on_conflict_do_update(
                index_elements=[ShardLock.shard_id],
                set_={
                    "holder_id": holder_id,
                    "acquired_at": now,
                    "heartbeat_at": now,
                    "expires_at": expires_at,
                    "state": "held",
                },
                where=(ShardLock.expires_at < now) | (ShardLock.holder_id == holder_id),
            )
            .returning(ShardLock.shard_id)
        )

        async with self.session_factory() as session:
            result = await session.execute(stmt)
            await session.commit()
            return result.scalar_one_or_none() is not None

    async def renew_shard_lock(self, shard_id: int, holder_id: str, ttl_seconds: int) -> bool:
        """
        Heartbeat/extend an existing lock if still owned and unexpired.
        """
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl_seconds)

        stmt = (
            update(ShardLock)
            .where(
                ShardLock.shard_id == shard_id,
                ShardLock.holder_id == holder_id,
                ShardLock.expires_at > now,
            )
            .values(heartbeat_at=now, expires_at=expires_at, state="held")
            .returning(ShardLock.shard_id)
        )

        async with self.session_factory() as session:
            result = await session.execute(stmt)
            await session.commit()
            return result.scalar_one_or_none() is not None

    async def release_shard_lock(self, shard_id: int, holder_id: str) -> None:
        """
        Release lock if owned by holder (best-effort).
        """
        stmt = delete(ShardLock).where(
            ShardLock.shard_id == shard_id,
            ShardLock.holder_id == holder_id,
        )
        async with self.session_factory() as session:
            await session.execute(stmt)
            await session.commit()


__all__ = [
    "DesDbConnector",
    "ShardLock",
    "DesContainer",
    "Base",
]
