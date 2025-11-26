from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from des.db.catalog import CatalogEntry
from des.db.connector import Base
from des.marker.file_marker import FileMarkerWorker


async def _ensure_tables(engine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@pytest.mark.asyncio
async def test_mark_batch_updates_old_rows(async_db_engine):
    await _ensure_tables(async_db_engine)
    session_factory = async_sessionmaker(async_db_engine, expire_on_commit=False)
    worker = FileMarkerWorker(session_factory, batch_size=10, max_age_days=1)

    old_entry = CatalogEntry(created_at=datetime.now(timezone.utc) - timedelta(days=2))
    recent_entry = CatalogEntry(created_at=datetime.now(timezone.utc) - timedelta(hours=12))

    async with session_factory() as session:
        async with session.begin():
            session.add_all([old_entry, recent_entry])

    updated = await worker.mark_batch()
    assert updated == 1

    async with session_factory() as session:
        result = await session.execute(select(CatalogEntry).order_by(CatalogEntry.id))
        entries = list(result.scalars().all())

    assert entries[0].des_status == "DES_TODO"
    assert entries[0].des_name is not None
    assert entries[0].des_hash is not None
    assert entries[0].des_shard is not None

    assert entries[1].des_status is None
    assert entries[1].des_name is None
    assert entries[1].des_hash is None
    assert entries[1].des_shard is None


@pytest.mark.asyncio
async def test_mark_batch_ignores_recent_only(async_db_engine):
    await _ensure_tables(async_db_engine)
    session_factory = async_sessionmaker(async_db_engine, expire_on_commit=False)
    worker = FileMarkerWorker(session_factory, batch_size=10, max_age_days=1)

    async with session_factory() as session:
        async with session.begin():
            session.add(
                CatalogEntry(created_at=datetime.now(timezone.utc) - timedelta(hours=1))
            )

    updated = await worker.mark_batch()
    assert updated == 0
