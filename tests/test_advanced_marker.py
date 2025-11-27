from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest
from des.db.catalog import CatalogEntry
from des.db.connector import Base
from des.marker.advanced_marker import AdvancedFileMarker
from des.marker.models import MarkerConfig, MarkerStatus
from des.marker.rate_limiter import TokenBucketRateLimiter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker


async def _ensure_tables(engine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@pytest.mark.asyncio
async def test_rate_limiter_throttles() -> None:
    """Token bucket should slow down bursts to the configured rate."""
    limiter = TokenBucketRateLimiter(rate=10.0, capacity=1)

    start = time.perf_counter()
    for _ in range(20):
        await limiter.acquire()
    duration = time.perf_counter() - start

    assert duration >= 1.8
    assert duration < 3.0


@pytest.mark.asyncio
async def test_advanced_marker_marks_old_rows(async_db_engine) -> None:
    await _ensure_tables(async_db_engine)
    session_factory = async_sessionmaker(async_db_engine, expire_on_commit=False)
    config = MarkerConfig(batch_size=10, max_age_days=1, enable_dead_letter_queue=False)
    worker = AdvancedFileMarker(session_factory, config=config)

    old_entry = CatalogEntry(created_at=datetime.now(timezone.utc) - timedelta(days=2))
    recent_entry = CatalogEntry(
        created_at=datetime.now(timezone.utc) - timedelta(hours=6)
    )

    async with session_factory() as session:
        async with session.begin():
            session.add_all([old_entry, recent_entry])

    stats = await worker.mark_batch_with_retry()
    assert stats.successful == 1
    assert stats.total_processed >= 1

    async with session_factory() as session:
        result = await session.execute(select(CatalogEntry).order_by(CatalogEntry.id))
        entries = list(result.scalars().all())

    assert entries[0].des_status == MarkerStatus.MARKED.value
    assert entries[0].des_name is not None
    assert entries[0].des_hash is not None
    assert entries[0].des_shard is not None

    assert entries[1].des_status is None
    assert entries[1].des_name is None
    assert entries[1].des_hash is None
    assert entries[1].des_shard is None
