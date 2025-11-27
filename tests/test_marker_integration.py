from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from des.db.catalog import CatalogEntry
from des.db.connector import Base
from des.marker.advanced_marker import AdvancedFileMarker
from des.marker.models import MarkerConfig, MarkerStatus
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker


async def _ensure_tables(engine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_marker_end_to_end_shutdown(async_db_engine) -> None:
    await _ensure_tables(async_db_engine)
    session_factory = async_sessionmaker(async_db_engine, expire_on_commit=False)
    config = MarkerConfig(batch_size=5, max_age_days=1, enable_dead_letter_queue=False)
    marker = AdvancedFileMarker(session_factory, config=config)

    entries = [
        CatalogEntry(created_at=datetime.now(timezone.utc) - timedelta(days=2))
        for _ in range(3)
    ]

    async with session_factory() as session:
        async with session.begin():
            session.add_all(entries)

    task = asyncio.create_task(marker.run_forever(interval_seconds=0.05))
    await asyncio.sleep(0.2)
    await marker.shutdown()
    await asyncio.wait_for(task, timeout=5)

    async with session_factory() as session:
        result = await session.execute(select(CatalogEntry))
        updated = list(result.scalars().all())

    assert all(entry.des_status == MarkerStatus.MARKED.value for entry in updated)
    assert all(entry.des_name for entry in updated)
