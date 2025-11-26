from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from des.db.catalog import CatalogEntry
from des.utils.logging import get_logger
from des.utils.snowflake_name import SnowflakeNameConfig, SnowflakeNameGenerator


class FileMarkerWorker:
    """Asynchronous worker that marks catalog rows ready for DES packing."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        batch_size: int = 100,
        max_age_days: int = 1,
        snowflake_config: Optional[SnowflakeNameConfig] = None,
    ) -> None:
        self.session_factory = session_factory
        self.batch_size = batch_size
        self.max_age_days = max_age_days
        self._generator = SnowflakeNameGenerator(config=snowflake_config)
        self.logger = get_logger(__name__)

    async def _select_candidates(
        self, session: AsyncSession, cutoff: datetime
    ) -> list[CatalogEntry]:
        """Select candidate rows that require DES metadata."""
        stmt = (
            select(CatalogEntry)
            .where(
                CatalogEntry.created_at <= cutoff,
                or_(
                    CatalogEntry.des_status.is_(None),
                    CatalogEntry.des_status != "DES_TODO",
                ),
                or_(
                    CatalogEntry.des_name.is_(None),
                    CatalogEntry.des_hash.is_(None),
                    CatalogEntry.des_shard.is_(None),
                ),
            )
            .order_by(CatalogEntry.id)
            .limit(self.batch_size)
        )

        bind = session.get_bind()
        dialect = getattr(bind, "dialect", None)
        if dialect and getattr(dialect, "name", "") not in {"sqlite"}:
            try:
                stmt = stmt.with_for_update(skip_locked=True)
            except Exception:
                # Fallback quietly on dialects that do not support FOR UPDATE.
                pass

        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def mark_batch(self) -> int:
        """Mark a batch of old, unprocessed rows with DES metadata.

        Returns:
            Number of rows updated.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.max_age_days)

        async with self.session_factory() as session:
            async with session.begin():
                entries = await self._select_candidates(session, cutoff)
                if not entries:
                    return 0

                for entry in entries:
                    des_name = self._generator.next_name()
                    des_hash = hashlib.sha256(des_name.encode("utf-8")).hexdigest()
                    des_shard = int(des_hash[:2], 16)

                    entry.des_name = des_name
                    entry.des_hash = des_hash
                    entry.des_shard = des_shard
                    entry.des_status = "DES_TODO"

                self.logger.info("marked_batch", updated=len(entries))
                return len(entries)

    async def run_forever(self, interval_seconds: int = 5) -> None:
        """Continuously mark files, sleeping when idle."""
        while True:
            try:
                updated = await self.mark_batch()
            except Exception:
                self.logger.exception("marker_batch_failed")
                await asyncio.sleep(interval_seconds)
                continue

            if updated == 0:
                self.logger.info("marker_idle", sleep_seconds=interval_seconds)
                await asyncio.sleep(interval_seconds)
            else:
                self.logger.info("marker_processed", updated=updated)
                # Immediately attempt next batch to drain backlog.
                continue
