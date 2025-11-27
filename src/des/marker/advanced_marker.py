from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from des.db.catalog import CatalogEntry
from des.marker.models import MarkerConfig, MarkerStats, MarkerStatus
from des.marker.rate_limiter import TokenBucketRateLimiter
from des.monitoring.marker_metrics import (
    MARKER_BATCH_DURATION,
    MARKER_BATCH_SIZE,
    MARKER_BATCH_STATS,
    MARKER_DLQ_ENTRIES,
    MARKER_ENTRIES_MARKED,
    MARKER_ENTRY_LATENCY,
    MARKER_ERRORS,
    MARKER_RATE_LIMIT,
    MARKER_RETRIES,
)
from des.utils.logging import get_logger
from des.utils.snowflake_name import SnowflakeNameConfig, SnowflakeNameGenerator

logger = get_logger(__name__)


class HashStrategy(Protocol):
    """Protocol for different hash computation strategies."""

    def compute_hash(self, name: str) -> str:
        """Compute hash from DES name."""
        ...


class SHA256HashStrategy:
    """Default SHA-256 hash strategy."""

    def compute_hash(self, name: str) -> str:
        return hashlib.sha256(name.encode("utf-8")).hexdigest()


class AdvancedFileMarker:
    """
    Enhanced file marker with:
    - Exponential backoff retry logic
    - Rate limiting for source DB protection
    - Prometheus metrics
    - Dead letter queue for permanent failures
    - Graceful shutdown handling
    - Detailed batch statistics
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        config: Optional[MarkerConfig] = None,
        snowflake_config: Optional[SnowflakeNameConfig] = None,
        hash_strategy: Optional[HashStrategy] = None,
    ) -> None:
        self.session_factory = session_factory
        self.config = config or MarkerConfig()
        self._generator = SnowflakeNameGenerator(config=snowflake_config)
        self.hash_strategy = hash_strategy or SHA256HashStrategy()
        self.logger = get_logger(__name__)

        MARKER_BATCH_SIZE.set(self.config.batch_size)
        if self.config.rate_limit_per_second:
            self.rate_limiter: Optional[TokenBucketRateLimiter] = (
                TokenBucketRateLimiter(rate=self.config.rate_limit_per_second)
            )
            MARKER_RATE_LIMIT.set(self.config.rate_limit_per_second)
        else:
            self.rate_limiter = None
            MARKER_RATE_LIMIT.set(0)

        self._shutdown = asyncio.Event()

    async def _select_candidates(
        self, session: AsyncSession, cutoff: datetime
    ) -> list[CatalogEntry]:
        """
        Select candidate rows with FOR UPDATE SKIP LOCKED.

        Includes retry logic for entries that previously failed.
        """
        stmt = (
            select(CatalogEntry)
            .where(
                CatalogEntry.created_at <= cutoff,
                or_(
                    CatalogEntry.des_status.is_(None),
                    CatalogEntry.des_status == MarkerStatus.RETRY.value,
                    CatalogEntry.des_name.is_(None),
                ),
            )
            .order_by(CatalogEntry.id)
            .limit(self.config.batch_size)
        )

        bind = session.get_bind()
        dialect = getattr(bind, "dialect", None)
        if dialect and getattr(dialect, "name", "") not in {"sqlite"}:
            try:
                stmt = stmt.with_for_update(skip_locked=True)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "dialect_no_for_update",
                    dialect=getattr(dialect, "name", ""),
                    error=str(exc),
                )

        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def _mark_single_entry(
        self,
        entry: CatalogEntry,
        stats: MarkerStats,
    ) -> bool:
        """
        Mark single entry with retry logic.

        Returns:
            True if successful, False if should retry.

        Raises:
            Exception for permanent failures.
        """
        start_time = time.perf_counter()

        try:
            if self.rate_limiter:
                await self.rate_limiter.acquire()

            entry.des_status = MarkerStatus.MARKING.value

            des_name = self._generator.next_name()
            des_hash = self.hash_strategy.compute_hash(des_name)
            des_shard = int(des_hash[:2], 16)

            entry.des_name = des_name
            entry.des_hash = des_hash
            entry.des_shard = des_shard
            entry.des_status = MarkerStatus.MARKED.value
            entry.retry_count = 0
            entry.last_error = None

            latency_ms = (time.perf_counter() - start_time) * 1000
            stats.record_success(latency_ms)
            MARKER_ENTRIES_MARKED.labels(status="success").inc()
            MARKER_ENTRY_LATENCY.observe(latency_ms)

            return True

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            latency_ms = (time.perf_counter() - start_time) * 1000
            error_type = type(exc).__name__

            stats.record_failure(error_type, latency_ms)
            MARKER_ERRORS.labels(error_type=error_type).inc()
            MARKER_ENTRY_LATENCY.observe(latency_ms)

            entry.last_error = str(exc)[:500]
            entry.retry_count = (entry.retry_count or 0) + 1

            self.logger.error(
                "mark_entry_failed",
                entry_id=entry.id,
                error=str(exc),
                error_type=error_type,
            )

            if self._is_retryable_error(exc):
                entry.des_status = MarkerStatus.RETRY.value
                return False

            raise

    def _is_retryable_error(self, exc: Exception) -> bool:
        """Determine if error is transient and should be retried."""
        retryable_errors = (
            "timeout",
            "connection",
            "deadlock",
            "lock",
            "temporary",
        )
        error_msg = str(exc).lower()
        return any(keyword in error_msg for keyword in retryable_errors)

    async def mark_batch_with_retry(self) -> MarkerStats:
        """
        Mark batch with exponential backoff retry logic.

        Returns:
            MarkerStats with detailed processing information.
        """
        batch_id = str(uuid.uuid4())[:8]
        stats = MarkerStats(batch_id=batch_id, started_at=datetime.now(timezone.utc))
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.config.max_age_days)

        start_time = time.perf_counter()

        async with self.session_factory() as session:
            async with session.begin():
                entries = await self._select_candidates(session, cutoff)

                if not entries:
                    stats.skipped = self.config.batch_size
                    stats.completed_at = datetime.now(timezone.utc)
                    MARKER_BATCH_STATS.labels(metric="skipped").observe(
                        float(stats.skipped)
                    )
                    return stats

                for entry in entries:
                    if self._shutdown.is_set():
                        self.logger.info("shutdown_during_batch", batch_id=batch_id)
                        break

                    for attempt in range(self.config.max_retries):
                        try:
                            success = await self._mark_single_entry(entry, stats)
                            if success:
                                break

                            if attempt < self.config.max_retries - 1:
                                wait_time = self.config.retry_backoff_base**attempt
                                await asyncio.sleep(wait_time)
                                MARKER_RETRIES.labels(attempt=str(attempt + 1)).inc()
                                stats.retried += 1
                            else:
                                entry.des_status = MarkerStatus.RETRY.value
                        except Exception as exc:  # noqa: BLE001
                            if self.config.enable_dead_letter_queue:
                                await self._send_to_dlq(session, entry, str(exc))
                                MARKER_DLQ_ENTRIES.inc()
                            entry.des_status = MarkerStatus.FAILED.value
                            break

        stats.completed_at = datetime.now(timezone.utc)
        duration_seconds = time.perf_counter() - start_time

        MARKER_BATCH_DURATION.observe(duration_seconds)
        MARKER_BATCH_STATS.labels(metric="successful").observe(float(stats.successful))
        MARKER_BATCH_STATS.labels(metric="failed").observe(float(stats.failed))
        MARKER_BATCH_STATS.labels(metric="retried").observe(float(stats.retried))

        self.logger.info(
            "batch_completed",
            batch_id=batch_id,
            duration_seconds=duration_seconds,
            **stats.__dict__,
        )

        return stats

    async def _send_to_dlq(
        self,
        session: AsyncSession,
        entry: CatalogEntry,
        error: str,
    ) -> None:
        """Send failed entry to dead letter queue for investigation."""
        from sqlalchemy import text

        dlq_insert = text(
            f"""
            INSERT INTO {self.config.dlq_table}
                (catalog_entry_id, created_at, error_message, retry_count)
            VALUES
                (:entry_id, :created_at, :error, :retry_count)
        """
        )

        await session.execute(
            dlq_insert,
            {
                "entry_id": entry.id,
                "created_at": datetime.now(timezone.utc),
                "error": error[:500],
                "retry_count": self.config.max_retries,
            },
        )

    async def run_forever(self, interval_seconds: int = 5) -> None:
        """
        Continuously mark files with graceful shutdown support.

        Args:
            interval_seconds: Sleep duration when idle.
        """
        self.logger.info(
            "marker_started",
            config=self.config.__dict__,
        )

        try:
            while not self._shutdown.is_set():
                try:
                    stats = await self.mark_batch_with_retry()

                    if stats.total_processed == 0:
                        self.logger.debug(
                            "marker_idle",
                            sleep_seconds=interval_seconds,
                        )
                        try:
                            await asyncio.wait_for(
                                self._shutdown.wait(),
                                timeout=interval_seconds,
                            )
                        except asyncio.TimeoutError:
                            continue
                    else:
                        continue

                except Exception:  # noqa: BLE001
                    self.logger.exception("marker_batch_failed")
                    await asyncio.sleep(interval_seconds)

        finally:
            self.logger.info("marker_stopped")

    async def shutdown(self) -> None:
        """Graceful shutdown - complete current batch before stopping."""
        self.logger.info("marker_shutdown_requested")
        self._shutdown.set()

    def stop(self) -> None:
        """Signal shutdown from synchronous contexts (e.g., signal handlers)."""
        self._shutdown.set()
