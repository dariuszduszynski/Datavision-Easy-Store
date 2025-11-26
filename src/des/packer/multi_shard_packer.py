"""Multi-shard DES packer with shard locking and day rollover."""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

from botocore.exceptions import ClientError
from des.core.des_writer import DesWriter
from des.db.connector import DesContainer, DesDbConnector
from des.monitoring.metrics import (
    PACKED_BYTES,
    PACKED_FILES,
    PACKER_LOOP_DURATION,
    SHARD_LOCK_CONFLICTS,
)
from des.utils.logging import get_logger, log_context
from des.utils.retry import async_retry
from sqlalchemy import insert, update
from sqlalchemy.exc import DBAPIError

logger = get_logger(__name__)


@dataclass
class PendingFile:
    shard_id: int
    name: str
    data: bytes
    meta: Optional[dict[str, Any]] = None


class SourceFileProvider(Protocol):
    async def get_pending_files(self, shard_id: int, limit: int) -> List[PendingFile]:
        """Fetch a batch of files waiting to be packed."""


class StorageBackend(Protocol):
    async def upload(self, local_path: str, dest_key: str) -> None:
        """Upload finished DES to remote storage (e.g., S3)."""


class RetryableS3Error(Exception):
    """Raised when an S3 operation should be retried."""


class RetryableDbError(Exception):
    """Raised when a DB operation should be retried."""


class HeartbeatManager:
    """Keeps shard locks alive by periodic renewal."""

    def __init__(
        self, connector: DesDbConnector, shard_id: int, holder_id: str, ttl_seconds: int
    ):
        self.connector = connector
        self.shard_id = shard_id
        self.holder_id = holder_id
        self.ttl_seconds = ttl_seconds
        self._task: Optional[asyncio.Task[None]] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._stop.set()
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
            self._stop.clear()

    async def _run(self) -> None:
        interval = max(1, self.ttl_seconds // 2)
        while not self._stop.is_set():
            await asyncio.sleep(interval)
            await self.connector.renew_shard_lock(
                self.shard_id, self.holder_id, self.ttl_seconds
            )


class MultiShardPacker:
    """
    Processes multiple shards: acquires locks, writes DES files, rolls over daily.
    """

    def __init__(
        self,
        db: DesDbConnector,
        storage: StorageBackend,
        shard_ids: List[int],
        config: Optional[dict[str, Any]],
        source_provider: SourceFileProvider,
    ):
        self.db = db
        self.storage = storage
        self.shard_ids = shard_ids
        self.cfg: dict[str, Any] = config or {}
        self.source = source_provider

        self.batch_size = self.cfg.get("batch_size", 100)
        self.lock_ttl = self.cfg.get("lock_ttl_seconds", 30)
        self.checkpoint_every_files = self.cfg.get("checkpoint_every_files", 100)
        self.checkpoint_every_seconds = self.cfg.get("checkpoint_every_seconds", 30)
        self.loop_sleep = self.cfg.get("loop_sleep_seconds", 2)
        self.base_dir = Path(self.cfg.get("work_dir", "/tmp/des_packer"))
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.holder_id = (
            self.cfg.get("holder_id") or f"{socket.gethostname()}-{os.getpid()}"
        )
        self.dest_prefix = self.cfg.get("dest_prefix", "")

        self._writers: Dict[int, Dict[str, Any]] = {}
        self._heartbeats: Dict[int, HeartbeatManager] = {}

    async def run_forever(self) -> None:
        """Main packer loop."""
        while True:
            for shard_id in self.shard_ids:
                await self._process_shard(shard_id)
            await asyncio.sleep(self.loop_sleep)

    async def _process_shard(self, shard_id: int) -> None:
        start = time.perf_counter()
        shard_label = str(shard_id)
        with log_context(shard_id=shard_id, holder_id=self.holder_id):
            try:
                logger.info("processing_shard", shard_id=shard_id)
                acquired = await self.db.try_acquire_shard_lock(
                    shard_id, self.holder_id, self.lock_ttl
                )
                if not acquired:
                    SHARD_LOCK_CONFLICTS.labels(shard_id=shard_label).inc()
                    logger.info(
                        "shard_lock_conflict",
                        shard_id=shard_id,
                        holder_id=self.holder_id,
                    )
                    return

                if shard_id not in self._heartbeats:
                    hb = HeartbeatManager(
                        self.db, shard_id, self.holder_id, self.lock_ttl
                    )
                    self._heartbeats[shard_id] = hb
                    await hb.start()
                    logger.info(
                        "heartbeat_started",
                        shard_id=shard_id,
                        holder_id=self.holder_id,
                        ttl_seconds=self.lock_ttl,
                    )

                today = date.today()
                await self._ensure_writer(shard_id, today)

                files = await self._claim_files(shard_id, self.batch_size)
                if not files:
                    logger.info(
                        "no_files_to_pack",
                        shard_id=shard_id,
                        batch_size=self.batch_size,
                    )
                    return

                state = self._writers[shard_id]
                writer: DesWriter = state["writer"]
                with log_context(
                    shard_id=shard_id,
                    holder_id=self.holder_id,
                    batch_size=self.batch_size,
                ):
                    logger.info("claimed_files", count=len(files))
                    for f in files:
                        logger.info(
                            "packing_file",
                            file_name=f.name,
                            size_bytes=len(f.data),
                            shard_id=f.shard_id,
                        )
                        writer.add_file(f.name, f.data, meta=f.meta or {})
                        state["file_count"] += 1
                        state["data_bytes"] += len(f.data)
                        PACKED_FILES.labels(shard_id=shard_label).inc()
                        PACKED_BYTES.labels(shard_id=shard_label).inc(len(f.data))

                await self._maybe_checkpoint(shard_id)
            except Exception as exc:
                logger.error(
                    "shard_processing_failed",
                    shard_id=shard_id,
                    holder_id=self.holder_id,
                    exc_info=exc,
                )
                raise
            finally:
                elapsed = time.perf_counter() - start
                PACKER_LOOP_DURATION.labels(shard_id=shard_label).set(elapsed)

    def _dest_key(self, shard_id: int, day: date) -> str:
        prefix = self.dest_prefix.rstrip("/")
        key = f"{day.isoformat()}/shard_{shard_id:02d}.des"
        return f"{prefix}/{key}" if prefix else key

    async def _ensure_writer(self, shard_id: int, day: date) -> None:
        state = self._writers.get(shard_id)
        if state and state["day"] != day:
            await self._finalize_writer(shard_id)
            state = None

        if state:
            return

        day_dir = self.base_dir / day.isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        local_path = day_dir / f"shard_{shard_id:02d}.des"

        writer = DesWriter(str(local_path))
        container_id = await self._create_container_record(
            shard_id=shard_id,
            day=day,
            status="writing",
            file_count=0,
            data_bytes=0,
            s3_key=self._dest_key(shard_id, day),
        )

        self._writers[shard_id] = {
            "day": day,
            "writer": writer,
            "path": local_path,
            "container_id": container_id,
            "file_count": 0,
            "data_bytes": 0,
            "last_checkpoint": datetime.now(timezone.utc),
        }
        logger.info(
            "writer_initialized",
            shard_id=shard_id,
            container_id=container_id,
            day=day.isoformat(),
            path=str(local_path),
        )

    async def _finalize_writer(self, shard_id: int) -> None:
        state = self._writers.get(shard_id)
        if not state:
            return

        writer: DesWriter = state["writer"]
        writer.close()

        dest_key = self._dest_key(shard_id, state["day"])
        logger.info(
            "finalizing_writer",
            shard_id=shard_id,
            container_id=state["container_id"],
            dest_key=dest_key,
            path=str(state["path"]),
            file_count=state["file_count"],
            data_bytes=state["data_bytes"],
        )
        await self._upload_to_s3(str(state["path"]), dest_key)
        await self._update_container_record(
            container_id=state["container_id"],
            status="uploaded",
            file_count=state["file_count"],
            data_bytes=state["data_bytes"],
            finalized_at=datetime.now(timezone.utc),
        )

        del self._writers[shard_id]
        logger.info(
            "writer_finalized",
            shard_id=shard_id,
            container_id=state["container_id"],
            dest_key=dest_key,
            file_count=state["file_count"],
            data_bytes=state["data_bytes"],
        )

    async def _maybe_checkpoint(self, shard_id: int) -> None:
        state = self._writers.get(shard_id)
        if not state:
            return

        now = datetime.now(timezone.utc)
        if (
            state["file_count"] % self.checkpoint_every_files == 0
            or (now - state["last_checkpoint"]).total_seconds()
            >= self.checkpoint_every_seconds
        ):
            await self._update_container_record(
                container_id=state["container_id"],
                status="writing",
                file_count=state["file_count"],
                data_bytes=state["data_bytes"],
                finalized_at=None,
            )
            logger.info(
                "container_checkpointed",
                shard_id=shard_id,
                container_id=state["container_id"],
                file_count=state["file_count"],
                data_bytes=state["data_bytes"],
            )
            state["last_checkpoint"] = now

    @async_retry(max_attempts=3, exceptions=(RetryableDbError,))
    async def _claim_files(self, shard_id: int, limit: int) -> List[PendingFile]:
        try:
            return await self.source.get_pending_files(shard_id, limit)
        except DBAPIError as exc:
            if _is_retryable_db_error(exc):
                logger.warning(
                    "claim_files_retryable_db_error",
                    shard_id=shard_id,
                    limit=limit,
                    exc_info=exc,
                )
                raise RetryableDbError from exc
            logger.error(
                "claim_files_db_error",
                shard_id=shard_id,
                limit=limit,
                exc_info=exc,
            )
            raise

    async def _create_container_record(
        self,
        shard_id: int,
        day: date,
        status: str,
        file_count: int,
        data_bytes: int,
        s3_key: str,
    ) -> int:
        now = datetime.now(timezone.utc)
        stmt = (
            insert(DesContainer)
            .values(
                shard_id=shard_id,
                day=day,
                status=status,
                s3_key=s3_key,
                file_count=file_count,
                data_bytes=data_bytes,
                created_at=now,
                finalized_at=None,
            )
            .returning(DesContainer.id)
        )
        async with self.db.session_factory() as session:
            result = await session.execute(stmt)
            await session.commit()
            return int(result.scalar_one())

    @async_retry(max_attempts=3, exceptions=(RetryableDbError,))
    async def _update_container_record(
        self,
        container_id: int,
        status: str,
        file_count: int,
        data_bytes: int,
        finalized_at: Optional[datetime],
    ) -> None:
        stmt = (
            update(DesContainer)
            .where(DesContainer.id == container_id)
            .values(
                status=status,
                file_count=file_count,
                data_bytes=data_bytes,
                finalized_at=finalized_at,
            )
        )
        try:
            async with self.db.session_factory() as session:
                await session.execute(stmt)
                await session.commit()
        except DBAPIError as exc:
            if _is_retryable_db_error(exc):
                logger.warning(
                    "update_container_retryable_db_error",
                    container_id=container_id,
                    status=status,
                    exc_info=exc,
                )
                raise RetryableDbError from exc
            logger.error(
                "update_container_db_error",
                container_id=container_id,
                status=status,
                exc_info=exc,
            )
            raise

    @async_retry(max_attempts=5, exceptions=(RetryableS3Error,))
    async def _upload_to_s3(self, local_path: str, dest_key: str) -> None:
        try:
            await self.storage.upload(local_path, dest_key)
        except ClientError as exc:
            if _is_retryable_s3_error(exc):
                logger.warning(
                    "s3_upload_retryable",
                    path=local_path,
                    dest_key=dest_key,
                    exc_info=exc,
                )
                raise RetryableS3Error from exc
            logger.error(
                "s3_upload_failed",
                path=local_path,
                dest_key=dest_key,
                exc_info=exc,
            )
            raise
        except Exception as exc:
            # Non-S3 errors bubble without retry handling.
            logger.error(
                "s3_upload_unexpected_error",
                path=local_path,
                dest_key=dest_key,
                exc_info=exc,
            )
            raise


def _is_retryable_s3_error(exc: ClientError) -> bool:
    """Return True if the S3 error is considered transient."""
    response = getattr(exc, "response", {}) or {}
    error = response.get("Error") or {}
    code = str(error.get("Code") or "").upper()
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    transient_status = {500, 503}
    transient_codes = {"500", "503", "429", "REQUESTTIMEOUT", "TOOMANYREQUESTS"}
    if status in transient_status:
        return True
    return code in transient_codes


def _is_retryable_db_error(exc: DBAPIError) -> bool:
    """Return True if DB error looks like a transient lock/deadlock issue."""
    orig = getattr(exc, "orig", None)
    sqlstate = getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)
    if sqlstate and sqlstate in {"40P01", "55P03", "40001"}:
        return True

    message = str(exc).lower()
    transient_markers = [
        "deadlock detected",
        "could not obtain lock",
        "lock wait timeout",
        "timeout expired",
        "could not serialize access",
        "serialization failure",
        "database is locked",
    ]
    return any(marker in message for marker in transient_markers)
