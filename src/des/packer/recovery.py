"""Crash recovery utilities for DES packer components.

Examples:
    ```python
    import boto3
    from des.db.connector import DesDbConnector
    from des.packer.recovery import CrashRecoveryManager

    db = DesDbConnector("postgresql+asyncpg://des:des@localhost/des")
    manager = CrashRecoveryManager(
        db=db,
        s3_client=boto3.client("s3"),
        s3_bucket="des-archives",
        s3_prefix="des/",
    )

    await manager.recover_stale_claims()
    await manager.cleanup_partial_containers()
    await manager.release_expired_locks()
    await manager.verify_container_integrity()
    ```
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Set

import structlog
from sqlalchemy import delete, select, text, update

from des.core.constants import MIN_DES_FILE_SIZE
from des.core.s3_des_reader import S3DesReader
from des.db.connector import DesContainer, DesDbConnector, ShardLock
from des.monitoring.metrics import (
    RECOVERY_CONTAINER_INTEGRITY,
    RECOVERY_EXPIRED_LOCKS,
    RECOVERY_PARTIAL_CONTAINERS,
    RECOVERY_STALE_CLAIMS,
)

logger = structlog.get_logger(__name__)


class CrashRecoveryManager:
    """Recovers stale state after unexpected packer crashes."""

    def __init__(
        self,
        db: DesDbConnector,
        *,
        s3_client=None,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        source_table: str = "source_files",
        status_column: str = "status",
        claimed_by_column: str = "claimed_by",
        claimed_at_column: str = "claimed_at",
        pending_status: str = "pending",
        claimed_status: str = "claimed",
        claim_timeout_seconds: int = 300,
        container_grace_seconds: int = 900,
        cleanup_orphaned_s3: bool = True,
    ):
        """
        Args:
            db: DES metadata connector.
            s3_client: boto3 S3 client for container verification/cleanup.
            s3_bucket: Bucket containing DES archives.
            s3_prefix: Optional prefix prepended to stored `s3_key` values.
            source_table: Table with source files to unclaim.
            status_column: Column storing file status in `source_table`.
            claimed_by_column: Column storing claim owner.
            claimed_at_column: Column storing claim timestamp.
            pending_status: Status value representing unclaimed files.
            claimed_status: Status value representing claimed files.
            claim_timeout_seconds: TTL for a claim before it is considered stale.
            container_grace_seconds: How long to wait before cleaning "writing" containers.
            cleanup_orphaned_s3: Whether to delete S3 objects that have no DB record.
        """
        self.db = db
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip("/") if s3_prefix else None
        self.source_table = source_table
        self.status_column = status_column
        self.claimed_by_column = claimed_by_column
        self.claimed_at_column = claimed_at_column
        self.pending_status = pending_status
        self.claimed_status = claimed_status
        self.claim_timeout_seconds = claim_timeout_seconds
        self.container_grace_seconds = container_grace_seconds
        self.cleanup_orphaned_s3 = cleanup_orphaned_s3

    def _full_s3_key(self, key: str) -> str:
        if self.s3_prefix:
            normalized = key.lstrip("/")
            if normalized.startswith(f"{self.s3_prefix}/"):
                return normalized
            return f"{self.s3_prefix}/{normalized}"
        return key

    async def recover_stale_claims(self) -> int:
        """Unclaim files whose claims expired.

        Returns:
            Number of rows updated.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.claim_timeout_seconds)
        stmt = text(
            f"""
            UPDATE {self.source_table}
            SET {self.status_column} = :pending,
                {self.claimed_by_column} = NULL,
                {self.claimed_at_column} = NULL
            WHERE {self.status_column} = :claimed
              AND ({self.claimed_at_column} IS NULL OR {self.claimed_at_column} < :cutoff)
            """
        )

        async with self.db.session_factory() as session:
            result = await session.execute(
                stmt,
                {"pending": self.pending_status, "claimed": self.claimed_status, "cutoff": cutoff},
            )
            await session.commit()

        released = result.rowcount or 0
        RECOVERY_STALE_CLAIMS.inc(released)
        logger.info(
            "recovered_stale_claims",
            released=released,
            cutoff=cutoff.isoformat(),
            table=self.source_table,
        )
        return released

    async def cleanup_partial_containers(self) -> int:
        """Finalize or mark containers stuck in 'writing' state.

        Returns:
            Number of containers updated.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.container_grace_seconds)
        async with self.db.session_factory() as session:
            result = await session.execute(
                select(DesContainer).where(
                    DesContainer.status == "writing",
                    DesContainer.created_at < cutoff,
                )
            )
            containers = result.scalars().all()

            actions = 0
            for container in containers:
                exists = await self._s3_exists(container.s3_key)
                if not exists:
                    container.status = "failed"
                    container.finalized_at = datetime.now(timezone.utc)
                    actions += 1
                    RECOVERY_PARTIAL_CONTAINERS.labels(action="missing_s3_mark_failed").inc()
                    logger.warning(
                        "container_missing_in_s3",
                        container_id=container.id,
                        shard_id=container.shard_id,
                        key=container.s3_key,
                    )
                    continue

                valid, file_count = await self._validate_container(container.s3_key)
                if not valid:
                    await self._delete_s3_object(container.s3_key)
                    container.status = "failed"
                    container.finalized_at = datetime.now(timezone.utc)
                    actions += 1
                    RECOVERY_PARTIAL_CONTAINERS.labels(action="corrupt_mark_failed").inc()
                    logger.warning(
                        "container_corrupt_removed",
                        container_id=container.id,
                        shard_id=container.shard_id,
                        key=container.s3_key,
                    )
                    continue

                container.status = "uploaded"
                container.finalized_at = datetime.now(timezone.utc)
                if file_count is not None:
                    container.file_count = file_count
                actions += 1
                RECOVERY_PARTIAL_CONTAINERS.labels(action="finalized").inc()
                logger.info(
                    "container_finalized_after_recovery",
                    container_id=container.id,
                    shard_id=container.shard_id,
                    key=container.s3_key,
                )

            await session.commit()

        return actions

    async def release_expired_locks(self) -> int:
        """Release expired shard locks."""
        now = datetime.now(timezone.utc)
        stmt = delete(ShardLock).where(ShardLock.expires_at < now)

        async with self.db.session_factory() as session:
            result = await session.execute(stmt)
            await session.commit()

        released = result.rowcount or 0
        RECOVERY_EXPIRED_LOCKS.inc(released)
        logger.info("released_expired_locks", released=released, cutoff=now.isoformat())
        return released

    async def verify_container_integrity(self) -> int:
        """Validate DES objects against DB metadata.

        Returns:
            Number of items fixed or removed.
        """
        actions = 0
        db_keys: Set[str] = set()
        async with self.db.session_factory() as session:
            result = await session.execute(select(DesContainer))
            containers = result.scalars().all()
            for container in containers:
                full_key = self._full_s3_key(container.s3_key)
                db_keys.add(full_key)

                exists = await self._s3_exists(container.s3_key)
                if not exists:
                    container.status = "failed"
                    container.finalized_at = datetime.now(timezone.utc)
                    actions += 1
                    RECOVERY_CONTAINER_INTEGRITY.labels(outcome="missing_in_s3_mark_failed").inc()
                    logger.warning(
                        "container_missing_in_s3_mark_failed",
                        container_id=container.id,
                        shard_id=container.shard_id,
                        key=container.s3_key,
                    )
                    continue

                valid, file_count = await self._validate_container(container.s3_key)
                if not valid:
                    await self._delete_s3_object(container.s3_key)
                    container.status = "failed"
                    container.finalized_at = datetime.now(timezone.utc)
                    actions += 1
                    RECOVERY_CONTAINER_INTEGRITY.labels(outcome="corrupt_mark_failed").inc()
                    logger.warning(
                        "container_corrupt_mark_failed",
                        container_id=container.id,
                        shard_id=container.shard_id,
                        key=container.s3_key,
                    )
                    continue

                if file_count is not None and file_count != container.file_count:
                    await session.execute(
                        update(DesContainer)
                        .where(DesContainer.id == container.id)
                        .values(file_count=file_count)
                    )
                    actions += 1
                    RECOVERY_CONTAINER_INTEGRITY.labels(outcome="file_count_corrected").inc()
                    logger.info(
                        "container_file_count_corrected",
                        container_id=container.id,
                        shard_id=container.shard_id,
                        key=container.s3_key,
                        db_count=container.file_count,
                        actual_count=file_count,
                    )

            await session.commit()

        if self.cleanup_orphaned_s3 and self.s3_bucket:
            orphaned = await self._find_orphaned_s3(db_keys)
            for key in orphaned:
                await self._delete_s3_object(key)
                actions += 1
                RECOVERY_CONTAINER_INTEGRITY.labels(outcome="orphan_s3_deleted").inc()
                logger.warning("orphan_s3_object_deleted", key=key, bucket=self.s3_bucket)

        return actions

    async def _s3_exists(self, key: str) -> bool:
        """Check if S3 object exists without downloading it."""
        if not self.s3_client or not self.s3_bucket:
            return False

        full_key = self._full_s3_key(key)
        try:
            await asyncio.to_thread(self.s3_client.head_object, Bucket=self.s3_bucket, Key=full_key)
            return True
        except Exception as exc:  # boto3 uses specific exceptions per client
            logger.debug("s3_head_failed", key=full_key, error=str(exc))
            return False

    async def _delete_s3_object(self, key: str) -> None:
        if not self.s3_client or not self.s3_bucket:
            return
        full_key = self._full_s3_key(key)
        await asyncio.to_thread(self.s3_client.delete_object, Bucket=self.s3_bucket, Key=full_key)

    async def _validate_container(self, key: str) -> tuple[bool, Optional[int]]:
        """
        Validate footer and return whether the DES object looks healthy.

        Returns:
            (is_valid, file_count_if_known)
        """
        if not self.s3_client or not self.s3_bucket:
            return False, None

        full_key = self._full_s3_key(key)
        try:
            head = await asyncio.to_thread(
                self.s3_client.head_object, Bucket=self.s3_bucket, Key=full_key
            )
        except Exception as exc:
            logger.debug("s3_head_failed", key=full_key, error=str(exc))
            return False, None

        size = head.get("ContentLength", 0)
        if size < MIN_DES_FILE_SIZE:
            logger.warning("container_too_small", key=full_key, size=size)
            return False, None

        try:
            reader = await asyncio.to_thread(S3DesReader, self.s3_bucket, full_key, self.s3_client)
            return True, reader.file_count
        except Exception as exc:
            logger.warning("container_footer_invalid", key=full_key, error=str(exc))
            return False, None

    async def _find_orphaned_s3(self, known_keys: Set[str]) -> Set[str]:
        """List S3 objects under prefix and return ones absent in DB."""
        if not self.s3_client or not self.s3_bucket:
            return set()

        params = {"Bucket": self.s3_bucket}
        if self.s3_prefix:
            params["Prefix"] = f"{self.s3_prefix}/"

        orphaned: Set[str] = set()
        continuation: Optional[str] = None

        while True:
            if continuation:
                params["ContinuationToken"] = continuation
            resp = await asyncio.to_thread(self.s3_client.list_objects_v2, **params)
            contents = resp.get("Contents", []) or []
            for item in contents:
                key = item["Key"]
                if key not in known_keys:
                    orphaned.add(key)

            continuation = resp.get("NextContinuationToken")
            if not continuation:
                break

        return orphaned


__all__ = ["CrashRecoveryManager"]
