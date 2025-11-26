"""Source file providers for MultiShardPacker."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, cast

from des.db.source_config import MultiSourceConfig
from des.db.source_connector import SourceDatabaseConnector

logger = logging.getLogger(__name__)


class PendingFile:
    """File pending packing (with data loaded from S3)."""

    def __init__(
        self,
        id: int,
        shard_id: int,
        name: str,
        data: bytes,
        meta: Optional[dict[str, Any]] = None,
    ):
        self.id = id
        self.shard_id = shard_id
        self.name = name
        self.data = data
        self.meta = meta or {}


class MultiSourceFileProvider:
    """
    Provides files from multiple source databases.

    Handles:
    - Multiple database connections
    - Atomic claiming from source DBs
    - S3 download
    - Error handling & retry
    """

    def __init__(self, config: MultiSourceConfig, s3_client: Any, holder_id: str):
        """
        Initialize provider.

        Args:
            config: Multi-source configuration
            s3_client: boto3 S3 client
            holder_id: Pod/process identifier
        """
        self.config = config
        self.s3 = s3_client
        self.holder_id = holder_id

        # Initialize connectors
        self.connectors: Dict[str, SourceDatabaseConnector] = {}
        for source_config in config.get_enabled_sources():
            connector = SourceDatabaseConnector(source_config)
            self.connectors[source_config.name] = connector

        logger.info(
            f"Initialized provider with {len(self.connectors)} source databases"
        )

    def connect_all(self) -> None:
        """Connect to all enabled source databases."""
        for name, connector in self.connectors.items():
            try:
                connector.connect()
            except Exception as e:
                logger.error(f"Failed to connect to {name}: {e}")
                raise

    def disconnect_all(self) -> None:
        """Disconnect from all databases."""
        for connector in self.connectors.values():
            try:
                connector.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting: {e}")

    async def get_pending_files(self, shard_id: int, limit: int) -> List[PendingFile]:
        """
        Get pending files for shard from all sources.

        Workflow:
        1. Claim from all enabled source DBs (round-robin)
        2. Download from S3
        3. Return as PendingFile list

        Args:
            shard_id: Target shard ID
            limit: Maximum files to return

        Returns:
            List of PendingFile ready for packing
        """
        pending_files: List[PendingFile] = []
        remaining = limit

        # Claim from each source until we have enough
        for name, connector in self.connectors.items():
            if remaining <= 0:
                break

            try:
                # Claim batch from this source
                source_files = await asyncio.to_thread(
                    connector.claim_pending_files,
                    shard_id=shard_id,
                    holder_id=self.holder_id,
                    limit=remaining,
                )

                if not source_files:
                    continue

                # Download from S3
                for sf in source_files:
                    try:
                        # Download file data
                        data = await self._download_from_s3(
                            bucket=sf.s3_bucket, key=sf.s3_key
                        )

                        # Extract filename from S3 key
                        filename = sf.s3_key.split("/")[-1]

                        # Create metadata
                        meta: Dict[str, Any] = {
                            "source_db": name,
                            "source_file_id": sf.id,
                            "original_s3_bucket": sf.s3_bucket,
                            "original_s3_key": sf.s3_key,
                            **sf.metadata,
                        }

                        pending_files.append(
                            PendingFile(
                                id=sf.id,
                                shard_id=sf.shard_id,
                                name=filename,
                                data=data,
                                meta=meta,
                            )
                        )

                        remaining -= 1

                        if remaining <= 0:
                            break

                    except Exception as e:
                        logger.error(
                            f"Failed to download {sf.s3_bucket}/{sf.s3_key}: {e}"
                        )
                        # Mark as failed
                        await asyncio.to_thread(
                            connector.mark_files_failed,
                            file_ids=[sf.id],
                            error_message=str(e),
                        )

            except Exception as e:
                logger.error(f"Error claiming from {name}: {e}")
                continue

        logger.info(
            f"Fetched {len(pending_files)} files for shard {shard_id} "
            f"from {len(self.connectors)} sources"
        )

        return pending_files

    async def _download_from_s3(self, bucket: str, key: str) -> bytes:
        """
        Download file from S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            File content as bytes
        """
        loop = asyncio.get_event_loop()

        # Run boto3 in thread pool
        resp: Dict[str, Any] = await loop.run_in_executor(
            None, lambda: self.s3.get_object(Bucket=bucket, Key=key)
        )

        body = resp["Body"]
        return cast(bytes, body.read())

    async def mark_files_packed(
        self,
        source_db: str,
        file_ids: List[int],
        des_names: List[str],
        container_id: int,
    ) -> None:
        """
        Mark files as packed in source database.

        Args:
            source_db: Source database name
            file_ids: List of source file IDs
            des_names: List of DES snowflake names
            container_id: DES container ID
        """
        connector = self.connectors.get(source_db)
        if not connector:
            logger.error(f"Unknown source database: {source_db}")
            return

        await asyncio.to_thread(
            connector.mark_files_packed,
            file_ids=file_ids,
            des_names=des_names,
            container_id=container_id,
        )

    def get_all_stats(self) -> Dict[str, Any]:
        """Get stats from all source databases."""
        all_stats: Dict[str, Any] = {}

        for name, connector in self.connectors.items():
            try:
                stats = connector.get_stats()
                all_stats[name] = stats
            except Exception as e:
                logger.error(f"Failed to get stats from {name}: {e}")
                all_stats[name] = {"error": str(e)}

        return all_stats

    def __enter__(self) -> "MultiSourceFileProvider":
        """Context manager entry."""
        self.connect_all()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Context manager exit."""
        self.disconnect_all()
