#!/usr/bin/env python3
"""
Entrypoint dla Multi-Shard Packer.
"""

import asyncio
import logging
import os
import socket

from des.assignment.shard_router import ShardAssignment
from des.config.config import Config
from des.db.postgres import PostgresConnector
from des.packer.multi_shard_packer import MultiShardPacker
from des.utils.signals import setup_signal_handlers

logger = logging.getLogger(__name__)


class DummySourceProvider:
    async def get_pending_files(self, shard_id: int, limit: int):
        files: list[object] = []
        if files:
            logger.info(
                "DummySourceProvider.get_pending_files stub returning files",
                extra={"shard_id": shard_id, "limit": limit, "count": len(files)},
            )
        else:
            logger.debug(
                "DummySourceProvider.get_pending_files stub no files",
                extra={"shard_id": shard_id, "limit": limit},
            )
        return files


class NullStorageBackend:
    async def upload(self, local_path: str, dest_key: str) -> None:
        logger.info(
            "NullStorageBackend.upload stub",
            extra={"local_path": local_path, "dest_key": dest_key},
        )


def extract_pod_index() -> int:
    """Extract pod index from env/hostname; default to 0 when unavailable."""
    env_value = os.getenv("DES_POD_INDEX")
    if env_value is not None:
        try:
            return int(env_value)
        except ValueError:
            logger.warning(
                "Invalid DES_POD_INDEX; attempting hostname-derived index",
                extra={"value": env_value},
            )

    hostname = socket.gethostname()
    suffix = hostname.split("-")[-1]
    if suffix.isdigit():
        return int(suffix)

    logger.warning(
        "Hostname does not end with numeric pod index; defaulting to 0",
        extra={"hostname": hostname},
    )
    return 0


def main():
    logging.basicConfig(level=logging.INFO)

    # Load config
    config = Config.from_env()

    # Get pod index
    pod_index = extract_pod_index()
    logging.info(f"Starting packer for pod index: {pod_index}")

    # Connect to DB
    db = PostgresConnector()
    db.connect(config.database)

    # Setup shard assignment
    assignment = ShardAssignment(
        n_bits=config.migration.n_bits, num_pods=config.migration.num_pods
    )

    # Create packer
    shard_ids = assignment.get_shards_for_pod(pod_index)
    packer = MultiShardPacker(
        db=db,
        storage=NullStorageBackend(),
        shard_ids=shard_ids,
        config=None,
        source_provider=DummySourceProvider(),
    )

    # Setup graceful shutdown
    setup_signal_handlers(packer)

    # Run
    asyncio.run(packer.run_forever())


if __name__ == "__main__":
    main()
