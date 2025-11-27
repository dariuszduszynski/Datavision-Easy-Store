#!/usr/bin/env python3
"""
Entrypoint dla Multi-Shard Packer.
"""

import logging
import os
import socket

from des.assignment.shard_router import ShardAssignment
from des.config.config import Config
from des.db.postgres import PostgresConnector
from des.packer.multi_shard_packer import MultiShardPacker
from des.utils.signals import setup_signal_handlers

logger = logging.getLogger(__name__)


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
    packer = MultiShardPacker(pod_index, assignment, config, db)

    # Setup graceful shutdown
    setup_signal_handlers(packer)

    # Run
    packer.run_forever()


if __name__ == "__main__":
    main()
