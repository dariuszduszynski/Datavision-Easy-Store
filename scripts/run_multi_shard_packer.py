# scripts/run_multi_shard_packer.py

"""Run multi-shard packer with multiple source databases."""

import asyncio
import logging
import os
import tempfile

import boto3

from des.db.source_config import MultiSourceConfig
from des.db.connector import DesDbConnector
from des.packer.source_provider import MultiSourceFileProvider
from des.packer.storage import S3StorageBackend
from des.packer.multi_shard_packer import MultiShardPacker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    # Load source database configurations
    source_config = MultiSourceConfig.from_yaml("configs/source_databases.yaml")

    logger.info(f"Loaded {len(source_config.sources)} source configurations")
    logger.info(f"Enabled: {len(source_config.get_enabled_sources())} sources")

    # Initialize DES metadata DB
    des_db = DesDbConnector(db_url=os.getenv("DES_DB_URL"))
    await des_db.init_models()

    # Initialize S3 clients
    s3_source = boto3.client("s3")  # For downloading source files
    s3_dest = boto3.client("s3")  # For uploading DES archives

    # Initialize storage backend
    storage = S3StorageBackend(
        s3_client=s3_dest, bucket=os.getenv("DES_ARCHIVE_BUCKET", "des-archives")
    )

    # Initialize source provider
    holder_id = f"{os.getenv('HOSTNAME', 'localhost')}-{os.getpid()}"

    with MultiSourceFileProvider(
        config=source_config, s3_client=s3_source, holder_id=holder_id
    ) as provider:
        # Get shard assignments for this pod
        # For demo: handle all shards (in production, split by pod index)
        shard_ids = list(range(source_config.sources[0].get_shard_count()))

        # Initialize packer
        packer = MultiShardPacker(
            db=des_db,
            storage=storage,
            shard_ids=shard_ids,
            config={
                "batch_size": 100,
                "work_dir": os.getenv(
                    "DES_PACKER_WORKDIR", str(Path(tempfile.gettempdir()) / "des_packer")
                ),
                "holder_id": holder_id,
            },
            source_provider=provider,
        )

        # Run forever
        logger.info(f"Starting packer for {len(shard_ids)} shards...")
        await packer.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
