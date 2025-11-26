from __future__ import annotations

import asyncio
import os

from des.db.connector import DesDbConnector
from des.marker.file_marker import FileMarkerWorker
from des.utils.logging import configure_logging, get_logger
from des.utils.snowflake_name import SnowflakeNameConfig


def _load_snowflake_config() -> SnowflakeNameConfig:
    prefix = os.getenv("DES_NAME_PREFIX")
    node_id = int(os.getenv("DES_NODE_ID", "0"))
    base = SnowflakeNameConfig()
    return SnowflakeNameConfig(
        node_id=node_id,
        prefix=prefix or base.prefix,
        wrap_bits=base.wrap_bits,
    )


def main() -> None:
    """Entry point for the DES marker worker."""
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"), json_output=False)
    logger = get_logger(__name__)

    batch_size = int(os.getenv("DES_MARKER_BATCH_SIZE", "100"))
    max_age_days = int(os.getenv("DES_MARKER_MAX_AGE_DAYS", "1"))
    interval_seconds = int(os.getenv("DES_MARKER_INTERVAL_SECONDS", "5"))

    connector = DesDbConnector()
    worker = FileMarkerWorker(
        connector.session_factory,
        batch_size=batch_size,
        max_age_days=max_age_days,
        snowflake_config=_load_snowflake_config(),
    )

    async def _run() -> None:
        await connector.init_models()
        await worker.run_forever(interval_seconds=interval_seconds)

    logger.info(
        "starting_marker",
        batch_size=batch_size,
        max_age_days=max_age_days,
        interval_seconds=interval_seconds,
        db_url=connector.db_url,
    )

    asyncio.run(_run())


if __name__ == "__main__":
    main()
