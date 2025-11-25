"""
Datavision Easy Store (DES)
---------------------------

Compatibility shim that re-exports the main DES API when importing
from the repository root. Preferred usage is via the installed
`des` package (src layout).
"""

__version__ = "0.1.0"

from des.core.des_core import (
    DesWriter,
    DesReader,
    IndexEntry,
    FOOTER_MAGIC,
    HEADER_MAGIC,
)
from des.core.s3_des_reader import S3DesReader
from des.utils.snowflake_name import (
    SnowflakeNameGenerator,
    SnowflakeNameConfig,
)
from des.packer.daily_sharded_store import (
    DailyShardedDesStore,
    shard_from_name,
    iter_daily_des_files,
)
from buffer_to_des import pack_buffer_directory

__all__ = [
    "DesWriter",
    "DesReader",
    "IndexEntry",
    "FOOTER_MAGIC",
    "HEADER_MAGIC",
    "S3DesReader",
    "SnowflakeNameGenerator",
    "SnowflakeNameConfig",
    "DailyShardedDesStore",
    "shard_from_name",
    "iter_daily_des_files",
    "pack_buffer_directory",
]
