"""
Datavision Easy Store (DES)
---------------------------

Minimalistic multi-language-friendly binary container format
for efficient storage and retrieval of many small files inside
a single object (S3/HCP-native, supports Range GET).

This package exposes:

- DES core writer/reader (des_core)
- S3 range-based reader (s3_reader)
- Snowflake-like filename generator (snowflake_name)
- Daily sharded DES builder (daily_sharded_store)
- Simple prototype pipeline from buffer to DES (buffer_to_des)

The goal is to keep the public API stable so that the same
DES file format can be implemented in Go, Rust, C#, Java and others.

"""

__version__ = "0.1.0"

# -----------------------------
# Public API re-exports
# -----------------------------

# Core DES writer/reader
from .des_core import (
    DesWriter,
    DesReader,
    IndexEntry,
    FOOTER_MAGIC,
    HEADER_MAGIC,
)

# S3 range reader
from .src.des.core.s3_des_reader import S3DesReader

# Snowflake-like name generator
from .snowflake_name import (
    SnowflakeNameGenerator,
    SnowflakeNameConfig,
)

# Daily sharded DES store
from .daily_sharded_store import (
    DailyShardedDesStore,
    shard_from_name,
    iter_daily_des_files,
)

# OPTIONAL: buffer→DES prototype
from .buffer_to_des import pack_buffer_directory
