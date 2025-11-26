"""Datavision Easy Store (DES) - Distributed archival system."""

from .core import (
    DesWriter,
    DesReader,
    S3DesReader,
    InMemoryIndexCache,
    RedisIndexCache,
    FLAG_IS_EXTERNAL,
)
from .utils import SnowflakeNameGenerator, SnowflakeNameConfig

__all__ = [
    "DesWriter",
    "DesReader",
    "S3DesReader",
    "InMemoryIndexCache",
    "RedisIndexCache",
    "SnowflakeNameGenerator",
    "SnowflakeNameConfig",
    "FLAG_IS_EXTERNAL",
]

__version__ = "1.0.0"
