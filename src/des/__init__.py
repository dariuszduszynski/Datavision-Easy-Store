"""Datavision Easy Store (DES) - Distributed archival system."""

from .core import (
    FLAG_IS_EXTERNAL,
    DesReader,
    DesWriter,
    InMemoryIndexCache,
    RedisIndexCache,
    S3DesReader,
)
from .utils import SnowflakeNameConfig, SnowflakeNameGenerator

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
