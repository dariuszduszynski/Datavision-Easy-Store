"""
DES Core - Binary container format for archiving files.
"""

# Core writers and readers
from des.core.des_writer import DesWriter
from des.core.des_reader import DesReader
from des.core.s3_des_reader import S3DesReader

from __future__ import annotations  # ← Na początku każdego pliku
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from des.core.cache import IndexCacheBackend

# Models
from des.core.models import (
    IndexEntry,
    DesStats,
    DesFooter,
    ExternalFileInfo,
)

# Cache backends
from des.core.cache import (
    IndexCacheBackend,
    InMemoryIndexCache,
    RedisIndexCache,
    NullCache,
)

# Constants
from des.core.constants import (
    HEADER_MAGIC,
    FOOTER_MAGIC,
    VERSION,
    FLAG_IS_EXTERNAL,
    FLAG_COMPRESSED,
    FLAG_ENCRYPTED,
    FLAG_DELETED,
    DEFAULT_BIG_FILE_THRESHOLD,
    DEFAULT_MAX_GAP_SIZE,
    EXTERNAL_FILES_FOLDER,
)

__all__ = [
    # Writers and readers
    'DesWriter',
    'DesReader',
    'S3DesReader',
    
    # Models
    'IndexEntry',
    'DesStats',
    'DesFooter',
    'ExternalFileInfo',
    
    # Cache
    'IndexCacheBackend',
    'InMemoryIndexCache',
    'RedisIndexCache',
    'NullCache',
    
    # Constants
    'HEADER_MAGIC',
    'FOOTER_MAGIC',
    'VERSION',
    'FLAG_IS_EXTERNAL',
    'FLAG_COMPRESSED',
    'FLAG_ENCRYPTED',
    'FLAG_DELETED',
    'DEFAULT_BIG_FILE_THRESHOLD',
    'DEFAULT_MAX_GAP_SIZE',
    'EXTERNAL_FILES_FOLDER',
]

__version__ = '1.0.0'
