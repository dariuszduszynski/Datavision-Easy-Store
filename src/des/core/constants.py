"""
DES format constants, magic numbers, and flags.
"""
import struct


from __future__ import annotations  # ← Na początku każdego pliku
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from des.core.cache import IndexCacheBackend

# Magic numbers
HEADER_MAGIC = b"DESHEAD1"
FOOTER_MAGIC = b"DESFOOT1"
VERSION = 1

# Struct formats
# Header: magic(8) + version(1) + reserved(7) = 16 bytes
HEADER_STRUCT = struct.Struct("<8sB7s")

# Footer: magic(8) + version(1) + reserved(7) + 7 x Q (uint64) = 72 bytes
# Fields: data_start, data_length, meta_start, meta_length, index_start, index_length, file_count
FOOTER_STRUCT = struct.Struct("<8sB7sQQQQQQQ")

# Index entry fixed part: 5 x Q + I = 44 bytes
# Fields: data_offset(8), data_length(8), meta_offset(8), meta_length(8), flags(4)
ENTRY_FIXED_STRUCT = struct.Struct("<QQQQI")

# Sizes
HEADER_SIZE = HEADER_STRUCT.size  # 16 bytes
FOOTER_SIZE = FOOTER_STRUCT.size  # 72 bytes
ENTRY_FIXED_SIZE = ENTRY_FIXED_STRUCT.size  # 44 bytes

# Flags (bitwise)
FLAG_IS_EXTERNAL = 1 << 0  # 0x01 - file stored in _bigFiles/
FLAG_COMPRESSED = 1 << 1   # 0x02 - reserved: file data is compressed
FLAG_ENCRYPTED = 1 << 2    # 0x04 - reserved: file data is encrypted
FLAG_DELETED = 1 << 3      # 0x08 - reserved: logically deleted (for v2 compaction)

# Big file threshold (default: 100 MB)
DEFAULT_BIG_FILE_THRESHOLD = 100 * 1024 * 1024  # 100 MB

# Batch read settings
DEFAULT_MAX_GAP_SIZE = 1024 * 1024  # 1 MB - merge adjacent files if gap < 1MB

# Validation constants
MAX_FILENAME_LENGTH = 65535  # Max filename bytes (uint16)
MAX_META_SIZE = 10 * 1024 * 1024  # 10 MB per file metadata
MIN_DES_FILE_SIZE = HEADER_SIZE + FOOTER_SIZE  # 88 bytes minimum

# External files folder name
EXTERNAL_FILES_FOLDER = "_bigFiles"