"""
DES format constants and flags.
"""

# Magic numbers
HEADER_MAGIC = b"DESHEAD1"
FOOTER_MAGIC = b"DESFOOT1"
VERSION = 1

# Struct formats
import struct
HEADER_STRUCT = struct.Struct("<8sB7s")
FOOTER_STRUCT = struct.Struct("<8sB7sQQQQQQQ")
ENTRY_FIXED_STRUCT = struct.Struct("<QQQQI")

# Footer size
FOOTER_SIZE = FOOTER_STRUCT.size  # 72 B

# Flags (bitowe)
FLAG_IS_EXTERNAL = 1 << 0  # 0x01 - plik jest zewnętrzny (w _bigFiles)
FLAG_COMPRESSED = 1 << 1   # 0x02 - reserved: plik skompresowany
FLAG_ENCRYPTED = 1 << 2    # 0x04 - reserved: plik zaszyfrowany

# Big file threshold (domyślnie 100MB)
DEFAULT_BIG_FILE_THRESHOLD = 100 * 1024 * 1024  # 100 MB