"""
DES data models and structures.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class IndexEntry:
    """
    Represents a single file entry in the DES index.

    Attributes:
        name: File name (SnowFlake ID)
        data_offset: Absolute byte offset in DATA region (0 for external files)
        data_length: Length of file data in bytes
        meta_offset: Absolute byte offset in META region
        meta_length: Length of metadata in bytes
        flags: Bitwise flags (FLAG_IS_EXTERNAL, FLAG_COMPRESSED, etc.)
    """

    name: str
    data_offset: int
    data_length: int
    meta_offset: int
    meta_length: int
    flags: int = 0

    def is_external(self) -> bool:
        """Check if file is stored externally (_bigFiles/)."""
        from des.core.constants import FLAG_IS_EXTERNAL

        return bool(self.flags & FLAG_IS_EXTERNAL)

    def is_compressed(self) -> bool:
        """Check if file data is compressed (reserved for future)."""
        from des.core.constants import FLAG_COMPRESSED

        return bool(self.flags & FLAG_COMPRESSED)

    def is_encrypted(self) -> bool:
        """Check if file data is encrypted (reserved for future)."""
        from des.core.constants import FLAG_ENCRYPTED

        return bool(self.flags & FLAG_ENCRYPTED)

    def __repr__(self) -> str:
        flags_str = []
        if self.is_external():
            flags_str.append("EXTERNAL")
        if self.is_compressed():
            flags_str.append("COMPRESSED")
        if self.is_encrypted():
            flags_str.append("ENCRYPTED")

        flags_repr = f" [{','.join(flags_str)}]" if flags_str else ""
        return (
            f"IndexEntry(name={self.name!r}, "
            f"data_offset={self.data_offset}, "
            f"data_length={self.data_length}{flags_repr})"
        )


@dataclass
class DesStats:
    """
    Statistics for a DES archive.
    """

    total_files: int
    internal_files: int
    external_files: int
    internal_size_bytes: int
    external_size_bytes: int
    archive_size_bytes: int

    @property
    def total_size_bytes(self) -> int:
        """Total size of all files (internal + external)."""
        return self.internal_size_bytes + self.external_size_bytes

    @property
    def compression_ratio(self) -> float:
        """
        Compression ratio (archive_size / internal_size).
        Only meaningful for internal files.
        """
        if self.internal_size_bytes == 0:
            return 0.0
        return self.archive_size_bytes / self.internal_size_bytes

    def __repr__(self) -> str:
        return (
            f"DesStats(files={self.total_files} "
            f"[{self.internal_files} internal, {self.external_files} external], "
            f"size={self._human_size(self.total_size_bytes)}, "
            f"archive={self._human_size(self.archive_size_bytes)})"
        )

    @staticmethod
    def _human_size(size_bytes: int) -> str:
        """Format bytes as human-readable string."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}PB"


@dataclass
class DesFooter:
    """
    Parsed DES footer structure.
    """

    magic: bytes
    version: int
    data_start: int
    data_length: int
    meta_start: int
    meta_length: int
    index_start: int
    index_length: int
    file_count: int

    def validate(self):
        """Validate footer integrity."""
        from des.core.constants import FOOTER_MAGIC, VERSION

        if self.magic != FOOTER_MAGIC:
            raise ValueError(
                f"Invalid footer magic: {self.magic!r} (expected {FOOTER_MAGIC!r})"
            )
        if self.version != VERSION:
            raise ValueError(
                f"Unsupported DES version: {self.version} (expected {VERSION})"
            )
        if self.data_start < 0 or self.data_length < 0:
            raise ValueError("Invalid data region offsets")
        if self.meta_start < 0 or self.meta_length < 0:
            raise ValueError("Invalid meta region offsets")
        if self.index_start < 0 or self.index_length < 0:
            raise ValueError("Invalid index region offsets")
        if self.file_count < 0:
            raise ValueError("Invalid file count")

    def __repr__(self) -> str:
        return (
            f"DesFooter(version={self.version}, "
            f"files={self.file_count}, "
            f"data={self.data_start}:{self.data_start + self.data_length}, "
            f"meta={self.meta_start}:{self.meta_start + self.meta_length}, "
            f"index={self.index_start}:{self.index_start + self.index_length})"
        )


@dataclass
class ExternalFileInfo:
    """
    Information about an uploaded external file.
    """

    name: str
    s3_key: str
    size_bytes: int
    upload_timestamp: Optional[str] = None

    def __repr__(self) -> str:
        size_str = DesStats._human_size(self.size_bytes)
        return f"ExternalFileInfo(name={self.name!r}, key={self.s3_key!r}, size={size_str})"
