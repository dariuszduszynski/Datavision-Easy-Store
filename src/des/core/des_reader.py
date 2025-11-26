"""DesReader for reading DES archives."""
import json
import os
import struct
from typing import BinaryIO, Dict, List, Optional, Sequence

from des.core.constants import *
from des.core.models import IndexEntry, DesStats
from des.core.cache import IndexCacheBackend


class DesReader:
    """
    Reader do plikƈw DES.
    """

    def __init__(
        self,
        path: str,
        cache: Optional[IndexCacheBackend] = None,
        cache_key: Optional[str] = None,
    ):
        self.path = path
        self._f: BinaryIO = open(path, "rb")
        self._read_footer()
        self._cache = cache
        self._cache_key = cache_key or self._default_cache_key()
        self._index_loaded = False
        self._index_by_name: Dict[str, IndexEntry] = {}

    def _read_footer(self):
        self._f.seek(0, os.SEEK_END)
        file_size = self._f.tell()
        self.file_size = file_size
        if file_size < FOOTER_SIZE:
            raise ValueError("File too small to be a valid DES file")

        self._f.seek(file_size - FOOTER_SIZE)
        raw = self._f.read(FOOTER_SIZE)
        self._parse_footer(raw)

    def _parse_footer(self, data: bytes):
        """
        Parse and validate DES footer.
        
        Raises:
            ValueError: If footer is corrupted or contains invalid values
        """
        (
            magic,
            version,
            _reserved,
            self.data_start,
            self.data_length,
            self.meta_start,
            self.meta_length,
            self.index_start,
            self.index_length,
            self.file_count,
        ) = FOOTER_STRUCT.unpack(data)
        
        # Validate magic and version
        if magic != FOOTER_MAGIC:
            raise ValueError(
                f"Invalid DES footer magic: {magic!r} (expected {FOOTER_MAGIC!r})"
            )
        if version != VERSION:
            raise ValueError(
                f"Unsupported DES version: {version} (expected {VERSION})"
            )
        
        # Validate numeric fields are non-negative
        if self.data_start < 0:
            raise ValueError(f"Invalid data_start in footer: {self.data_start}")
        if self.data_length < 0:
            raise ValueError(f"Invalid data_length in footer: {self.data_length}")
        if self.meta_start < 0:
            raise ValueError(f"Invalid meta_start in footer: {self.meta_start}")
        if self.meta_length < 0:
            raise ValueError(f"Invalid meta_length in footer: {self.meta_length}")
        if self.index_start < 0:
            raise ValueError(f"Invalid index_start in footer: {self.index_start}")
        if self.index_length < 0:
            raise ValueError(f"Invalid index_length in footer: {self.index_length}")
        if self.file_count < 0:
            raise ValueError(f"Invalid file_count in footer: {self.file_count}")
        min_entry_size = 2 + ENTRY_FIXED_STRUCT.size
        if self.file_count * min_entry_size > self.index_length:
            raise ValueError(
                f"Invalid file_count in footer: {self.file_count} (index too small: {self.index_length} bytes)"
            )
        
        # Validate regions don't overlap and fit in file
        if self.data_start > self.file_size:
            raise ValueError(f"data_start ({self.data_start}) exceeds file size ({self.file_size})")
        if self.meta_start > self.file_size:
            raise ValueError(f"meta_start ({self.meta_start}) exceeds file size ({self.file_size})")
        if self.index_start > self.file_size:
            raise ValueError(f"index_start ({self.index_start}) exceeds file size ({self.file_size})")
        
        # Validate data region ends before meta region starts
        data_end = self.data_start + self.data_length
        if data_end > self.meta_start:
            raise ValueError(
                f"Data region overlaps meta region: data ends at {data_end}, "
                f"meta starts at {self.meta_start}"
            )
        
        # Validate meta region ends before or at index region start
        meta_end = self.meta_start + self.meta_length
        if meta_end > self.index_start:
            raise ValueError(
                f"Meta region overlaps index region: meta ends at {meta_end}, "
                f"index starts at {self.index_start}"
            )
        
        # Validate index region fits in file (before footer)
        index_end = self.index_start + self.index_length
        footer_start = self.file_size - FOOTER_SIZE
        if index_end > footer_start:
            raise ValueError(
                f"Index region overlaps footer: index ends at {index_end}, "
                f"footer starts at {footer_start}"
            )

    def _default_cache_key(self) -> str:
        try:
            mtime = int(os.path.getmtime(self.path))
        except OSError:
            mtime = 0
        return f"DES:{os.path.abspath(self.path)}:{self.file_size}:{mtime}:{VERSION}"

    def _load_index(self):
        if self._index_loaded:
            return

        if self._cache and self._cache_key:
            cached = self._cache.get(self._cache_key)
            if cached:
                self._index_by_name = {e.name: e for e in cached}
                self._index_loaded = True
                return

        self._f.seek(self.index_start)
        end = self.index_start + self.index_length
        index: Dict[str, IndexEntry] = {}

        while self._f.tell() < end:
            # u16 filename_len
            raw_len = self._f.read(2)
            if not raw_len or len(raw_len) < 2:
                break
            (name_len,) = struct.unpack("<H", raw_len)
            name_bytes = self._f.read(name_len)
            name = name_bytes.decode("utf-8")

            fixed = self._f.read(ENTRY_FIXED_STRUCT.size)
            (
                data_offset,
                data_length,
                meta_offset,
                meta_length,
                flags,
            ) = ENTRY_FIXED_STRUCT.unpack(fixed)

            index[name] = IndexEntry(
                name=name,
                data_offset=data_offset,
                data_length=data_length,
                meta_offset=meta_offset,
                meta_length=meta_length,
                flags=flags,
            )

        self._index_by_name = index
        self._index_loaded = True

        if self._cache and self._cache_key:
            self._cache.set(self._cache_key, list(index.values()))

    def list_files(self) -> List[str]:
        self._load_index()
        return list(self._index_by_name.keys())

    def get_index(self) -> List[IndexEntry]:
        self._load_index()
        return list(self._index_by_name.values())

    def get_file(self, name: str) -> bytes:
        self._load_index()
        entry = self._index_by_name.get(name)
        if entry is None:
            raise KeyError(f"File not found: {name}")

        self._f.seek(entry.data_offset)
        return self._f.read(entry.data_length)

    def get_meta(self, name: str) -> dict:
        self._load_index()
        entry = self._index_by_name.get(name)
        if entry is None:
            raise KeyError(f"File not found: {name}")

        self._f.seek(entry.meta_offset)
        raw = self._f.read(entry.meta_length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def __contains__(self, name: str) -> bool:
        self._load_index()
        return name in self._index_by_name

    def close(self):
        if not self._f.closed:
            self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
