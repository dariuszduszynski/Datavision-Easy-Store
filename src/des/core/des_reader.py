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
        ) = FOOTER_STRUCT.unpack(raw)

        if magic != FOOTER_MAGIC:
            raise ValueError("Invalid DES footer magic")
        if version != VERSION:
            raise ValueError(f"Unsupported DES version: {version}")

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
