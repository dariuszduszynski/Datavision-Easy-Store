"""DesReader for reading DES archives."""

from __future__ import annotations

import json
import os
import struct
from typing import Any, BinaryIO, Optional, Sequence, cast

from des.core.constants import (
    FOOTER_SIZE,
    FOOTER_STRUCT,
    FOOTER_MAGIC,
    VERSION,
    ENTRY_FIXED_STRUCT,
    FLAG_IS_EXTERNAL,
    DEFAULT_MAX_GAP_SIZE,
)
from des.core.cache import IndexCacheBackend
from des.core.models import DesStats, IndexEntry


class DesReader:
    """
    Reader do plikƈw DES.
    """

    def __init__(
        self,
        path: str,
        cache: Optional[IndexCacheBackend] = None,
        cache_key: Optional[str] = None,
    ) -> None:
        self.path = path
        self._f: BinaryIO = open(path, "rb")
        self._cache = cache
        self._cache_key = cache_key or self._default_cache_key()
        self._index_loaded = False
        self._index_by_name: dict[str, IndexEntry] = {}
        self.file_size: int = 0
        self.data_start: int = 0
        self.data_length: int = 0
        self.meta_start: int = 0
        self.meta_length: int = 0
        self.index_start: int = 0
        self.index_length: int = 0
        self.file_count: int = 0
        self._read_footer()

    def _read_footer(self) -> None:
        self._f.seek(0, os.SEEK_END)
        file_size = self._f.tell()
        self.file_size = file_size
        if file_size < FOOTER_SIZE:
            raise ValueError("File too small to be a valid DES file")

        self._f.seek(file_size - FOOTER_SIZE)
        raw = self._f.read(FOOTER_SIZE)
        self._parse_footer(raw)

    def _parse_footer(self, data: bytes) -> None:
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
            raise ValueError(f"Unsupported DES version: {version} (expected {VERSION})")

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
            raise ValueError(
                f"data_start ({self.data_start}) exceeds file size ({self.file_size})"
            )
        if self.meta_start > self.file_size:
            raise ValueError(
                f"meta_start ({self.meta_start}) exceeds file size ({self.file_size})"
            )
        if self.index_start > self.file_size:
            raise ValueError(
                f"index_start ({self.index_start}) exceeds file size ({self.file_size})"
            )

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

    def _load_index(self) -> None:
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
        index: dict[str, IndexEntry] = {}

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

    def list_files(self) -> list[str]:
        self._load_index()
        return list(self._index_by_name.keys())

    def get_index(self) -> list[IndexEntry]:
        self._load_index()
        return list(self._index_by_name.values())

    def get_file(self, name: str) -> bytes:
        self._load_index()
        entry = self._index_by_name.get(name)
        if entry is None:
            raise KeyError(f"File not found: {name}")

        self._f.seek(entry.data_offset)
        return self._f.read(entry.data_length)

    def get_files_batch(
        self,
        names: Sequence[str],
        max_gap_size: int = DEFAULT_MAX_GAP_SIZE,
    ) -> dict[str, bytes]:
        """
        Batch read multiple files with gap merging optimization.

        Adjacent files are merged into single read if gap < max_gap_size.

        Args:
            names: Sequence of filenames to retrieve
            max_gap_size: Maximum gap between files to merge (bytes)

        Returns:
            Dict mapping filename to content (missing files are skipped)

        Raises:
            TypeError: If names is a string instead of sequence
        """
        if isinstance(names, str):
            raise TypeError("names must be a sequence of file names, not string")

        self._load_index()

        # Filter existing entries and sort by offset
        entries = []
        for name in names:
            entry = self._index_by_name.get(name)
            if entry:
                entries.append(entry)

        if not entries:
            return {}

        entries.sort(key=lambda e: e.data_offset)

        # Group adjacent entries for batch reading
        batches = self._group_entries(entries, max_gap_size)

        # Fetch batches
        results: dict[str, bytes] = {}
        for batch in batches:
            if not batch:
                continue

            # Calculate range for entire batch
            first = batch[0]
            last = batch[-1]
            start_offset = first.data_offset
            end_offset = last.data_offset + last.data_length
            batch_length = end_offset - start_offset

            # Read entire batch
            self._f.seek(start_offset)
            batch_data = self._f.read(batch_length)

            # Split by individual files
            for entry in batch:
                file_start = entry.data_offset - start_offset
                file_end = file_start + entry.data_length
                results[entry.name] = batch_data[file_start:file_end]

        return results

    def _group_entries(
        self, entries: list[IndexEntry], max_gap_size: int
    ) -> list[list[IndexEntry]]:
        """
        Group adjacent entries for batch reading.

        Files are grouped if gap between them is < max_gap_size.

        Args:
            entries: List of IndexEntry (must be sorted by data_offset)
            max_gap_size: Maximum gap to merge (bytes)

        Returns:
            List of groups (each group is List[IndexEntry])
        """
        if not entries:
            return []

        batches = []
        current_batch = [entries[0]]

        for entry in entries[1:]:
            prev_entry = current_batch[-1]
            prev_end = prev_entry.data_offset + prev_entry.data_length
            gap = entry.data_offset - prev_end

            if gap <= max_gap_size:
                # Merge into current batch
                current_batch.append(entry)
            else:
                # Start new batch
                batches.append(current_batch)
                current_batch = [entry]

        # Add last batch
        if current_batch:
            batches.append(current_batch)

        return batches

    def get_meta(self, name: str) -> dict[str, Any]:
        self._load_index()
        entry = self._index_by_name.get(name)
        if entry is None:
            raise KeyError(f"File not found: {name}")

        self._f.seek(entry.meta_offset)
        raw = self._f.read(entry.meta_length)
        if not raw:
            return {}
        return cast(dict[str, Any], json.loads(raw.decode("utf-8")))

    def get_stats(self) -> DesStats:
        """
        Get archive statistics.

        Returns:
            DesStats with file counts and sizes
        """
        self._load_index()

        internal_count = 0
        external_count = 0
        internal_size = 0
        external_size = 0

        for entry in self._index_by_name.values():
            if entry.flags & FLAG_IS_EXTERNAL:
                external_count += 1
                external_size += entry.data_length
            else:
                internal_count += 1
                internal_size += entry.data_length

        return DesStats(
            total_files=len(self._index_by_name),
            internal_files=internal_count,
            external_files=external_count,
            internal_size_bytes=internal_size,
            external_size_bytes=external_size,
            archive_size_bytes=self.file_size,
        )

    def __contains__(self, name: str) -> bool:
        self._load_index()
        return name in self._index_by_name

    def close(self) -> None:
        if not self._f.closed:
            self._f.close()

    def __enter__(self) -> "DesReader":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Any,
    ) -> None:
        self.close()
