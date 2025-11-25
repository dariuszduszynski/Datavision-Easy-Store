"""
S3DesReader with support for external big files.
"""
import json
import struct
from typing import Dict, List, Optional, Sequence

import boto3

from des.core.constants import *
from des.core.models import IndexEntry, DesStats

from __future__ import annotations  # ← Na początku każdego pliku
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from des.core.cache import IndexCacheBackend


class S3DesReader:
    """
    DES reader operating directly on S3 via Range GET.
    Supports external big files in _bigFiles/ folder.
    """
    
    def __init__(
        self,
        bucket: str,
        key: str,
        s3_client=None,
        cache: Optional['IndexCacheBackend'] = None,
        cache_key: Optional[str] = None,
    ):
        """
        Args:
            bucket: S3 bucket name
            key: S3 key to DES archive (e.g. "2025-01-15/shard_00.des")
            s3_client: Optional boto3 S3 client (creates default if None)
            cache: Optional cache backend for index
            cache_key: Optional cache key (auto-generated if None)
        """
        self.bucket = bucket
        self.key = key
        self.s3 = s3_client or boto3.client("s3")
        self._cache = cache
        
        # Parse base path for external files
        # key: "2025-01-15/shard_00.des" → base_prefix: "2025-01-15"
        self.base_prefix = "/".join(key.split("/")[:-1]) if "/" in key else ""
        
        # Get object metadata
        head = self._get_head()
        self.file_size = head["ContentLength"]
        self._etag = head.get("ETag", "").strip('"')
        
        if self.file_size < MIN_DES_FILE_SIZE:
            raise ValueError(
                f"Object too small to be valid DES: {self.file_size} bytes "
                f"(min {MIN_DES_FILE_SIZE})"
            )
        
        # Load and parse footer
        footer_bytes = self._range_get(self.file_size - FOOTER_SIZE, FOOTER_SIZE)
        self._parse_footer(footer_bytes)
        
        # Index state
        self._index_loaded = False
        self._index_by_name: Dict[str, IndexEntry] = {}
        self._cache_key = cache_key or self._default_cache_key()
    
    def _get_head(self) -> dict:
        """Get S3 object metadata (HEAD)."""
        return self.s3.head_object(Bucket=self.bucket, Key=self.key)
    
    def _range_get(self, offset: int, length: int) -> bytes:
        """
        Standard Range GET from DES archive.
        
        Args:
            offset: Start byte offset
            length: Number of bytes to read
        
        Returns:
            bytes content
        """
        end = offset + length - 1
        resp = self.s3.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range=f"bytes={offset}-{end}",
        )
        return resp["Body"].read()
    
    def _external_get(self, name: str) -> bytes:
        """
        GET entire external file from _bigFiles/.
        
        Args:
            name: Filename
        
        Returns:
            File content
        
        Raises:
            KeyError: If external file not found
        """
        external_key = f"{self.base_prefix}/{EXTERNAL_FILES_FOLDER}/{name}" if self.base_prefix else f"{EXTERNAL_FILES_FOLDER}/{name}"
        
        try:
            resp = self.s3.get_object(
                Bucket=self.bucket,
                Key=external_key
            )
            return resp["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            raise KeyError(f"External file not found: {external_key}")
    
    def get_file(self, name: str) -> bytes:
        """
        Get file content from DES or external storage.
        
        Args:
            name: Filename to retrieve
        
        Returns:
            File content as bytes
        
        Raises:
            KeyError: If file not found
        """
        self._load_index()
        entry = self._index_by_name.get(name)
        if not entry:
            raise KeyError(f"File not found: {name}")
        
        # Check external flag (fast path)
        if entry.flags & FLAG_IS_EXTERNAL:
            return self._external_get(entry.name)
        
        # Internal file - standard Range GET
        return self._range_get(entry.data_offset, entry.data_length)
    
    def get_files_batch(
        self,
        names: Sequence[str],
        max_gap_size: int = DEFAULT_MAX_GAP_SIZE,
    ) -> Dict[str, bytes]:
        """
        Batch GET with support for external files.
        
        Adjacent internal files are merged into single Range GET if gap < max_gap_size.
        External files are fetched individually.
        
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
        
        # Separate internal and external files
        internal_names = []
        external_names = []
        
        for name in names:
            entry = self._index_by_name.get(name)
            if not entry:
                continue
            
            if entry.flags & FLAG_IS_EXTERNAL:
                external_names.append(name)
            else:
                internal_names.append(name)
        
        results = {}
        
        # 1. Fetch internal files (with batching)
        if internal_names:
            entries = self._entries_for_names(internal_names)
            batches = self._group_entries(entries, max_gap_size)
            internal_results = self._fetch_batches(batches)
            results.update(internal_results)
        
        # 2. Fetch external files (individually, could parallelize)
        for name in external_names:
            try:
                results[name] = self._external_get(name)
            except KeyError:
                # Skip missing external files
                pass
        
        return results
    
    def get_meta(self, name: str) -> dict:
        """
        Get file metadata (always from DES, even for external files).
        
        Args:
            name: Filename
        
        Returns:
            Metadata dict
        
        Raises:
            KeyError: If file not found
        """
        self._load_index()
        entry = self._index_by_name.get(name)
        if not entry:
            raise KeyError(f"File not found: {name}")
        
        raw = self._range_get(entry.meta_offset, entry.meta_length)
        return json.loads(raw.decode("utf-8"))
    
    def list_files(self, include_external: bool = True) -> List[str]:
        """
        List all filenames in archive.
        
        Args:
            include_external: Whether to include external files
        
        Returns:
            List of filenames
        """
        self._load_index()
        
        if include_external:
            return list(self._index_by_name.keys())
        
        # Only internal files
        return [
            name for name, entry in self._index_by_name.items()
            if not (entry.flags & FLAG_IS_EXTERNAL)
        ]
    
    def get_index(self) -> List[IndexEntry]:
        """
        Get full index as list of IndexEntry.
        
        Returns:
            List of all index entries
        """
        self._load_index()
        return list(self._index_by_name.values())
    
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
        """
        Check if file exists in archive.
        
        Args:
            name: Filename to check
        
        Returns:
            True if file exists
        """
        self._load_index()
        return name in self._index_by_name
    
    def __repr__(self) -> str:
        return f"S3DesReader(bucket={self.bucket!r}, key={self.key!r}, files={self.file_count})"
    
    # ========== Internal helper methods ==========
    
    def _parse_footer(self, data: bytes):
        """Parse DES footer."""
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
        
        if magic != FOOTER_MAGIC:
            raise ValueError(
                f"Invalid DES footer magic: {magic!r} (expected {FOOTER_MAGIC!r})"
            )
        if version != VERSION:
            raise ValueError(
                f"Unsupported DES version: {version} (expected {VERSION})"
            )
    
    def _load_index(self):
        """Load index from cache or S3."""
        if self._index_loaded:
            return
        
        # Try cache first
        if self._cache and self._cache_key:
            cached = self._cache.get(self._cache_key)
            if cached:
                self._index_by_name = {e.name: e for e in cached}
                self._index_loaded = True
                return
        
        # Empty index case
        if self.index_length == 0:
            self._index_by_name = {}
            self._index_loaded = True
            return
        
        # Load from S3
        raw = self._range_get(self.index_start, self.index_length)
        idx = {}
        p = 0
        
        while p < len(raw):
            # Read name_length (2 bytes)
            if p + 2 > len(raw):
                raise ValueError("Corrupted index: incomplete name length")
            (name_len,) = struct.unpack("<H", raw[p : p + 2])
            p += 2
            
            # Read name
            if p + name_len > len(raw):
                raise ValueError("Corrupted index: incomplete name")
            name = raw[p : p + name_len].decode("utf-8")
            p += name_len
            
            # Read fixed fields
            if p + ENTRY_FIXED_SIZE > len(raw):
                raise ValueError("Corrupted index: incomplete entry")
            fixed = raw[p : p + ENTRY_FIXED_SIZE]
            (
                data_offset,
                data_length,
                meta_offset,
                meta_length,
                flags,
            ) = ENTRY_FIXED_STRUCT.unpack(fixed)
            p += ENTRY_FIXED_SIZE
            
            idx[name] = IndexEntry(
                name=name,
                data_offset=data_offset,
                data_length=data_length,
                meta_offset=meta_offset,
                meta_length=meta_length,
                flags=flags,
            )
        
        self._index_by_name = idx
        self._index_loaded = True
        
        # Store in cache
        if self._cache and self._cache_key:
            self._cache.set(self._cache_key, list(idx.values()))
    
    def _default_cache_key(self) -> str:
        """Generate default cache key from bucket/key/etag."""
        return f"{self.bucket}/{self.key}/{self._etag}"
    
    def _entries_for_names(self, names: List[str]) -> List[IndexEntry]:
        """
        Filter index entries by names.
        
        Args:
            names: List of filenames
        
        Returns:
            List of IndexEntry (sorted by data_offset)
        """
        entries = []
        for name in names:
            entry = self._index_by_name.get(name)
            if entry:
                entries.append(entry)
        
        # Sort by data_offset for efficient batching
        return sorted(entries, key=lambda e: e.data_offset)
    
    def _group_entries(
        self, 
        entries: List[IndexEntry], 
        max_gap_size: int
    ) -> List[List[IndexEntry]]:
        """
        Group adjacent entries for batch Range GET.
        
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
    
    def _fetch_batches(
        self, 
        batches: List[List[IndexEntry]]
    ) -> Dict[str, bytes]:
        """
        Fetch multiple batches via Range GET.
        
        Each batch is fetched as single Range GET, then split by offsets.
        
        Args:
            batches: List of entry groups
        
        Returns:
            Dict mapping filename to content
        """
        results = {}
        
        for batch in batches:
            if not batch:
                continue
            
            # Calculate range for entire batch
            first = batch[0]
            last = batch[-1]
            start_offset = first.data_offset
            end_offset = last.data_offset + last.data_length
            batch_length = end_offset - start_offset
            
            # Fetch entire batch
            batch_data = self._range_get(start_offset, batch_length)
            
            # Split by individual files
            for entry in batch:
                file_start = entry.data_offset - start_offset
                file_end = file_start + entry.data_length
                results[entry.name] = batch_data[file_start:file_end]
        
        return results