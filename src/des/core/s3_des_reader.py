"""
S3DesReader with support for external big files.
"""
import json
import struct
from typing import Dict, List, Optional, Sequence

import boto3

from des.core.constants import *
from des.core.models import IndexEntry


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
        self.bucket = bucket
        self.key = key
        self.s3 = s3_client or boto3.client("s3")
        self._cache = cache
        
        # Parse base path for external files
        # key: "2025-01-15/shard_00.des" → base_prefix: "2025-01-15"
        self.base_prefix = "/".join(key.split("/")[:-1])
        
        head = self._get_head()
        self.file_size = head["ContentLength"]
        self._etag = head.get("ETag") or ""
        
        if self.file_size < FOOTER_SIZE:
            raise ValueError("Object too small to be a valid DES file")
        
        footer_bytes = self._range_get(self.file_size - FOOTER_SIZE, FOOTER_SIZE)
        self._parse_footer(footer_bytes)
        
        self._index_loaded = False
        self._index_by_name: Dict[str, IndexEntry] = {}
        self._cache_key = cache_key or self._default_cache_key()
    
    def _get_head(self) -> dict:
        return self.s3.head_object(Bucket=self.bucket, Key=self.key)
    
    def _range_get(self, offset: int, length: int) -> bytes:
        """Standard Range GET from DES archive."""
        end = offset + length - 1
        resp = self.s3.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range=f"bytes={offset}-{end}",
        )
        return resp["Body"].read()
    
    def _external_get(self, name: str) -> bytes:
        """
        GET całego pliku z _bigFiles/.
        """
        external_key = f"{self.base_prefix}/_bigFiles/{name}"
        
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
        Pobierz plik z DES lub external storage.
        """
        self._load_index()
        entry = self._index_by_name.get(name)
        if not entry:
            raise KeyError(f"File not found: {name}")
        
        # Sprawdzamy flagę (SZYBKA ŚCIEŻKA)
        if entry.flags & FLAG_IS_EXTERNAL:
            # External file
            return self._external_get(entry.name)
        
        # Internal file - standardowy Range GET
        return self._range_get(entry.data_offset, entry.data_length)
    
    def get_files_batch(
        self,
        names: Sequence[str],
        max_gap_size: int = 1024 * 1024,
    ) -> Dict[str, bytes]:
        """
        Batch GET z obsługą external files.
        """
        if isinstance(names, str):
            raise TypeError("names must be a sequence of file names")
        
        self._load_index()
        
        # Rozdziel na internal i external
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
        
        # 1. Fetch internal files (batching z max_gap_size)
        if internal_names:
            entries = self._entries_for_names(internal_names)
            batches = self._group_entries(entries, max_gap_size)
            internal_results = self._fetch_batches(batches)
            results.update(internal_results)
        
        # 2. Fetch external files (pojedynczo, można zrównoleglić)
        for name in external_names:
            try:
                results[name] = self._external_get(name)
            except KeyError:
                # Skip missing external files
                pass
        
        return results
    
    def get_meta(self, name: str) -> dict:
        """
        Pobierz metadane (zawsze z DES, nawet dla external).
        """
        self._load_index()
        entry = self._index_by_name.get(name)
        if not entry:
            raise KeyError(f"File not found: {name}")
        
        raw = self._range_get(entry.meta_offset, entry.meta_length)
        return json.loads(raw.decode("utf-8"))
    
    def list_files(self, include_external: bool = True) -> List[str]:
        """
        Lista wszystkich plików.
        
        Args:
            include_external: Czy uwzględnić external files
        """
        self._load_index()
        
        if include_external:
            return list(self._index_by_name.keys())
        
        # Only internal
        return [
            name for name, entry in self._index_by_name.items()
            if not (entry.flags & FLAG_IS_EXTERNAL)
        ]
    
    def get_stats(self) -> dict:
        """
        Statystyki archiwum.
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
        
        return {
            'total_files': len(self._index_by_name),
            'internal_files': internal_count,
            'external_files': external_count,
            'internal_size_bytes': internal_size,
            'external_size_bytes': external_size,
            'archive_size_bytes': self.file_size,
        }
    
    # ... reszta metod bez zmian (_load_index, etc.)
    
    def _parse_footer(self, data: bytes):
        """Parse footer (bez zmian)."""
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
            raise ValueError("Invalid DES footer magic")
        if version != VERSION:
            raise ValueError(f"Unsupported DES version: {version}")
    
    def _load_index(self):
        """Load index z cache lub S3 (bez zmian)."""
        if self._index_loaded:
            return
        
        if self._cache and self._cache_key:
            cached = self._cache.get(self._cache_key)
            if cached:
                self._index_by_name = {e.name: e for e in cached}
                self._index_loaded = True
                return
        
        if self.index_length == 0:
            self._index_by_name = {}
            self._index_loaded = True
            return
        
        raw = self._range_get(self.index_start, self.index_length)
        p = 0
        idx = {}
        
        while p < len(raw):
            (name_len,) = struct.unpack("<H", raw[p : p + 2])
            p += 2
            name = raw[p : p + name_len].decode("utf-8")
            p += name_len
            
            fixed = raw[p : p + ENTRY_FIXED_STRUCT.size]
            (
                data_offset,
                data_length,
                meta_offset,
                meta_length,
                flags,
            ) = ENTRY_FIXED_STRUCT.unpack(fixed)
            p += ENTRY_FIXED_STRUCT.size
            
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
        
        if self._cache and self._cache_key:
            self._cache.set(self._cache_key, list(idx.values()))