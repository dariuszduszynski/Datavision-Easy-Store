import json
import struct
from typing import Dict, Optional, Sequence

import boto3

from dv_easystore import (
    ENTRY_FIXED_STRUCT,
    FOOTER_SIZE,
    FOOTER_STRUCT,
    IndexCacheBackend,
    IndexEntry,
    VERSION,
)


class S3DesReader:
    """
    DES reader operating directly on S3 via Range GET.
    No local temp files. Pure streaming.
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        s3_client=None,
        cache: Optional[IndexCacheBackend] = None,
        cache_key: Optional[str] = None,
    ):
        self.bucket = bucket
        self.key = key
        self.s3 = s3_client or boto3.client("s3")
        self._cache = cache

        head = self._get_head()
        self.file_size = head["ContentLength"]
        self._etag = head.get("ETag") or ""

        footer_bytes = self._range_get(self.file_size - FOOTER_SIZE, FOOTER_SIZE)
        self._parse_footer(footer_bytes)

        self._index_loaded = False
        self._index_by_name: Dict[str, IndexEntry] = {}
        self._cache_key = cache_key or self._default_cache_key()

    # ----------------------------------------------------------
    # S3 helpers
    # ----------------------------------------------------------

    def _get_head(self) -> dict:
        return self.s3.head_object(Bucket=self.bucket, Key=self.key)

    def _range_get(self, offset: int, length: int) -> bytes:
        end = offset + length - 1
        resp = self.s3.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range=f"bytes={offset}-{end}",
        )
        return resp["Body"].read()

    def get_files_batch(
        self,
        names: Sequence[str],
        max_gap_size: int = 1024 * 1024,
    ) -> Dict[str, bytes]:
        """
        Fetch multiple files with minimal S3 requests.
        Files that are close to each other (gap <= max_gap_size) are fetched in one range.
        Missing names are ignored.
        """
        if max_gap_size < 0:
            raise ValueError("max_gap_size must be non-negative")

        self._load_index()

        seen = set()
        unique_names = []
        for name in names:
            if name not in seen:
                unique_names.append(name)
                seen.add(name)

        entries = [self._index_by_name.get(name) for name in unique_names]
        entries = [e for e in entries if e is not None]
        if not entries:
            return {}

        entries.sort(key=lambda e: e.data_offset)

        batches = []
        current_batch = [entries[0]]
        for entry in entries[1:]:
            prev = current_batch[-1]
            prev_end = prev.data_offset + prev.data_length
            gap = entry.data_offset - prev_end
            if gap <= max_gap_size:
                current_batch.append(entry)
            else:
                batches.append(current_batch)
                current_batch = [entry]
        batches.append(current_batch)

        results: Dict[str, bytes] = {}
        for batch in batches:
            start = batch[0].data_offset
            end = batch[-1].data_offset + batch[-1].data_length
            blob = self._range_get(start, end - start)
            for entry in batch:
                rel_start = entry.data_offset - start
                rel_end = rel_start + entry.data_length
                results[entry.name] = blob[rel_start:rel_end]

        return results

    # ----------------------------------------------------------
    # Footer + index
    # ----------------------------------------------------------

    def _parse_footer(self, data: bytes):
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

        if magic != b"DESFOOT1":
            raise ValueError("Invalid DES footer magic")
        if version != VERSION:
            raise ValueError(f"Unsupported DES version: {version}")

    def _default_cache_key(self) -> str:
        return f"DES_S3:{self.bucket}:{self.key}:{self.file_size}:{self._etag}:{VERSION}"

    def _load_index(self):
        if self._index_loaded:
            return

        if self._cache and self._cache_key:
            cached = self._cache.get(self._cache_key)
            if cached:
                self._index_by_name = {e.name: e for e in cached}
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

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def list_files(self):
        self._load_index()
        return list(self._index_by_name.keys())

    def get_file(self, name: str) -> bytes:
        self._load_index()
        entry = self._index_by_name.get(name)
        if not entry:
            raise KeyError(name)
        return self._range_get(entry.data_offset, entry.data_length)

    def get_meta(self, name: str) -> dict:
        self._load_index()
        entry = self._index_by_name.get(name)
        if not entry:
            raise KeyError(name)

        raw = self._range_get(entry.meta_offset, entry.meta_length)
        return json.loads(raw.decode("utf-8"))

    def get_index(self):
        self._load_index()
        return list(self._index_by_name.values())

    def __contains__(self, name):
        self._load_index()
        return name in self._index_by_name
