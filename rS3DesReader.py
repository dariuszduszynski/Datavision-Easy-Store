import json
import struct
import boto3

from dv_easystore import (
    FOOTER_STRUCT,
    FOOTER_SIZE,
    ENTRY_FIXED_STRUCT,
    IndexEntry,
)


class S3DesReader:
    """
    DES reader operating directly on S3 via Range GET.
    No local temp files. Pure streaming.
    """

    def __init__(self, bucket: str, key: str, s3_client=None):
        self.bucket = bucket
        self.key = key
        self.s3 = s3_client or boto3.client("s3")

        # Read footer (always last FOOTER_SIZE bytes)
        file_size = self._get_size()
        self.file_size = file_size

        footer_bytes = self._range_get(
            file_size - FOOTER_SIZE, FOOTER_SIZE
        )
        self._parse_footer(footer_bytes)

        self._index_loaded = False
        self._index_by_name = {}

    # ----------------------------------------------------------
    # S3 helpers
    # ----------------------------------------------------------

    def _get_size(self) -> int:
        head = self.s3.head_object(Bucket=self.bucket, Key=self.key)
        return head["ContentLength"]

    def _range_get(self, offset: int, length: int) -> bytes:
        end = offset + length - 1
        resp = self.s3.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range=f"bytes={offset}-{end}",
        )
        return resp["Body"].read()

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

    def _load_index(self):
        if self._index_loaded:
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
