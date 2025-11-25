import io
import json
import os
import struct
import gzip
from dataclasses import dataclass
from typing import BinaryIO, Dict, List, Optional

# --- Stałe formatu ---

HEADER_MAGIC = b"DESHEAD1"
FOOTER_MAGIC = b"DESFOOT1"
VERSION = 1

HEADER_STRUCT = struct.Struct("<8sB7s")          # magic, version, reserved
FOOTER_STRUCT = struct.Struct("<8sB7sQQQQQQQ")   # magic, version, reserved,
                                                 # data_start, data_length,
                                                 # meta_start, meta_length,
                                                 # index_start, index_length,
                                                 # file_count
ENTRY_FIXED_STRUCT = struct.Struct("<QQQQI")     # data_offset, data_length,
                                                 # meta_offset, meta_length,
                                                 # flags

FOOTER_SIZE = FOOTER_STRUCT.size  # 72 B


class IndexCacheBackend:
    """
    Minimal adapter dla cache indeksu.
    Implementacje powinny zwracać listę IndexEntry lub None, gdy wpisu brak.
    """

    def get(self, key: str) -> Optional[List["IndexEntry"]]:
        raise NotImplementedError

    def set(self, key: str, entries: List["IndexEntry"]) -> None:
        raise NotImplementedError


class InMemoryIndexCache(IndexCacheBackend):
    """
    Prosty cache w pamięci procesu.
    """

    def __init__(self, compress: bool = False):
        self._store: Dict[str, bytes] = {}
        self.compress = compress

    def get(self, key: str) -> Optional[List["IndexEntry"]]:
        raw = self._store.get(key)
        if raw is None:
            return None
        return _decode_index_cache(raw, compressed=self.compress)

    def set(self, key: str, entries: List["IndexEntry"]) -> None:
        self._store[key] = _encode_index_cache(entries, compress=self.compress)


class RedisIndexCache(IndexCacheBackend):
    """
    Adapter pod klienta Redis (np. redis-py) – zależność opcjonalna.
    Wartość trzymana jako JSON (lista słowników).
    """

    def __init__(self, client, ttl_seconds: Optional[int] = None, compress: bool = True):
        self.client = client
        self.ttl_seconds = ttl_seconds
        self.compress = compress

    def get(self, key: str) -> Optional[List["IndexEntry"]]:
        raw = self.client.get(key)
        if not raw:
            return None
        return _decode_index_cache(raw, compressed=self.compress)

    def set(self, key: str, entries: List["IndexEntry"]) -> None:
        payload = _encode_index_cache(entries, compress=self.compress)
        if self.ttl_seconds:
            self.client.set(key, payload, ex=self.ttl_seconds)
        else:
            self.client.set(key, payload)


@dataclass
class IndexEntry:
    name: str
    data_offset: int
    data_length: int
    meta_offset: int
    meta_length: int
    flags: int = 0


def _serialize_index_entries(entries: List[IndexEntry]) -> List[dict]:
    return [
        {
            "name": e.name,
            "data_offset": e.data_offset,
            "data_length": e.data_length,
            "meta_offset": e.meta_offset,
            "meta_length": e.meta_length,
            "flags": e.flags,
        }
        for e in entries
    ]


def _deserialize_index_entries(items: List[dict]) -> List[IndexEntry]:
    result = []
    for d in items or []:
        try:
            result.append(
                IndexEntry(
                    name=d["name"],
                    data_offset=int(d["data_offset"]),
                    data_length=int(d["data_length"]),
                    meta_offset=int(d["meta_offset"]),
                    meta_length=int(d["meta_length"]),
                    flags=int(d.get("flags", 0)),
                )
            )
        except KeyError:
            continue
    return result


def _encode_index_cache(entries: List[IndexEntry], compress: bool) -> bytes:
    payload = json.dumps(_serialize_index_entries(entries), separators=(",", ":")).encode("utf-8")
    return gzip.compress(payload) if compress else payload


def _decode_index_cache(raw: bytes | str, compressed: bool) -> Optional[List[IndexEntry]]:
    if raw is None:
        return None
    if isinstance(raw, str):
        raw_bytes = raw.encode("utf-8")
    else:
        raw_bytes = raw
    if compressed:
        try:
            raw_bytes = gzip.decompress(raw_bytes)
        except Exception:
            return None
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        return None
    return _deserialize_index_entries(payload)


# --- Writer ---

class DesWriter:
    """
    Tworzy nowy plik DES w trybie 'append' na świeżo.
    Założenie v1: używany tylko do zbudowania pliku od zera (dzienny batch).
    """

    def __init__(self, path: str):
        self.path = path
        # zabezpieczenie przed przypadkowym nadpisaniem
        if os.path.exists(path):
            raise FileExistsError(f"File already exists: {path}")

        self._f: BinaryIO = open(path, "wb")
        # HEADER
        self._f.write(HEADER_STRUCT.pack(HEADER_MAGIC, VERSION, b"\0" * 7))
        self.data_start = self._f.tell()
        self._entries: List[IndexEntry] = []

        # meta bufor w pamięci
        self._meta_buf = io.BytesIO()
        self._closed = False

    def add_file(self, name: str, data: bytes, meta: Optional[dict] = None):
        if self._closed:
            raise RuntimeError("Writer is already closed")

        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes-like")

        # DATA
        data_offset = self._f.tell()
        self._f.write(data)
        data_length = len(data)

        # META (JSON -> bytes)
        meta_dict = meta or {}
        meta_bytes = json.dumps(meta_dict, separators=(",", ":")).encode("utf-8")
        meta_offset = self._meta_buf.tell()
        self._meta_buf.write(meta_bytes)
        meta_length = len(meta_bytes)

        entry = IndexEntry(
            name=name,
            data_offset=data_offset,
            data_length=data_length,
            meta_offset=meta_offset,   # na razie w buforze; potem przesuniemy
            meta_length=meta_length,
            flags=0,
        )
        self._entries.append(entry)

    def close(self):
        if self._closed:
            return

        # domykamy DATA
        self._f.flush()
        data_end = self._f.tell()
        data_length = data_end - self.data_start

        # zapis META REGION
        meta_start = self._f.tell()
        meta_bytes = self._meta_buf.getvalue()
        self._f.write(meta_bytes)
        meta_length = len(meta_bytes)

        # przeliczenie meta_offset na absolutne (w pliku)
        for e in self._entries:
            e.meta_offset = meta_start + e.meta_offset

        # zapis INDEX (w META REGION, ale po meta content)
        index_start = self._f.tell()
        for e in self._entries:
            name_bytes = e.name.encode("utf-8")
            if len(name_bytes) > 65535:
                raise ValueError("Filename too long")
            # u16 length
            self._f.write(struct.pack("<H", len(name_bytes)))
            self._f.write(name_bytes)
            self._f.write(
                ENTRY_FIXED_STRUCT.pack(
                    e.data_offset,
                    e.data_length,
                    e.meta_offset,
                    e.meta_length,
                    e.flags,
                )
            )
        index_length = self._f.tell() - index_start

        file_count = len(self._entries)

        # FOOTER
        footer = FOOTER_STRUCT.pack(
            FOOTER_MAGIC,
            VERSION,
            b"\0" * 7,
            self.data_start,
            data_length,
            meta_start,
            meta_length,
            index_start,
            index_length,
            file_count,
        )
        self._f.write(footer)
        self._f.flush()
        self._f.close()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


# --- Reader ---

class DesReader:
    """
    Reader do plików DES.
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
