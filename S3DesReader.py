import json
import struct
from typing import Optional
import boto3

from dv_easystore import (
    FOOTER_STRUCT,
    FOOTER_SIZE,
    ENTRY_FIXED_STRUCT,
    IndexEntry,
    IndexCacheBackend,
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

        # Read footer (always last FOOTER_SIZE bytes)
        head = self._get_head()
        self.file_size = head["ContentLength"]
        self._etag = head.get("ETag") or ""

        footer_bytes = self._range_get(
            self.file_size - FOOTER_SIZE, FOOTER_SIZE
        )
        self._parse_footer(footer_bytes)

        self._index_loaded = False
        self._index_by_name = {}
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

    def get_files_batch(self, names: list[str], max_gap_size: int = 1024 * 1024) -> dict[str, bytes]:
        """
        Pobiera wiele plików, optymalizując liczbę zapytań do S3.
        Jeśli pliki leżą blisko siebie (luka < max_gap_size), są pobierane jednym strzałem.
        
        :param names: lista nazw plików do pobrania
        :param max_gap_size: maksymalna wielkość luki (w bajtach), którą jesteśmy w stanie
                             pobrać "na darmo", aby uniknąć kolejnego requestu HTTP.
                             Domyślnie 1MB.
        """
        self._load_index()
        
        # 1. Znajdź wpisy i posortuj je po offsecie w pliku
        entries = []
        for name in names:
            entry = self._index_by_name.get(name)
            if entry:
                entries.append(entry)
            # Opcjonalnie: loguj warning, jeśli plik nie istnieje
        
        if not entries:
            return {}

        # Sortujemy, żeby wykrywać sąsiedztwo
        entries.sort(key=lambda e: e.data_offset)

        results = {}
        batch_start_idx = 0
        
        # 2. Grupuj wpisy w "wsady" (batches)
        while batch_start_idx < len(entries):
            current_batch = [entries[batch_start_idx]]
            
            # Próbujemy dokleić kolejne pliki do tego batcha
            next_idx = batch_start_idx + 1
            while next_idx < len(entries):
                prev_entry = current_batch[-1]
                curr_entry = entries[next_idx]
                
                # Oblicz koniec poprzedniego i początek następnego
                prev_end = prev_entry.data_offset + prev_entry.data_length
                gap = curr_entry.data_offset - prev_end
                
                # Jeśli luka jest akceptowalna, dodajemy do batcha
                if gap <= max_gap_size:
                    current_batch.append(curr_entry)
                    next_idx += 1
                else:
                    # Luka za duża, kończymy ten batch
                    break
            
            # 3. Wykonaj pobranie dla wyznaczonego batcha
            batch_start_offset = current_batch[0].data_offset
            last_entry = current_batch[-1]
            batch_end_offset = last_entry.data_offset + last_entry.data_length
            total_length = batch_end_offset - batch_start_offset
            
            # Jeden duży Range Request
            raw_batch_data = self._range_get(batch_start_offset, total_length)
            
            # 4. Pokrój pobrany blob na poszczególne pliki
            for entry in current_batch:
                # Oblicz relatywny offset wewnątrz pobranego bloku
                rel_start = entry.data_offset - batch_start_offset
                rel_end = rel_start + entry.data_length
                results[entry.name] = raw_batch_data[rel_start:rel_end]
            
            # Przesuń wskaźnik na początek nowej grupy
            batch_start_idx = next_idx

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
