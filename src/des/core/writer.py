"""
DesWriter with support for external big files.
"""
import os
import io
import json
from typing import Optional, BinaryIO
from des.core.constants import *
from des.core.models import IndexEntry


class DesWriter:
    """
    Tworzy nowy plik DES z możliwością przechowywania dużych plików zewnętrznie.
    """
    
    def __init__(
        self, 
        path: str,
        big_file_threshold: int = DEFAULT_BIG_FILE_THRESHOLD,
        external_storage=None  # Opcjonalny S3 client
    ):
        self.path = path
        self.big_file_threshold = big_file_threshold
        self.external_storage = external_storage
        
        if os.path.exists(path):
            raise FileExistsError(f"File already exists: {path}")
        
        self._f: BinaryIO = open(path, "wb")
        
        # HEADER
        self._f.write(HEADER_STRUCT.pack(HEADER_MAGIC, VERSION, b"\0" * 7))
        self.data_start = self._f.tell()
        self._entries: List[IndexEntry] = []
        
        # Meta bufor
        self._meta_buf = io.BytesIO()
        self._closed = False
        
        # External files tracking
        self._external_files: List[str] = []
    
    def add_file(
        self, 
        name: str, 
        data: bytes, 
        meta: Optional[dict] = None,
        force_external: bool = False
    ):
        """
        Dodaj plik do archiwum.
        
        Args:
            name: Nazwa pliku (SnowFlake ID)
            data: Zawartość pliku
            meta: Opcjonalne metadane
            force_external: Wymuś external storage (ignore threshold)
        """
        if self._closed:
            raise RuntimeError("Writer is already closed")
        
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes-like")
        
        data_length = len(data)
        flags = 0
        
        # Decyzja: internal czy external?
        is_big_file = force_external or (data_length >= self.big_file_threshold)
        
        if is_big_file and self.external_storage:
            # Duży plik → external storage
            self._add_external_file(name, data, meta)
            flags |= FLAG_IS_EXTERNAL
            
            # W DES zapisujemy TYLKO metadane, bez danych
            data_offset = 0  # Nie używany dla external
            data_length = len(data)  # Zachowujemy dla info
            
        else:
            # Normalny plik → internal storage
            data_offset = self._f.tell()
            self._f.write(data)
        
        # META (zawsze w DES, nawet dla external)
        meta_dict = meta or {}
        meta_dict['size'] = data_length
        if is_big_file:
            meta_dict['is_external'] = True
        
        meta_bytes = json.dumps(meta_dict, separators=(",", ":")).encode("utf-8")
        meta_offset = self._meta_buf.tell()
        self._meta_buf.write(meta_bytes)
        meta_length = len(meta_bytes)
        
        # Index entry
        entry = IndexEntry(
            name=name,
            data_offset=data_offset,
            data_length=data_length,
            meta_offset=meta_offset,
            meta_length=meta_length,
            flags=flags,
        )
        self._entries.append(entry)
    
    def _add_external_file(self, name: str, data: bytes, meta: Optional[dict]):
        """
        Upload pliku do external storage (_bigFiles/).
        """
        if not self.external_storage:
            raise RuntimeError("External storage not configured")
        
        # Ścieżka: base_path/_bigFiles/filename
        base_path = os.path.dirname(self.path)
        external_key = f"{base_path}/_bigFiles/{name}"
        
        # Upload do S3
        self.external_storage.put_object(
            Bucket=self.external_storage.bucket,
            Key=external_key,
            Body=data,
            Metadata={
                'original_name': name,
                'size': str(len(data)),
                'source': 'des-writer'
            }
        )
        
        self._external_files.append(external_key)
        print(f"✓ Uploaded external file: {external_key} ({len(data)} bytes)")
    
    def close(self):
        """Finalizacja DES archive."""
        if self._closed:
            return
        
        # Domykamy DATA region
        self._f.flush()
        data_end = self._f.tell()
        data_length = data_end - self.data_start
        
        # META REGION
        meta_start = self._f.tell()
        meta_bytes = self._meta_buf.getvalue()
        self._f.write(meta_bytes)
        meta_length = len(meta_bytes)
        
        # Przelicz meta_offset na absolutne
        for e in self._entries:
            e.meta_offset = meta_start + e.meta_offset
        
        # INDEX REGION
        index_start = self._f.tell()
        for e in self._entries:
            name_bytes = e.name.encode("utf-8")
            if len(name_bytes) > 65535:
                raise ValueError(f"Filename too long: {e.name}")
            
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
        
        # Stats
        external_count = len(self._external_files)
        internal_count = file_count - external_count
        print(f"✓ DES archive created: {self.path}")
        print(f"  Internal files: {internal_count}")
        print(f"  External files: {external_count}")
        print(f"  Total files: {file_count}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc, tb):
        self.close()