"""
DesWriter with support for external big files.
"""
import io
import json
import os
import re
import struct
from typing import BinaryIO, List, Optional

from des.core.constants import *
from des.core.models import IndexEntry, ExternalFileInfo


class DesWriter:
    """
    Creates new DES archive with optional external storage for big files.
    
    External files are stored in S3 under: {s3_prefix}/_bigFiles/{filename}
    """
    
    def __init__(
        self,
        path: str,
        big_file_threshold: int = DEFAULT_BIG_FILE_THRESHOLD,
        s3_client=None,
        bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
    ):
        """
        Args:
            path: Local file path for DES archive (e.g. "/tmp/shard_00.des")
            big_file_threshold: Files >= this size go to external storage (bytes)
            s3_client: Optional boto3 S3 client for external files
            bucket: S3 bucket name for external files
            s3_prefix: S3 prefix for this archive (e.g. "2025-01-15/shard_00")
                      External files will be: {bucket}/{s3_prefix}/_bigFiles/{name}
        """
        self.path = path
        self.big_file_threshold = big_file_threshold
        
        # External storage config
        self.s3_client = s3_client
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        
        # Validate external storage setup
        has_s3 = s3_client is not None
        has_bucket = bucket is not None
        has_prefix = s3_prefix is not None
        
        if has_s3 or has_bucket or has_prefix:
            # If any external storage param is set, all must be set
            if not (has_s3 and has_bucket and has_prefix):
                raise ValueError(
                    "External storage requires all of: s3_client, bucket, s3_prefix"
                )
        
        self._external_storage_enabled = has_s3 and has_bucket and has_prefix
        
        if os.path.exists(path):
            raise FileExistsError(f"File already exists: {path}")
        
        # Ensure directory exists
        dir_path = os.path.dirname(path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        self._f: BinaryIO = open(path, "wb")
        
        # Write HEADER
        self._f.write(HEADER_STRUCT.pack(HEADER_MAGIC, VERSION, b"\0" * 7))
        self.data_start = self._f.tell()
        
        # State
        self._entries: List[IndexEntry] = []
        self._meta_buf = io.BytesIO()
        self._closed = False
        
        # External files tracking
        self._external_files: List[ExternalFileInfo] = []
    
    def add_file(
        self,
        name: str,
        data: bytes,
        meta: Optional[dict] = None,
        force_external: bool = False,
    ):
        """
        Add file to DES archive.
        
        Args:
            name: File name (should be SnowFlake ID)
            data: File content (bytes)
            meta: Optional metadata dict (will be JSON-serialized)
            force_external: Force external storage regardless of size
        
        Raises:
            RuntimeError: If writer is closed
            TypeError: If data is not bytes
            ValueError: If name is invalid
        """
        if self._closed:
            raise RuntimeError("Writer is already closed")
        
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes-like")
        
        # Validate filename
        self._validate_filename(name)
        
        data_length = len(data)
        flags = 0
        
        # Decision: internal or external?
        should_externalize = (
            force_external or data_length >= self.big_file_threshold
        ) and self._external_storage_enabled
        
        if should_externalize:
            # Big file → external storage
            self._upload_external_file(name, data)
            flags |= FLAG_IS_EXTERNAL
            
            # In DES: no data, only metadata
            data_offset = 0  # Not used for external files
            
        else:
            # Normal file → internal storage
            data_offset = self._f.tell()
            self._f.write(data)
        
        # META (always in DES, even for external files)
        meta_dict = meta or {}
        meta_dict['size'] = data_length
        
        if should_externalize:
            meta_dict['is_external'] = True
            if self.s3_prefix:
                meta_dict['external_key'] = f"{self.s3_prefix}/{EXTERNAL_FILES_FOLDER}/{name}"
        
        meta_bytes = json.dumps(meta_dict, separators=(",", ":")).encode("utf-8")
        
        if len(meta_bytes) > MAX_META_SIZE:
            raise ValueError(f"Metadata too large: {len(meta_bytes)} bytes (max {MAX_META_SIZE})")
        
        meta_offset = self._meta_buf.tell()
        self._meta_buf.write(meta_bytes)
        meta_length = len(meta_bytes)
        
        # Create index entry
        entry = IndexEntry(
            name=name,
            data_offset=data_offset,
            data_length=data_length,
            meta_offset=meta_offset,
            meta_length=meta_length,
            flags=flags,
        )
        self._entries.append(entry)
    
    def _validate_filename(self, name: str):
        """
        Validate filename (SnowFlake ID format expected).
        
        Valid format: PREFIX_YYYYMMDD_XXXXXXXXXXXX_CC
        Example: IMG_20250115_1A2B3C4D5E6F_01
        """
        if not name:
            raise ValueError("Filename cannot be empty")
        
        # Check byte length
        name_bytes = name.encode("utf-8")
        if len(name_bytes) > MAX_FILENAME_LENGTH:
            raise ValueError(f"Filename too long: {len(name_bytes)} bytes (max {MAX_FILENAME_LENGTH})")
        
        # Check for invalid characters (S3 best practices)
        # Allow: alphanumeric, underscore, dash, period
        if not re.match(r'^[a-zA-Z0-9_\-\.]+$', name):
            raise ValueError(
                f"Invalid filename: {name!r} (allowed: alphanumeric, _, -, .)"
            )
        
        # Warn if doesn't look like SnowFlake ID (but don't fail)
        # Expected pattern: PREFIX_YYYYMMDD_XXXXXXXXXXXX_CC
        if not re.match(r'^[A-Z]+_\d{8}_[A-F0-9]{12}_\d{2}$', name):
            # This is just a warning - allow other formats
            pass
    
    def _upload_external_file(self, name: str, data: bytes):
        """
        Upload file to S3 external storage.
        
        Args:
            name: Filename
            data: File content
        
        Raises:
            RuntimeError: If external storage not configured
        """
        if not self._external_storage_enabled:
            raise RuntimeError("External storage not configured")
        
        # S3 key: {s3_prefix}/_bigFiles/{name}
        external_key = f"{self.s3_prefix}/{EXTERNAL_FILES_FOLDER}/{name}"
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=external_key,
                Body=data,
                Metadata={
                    'original_name': name,
                    'size': str(len(data)),
                    'source': 'des-writer',
                }
            )
            
            # Track uploaded file
            self._external_files.append(
                ExternalFileInfo(
                    name=name,
                    s3_key=external_key,
                    size_bytes=len(data),
                )
            )
            
        except Exception as e:
            raise RuntimeError(f"Failed to upload external file {name}: {e}") from e
    
    def get_external_files(self) -> List[ExternalFileInfo]:
        """
        Get list of uploaded external files.
        
        Returns:
            List of ExternalFileInfo with name, s3_key, size
        """
        return list(self._external_files)
    
    def get_stats(self) -> dict:
        """
        Get current writer statistics (before close).
        
        Returns:
            Dict with files count, sizes, etc.
        """
        internal_count = 0
        external_count = 0
        internal_size = 0
        external_size = 0
        
        for entry in self._entries:
            if entry.flags & FLAG_IS_EXTERNAL:
                external_count += 1
                external_size += entry.data_length
            else:
                internal_count += 1
                internal_size += entry.data_length
        
        return {
            'total_files': len(self._entries),
            'internal_files': internal_count,
            'external_files': external_count,
            'internal_size_bytes': internal_size,
            'external_size_bytes': external_size,
        }
    
    def close(self):
        """
        Finalize DES archive.
        
        Writes META region, INDEX region, and FOOTER.
        """
        if self._closed:
            return
        
        # Close DATA region
        self._f.flush()
        data_end = self._f.tell()
        data_length = data_end - self.data_start
        
        # Write META REGION
        meta_start = self._f.tell()
        meta_bytes = self._meta_buf.getvalue()
        self._f.write(meta_bytes)
        meta_length = len(meta_bytes)
        
        # Convert relative meta_offset to absolute
        for entry in self._entries:
            entry.meta_offset = meta_start + entry.meta_offset
        
        # Write INDEX REGION
        index_start = self._f.tell()
        for entry in self._entries:
            name_bytes = entry.name.encode("utf-8")
            
            # Write: name_length(2) + name + fixed_fields(44)
            self._f.write(struct.pack("<H", len(name_bytes)))
            self._f.write(name_bytes)
            self._f.write(
                ENTRY_FIXED_STRUCT.pack(
                    entry.data_offset,
                    entry.data_length,
                    entry.meta_offset,
                    entry.meta_length,
                    entry.flags,
                )
            )
        
        index_length = self._f.tell() - index_start
        file_count = len(self._entries)
        
        # Write FOOTER
        footer = FOOTER_STRUCT.pack(
            FOOTER_MAGIC,
            VERSION,
            b"\0" * 7,  # reserved
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
        
        # Print summary
        stats = self.get_stats()
        print(f"✓ DES archive created: {self.path}")
        print(f"  Total files: {stats['total_files']}")
        print(f"  Internal: {stats['internal_files']} ({stats['internal_size_bytes']:,} bytes)")
        print(f"  External: {stats['external_files']} ({stats['external_size_bytes']:,} bytes)")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._closed:
            self.close()
        return False