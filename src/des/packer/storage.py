"""Storage backends for DES packer."""

from __future__ import annotations

import asyncio
from typing import Any, Optional


class S3StorageBackend:
    """Simple async wrapper around boto3 S3 uploads."""

    def __init__(
        self, s3_client: Any, bucket: str, prefix: Optional[str] = None
    ) -> None:
        self.s3 = s3_client
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") if prefix else None

    def _full_key(self, key: str) -> str:
        """Build destination key with optional prefix."""
        if self.prefix:
            return f"{self.prefix}/{key.lstrip('/')}"
        return key

    async def upload(self, local_path: str, dest_key: str) -> None:
        """Upload file to S3."""
        key = self._full_key(dest_key)
        await asyncio.to_thread(self.s3.upload_file, local_path, self.bucket, key)
