from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import Any, Tuple

from des.assignment.hash_routing import consistent_hash
from des.config.retriever_config import RetrieverConfig
from des.core.cache import IndexCacheBackend
from des.core.s3_des_reader import S3DesReader
from des.utils.logging import get_logger

logger = get_logger(__name__)


class FileHandler:
    """Handles file retrieval using S3DesReader."""

    def __init__(
        self,
        s3_client: Any,
        config: RetrieverConfig,
        cache_backend: IndexCacheBackend,
    ):
        self.s3_client = s3_client
        self.config = config
        self.cache = cache_backend

    def compute_shard_id(self, file_name: str) -> int:
        return consistent_hash(file_name, self.config.shard_bits)

    def _parse_day(self, file_name: str) -> str:
        match = re.search(r"_(\d{8})_", file_name)
        if not match:
            raise ValueError("Invalid DES filename format")
        day_raw = match.group(1)
        dt = datetime.strptime(day_raw, "%Y%m%d")
        return dt.strftime("%Y-%m-%d")

    def get_container_key(self, file_name: str) -> str:
        day_part = self._parse_day(file_name)
        shard_id = self.compute_shard_id(file_name)
        shard_label = f"{shard_id:02d}"
        key = f"{day_part}/shard_{shard_label}.des"
        if self.config.s3_prefix:
            prefix = self.config.s3_prefix.rstrip("/")
            return f"{prefix}/{key}"
        return key

    async def get_file(self, file_name: str) -> Tuple[bytes, dict]:
        key = self.get_container_key(file_name)
        reader = await asyncio.to_thread(
            S3DesReader,
            self.config.s3_bucket,
            key,
            self.s3_client,
            self.cache,
            f"{self.config.s3_bucket}/{key}",
        )
        try:
            content = await asyncio.to_thread(reader.get_file, file_name)
            meta = await asyncio.to_thread(reader.get_meta, file_name)
        except KeyError as exc:
            raise FileNotFoundError(str(exc)) from exc

        exists = True
        info = {
            "container": key,
            "shard_id": self.compute_shard_id(file_name),
            "meta": meta,
            "size": len(content),
            "exists": exists,
            "is_external": bool(meta.get("is_external") or meta.get("external")),
        }
        return content, info

    async def file_exists(self, file_name: str) -> Tuple[bool, dict]:
        key = self.get_container_key(file_name)
        reader = await asyncio.to_thread(
            S3DesReader,
            self.config.s3_bucket,
            key,
            self.s3_client,
            self.cache,
            f"{self.config.s3_bucket}/{key}",
        )
        try:
            meta = await asyncio.to_thread(reader.get_meta, file_name)
            size = await asyncio.to_thread(
                lambda: len(reader.get_file(file_name))
            )  # coarse size
            info = {
                "container": key,
                "shard_id": self.compute_shard_id(file_name),
                "meta": meta,
                "size": size,
                "exists": True,
                "is_external": bool(meta.get("is_external") or meta.get("external")),
            }
            return True, info
        except KeyError:
            return False, {"container": key, "exists": False}
