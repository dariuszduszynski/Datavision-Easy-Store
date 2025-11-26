"""Cache backends for DES index storage."""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from threading import Lock
from typing import Any, Dict, List, Optional

from des.core.models import IndexEntry

__all__ = ["IndexCacheBackend", "InMemoryIndexCache", "RedisIndexCache", "NullCache"]


class IndexCacheBackend(ABC):
    """
    Abstract base class for index cache backends.

    Cache stores List[IndexEntry] under a string key (e.g. "bucket/key/etag").
    """

    @abstractmethod
    def get(self, key: str) -> Optional[List[IndexEntry]]:
        """
        Retrieve cached index entries.

        Args:
            key: Cache key (e.g. "my-bucket/2025-01-15/shard_00.des/etag123")

        Returns:
            List of IndexEntry or None if not found/expired
        """
        pass

    @abstractmethod
    def set(self, key: str, entries: List[IndexEntry], ttl: Optional[int] = None) -> None:
        """
        Store index entries in cache.

        Args:
            key: Cache key
            entries: List of IndexEntry to cache
            ttl: Optional TTL in seconds (None = no expiration)
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete cache entry."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all cache entries."""
        pass


class InMemoryIndexCache(IndexCacheBackend):
    """
    Thread-safe in-memory cache for index entries.

    Features:
    - TTL support with lazy expiration check
    - Thread-safe operations
    - LRU eviction (optional, via max_size)
    """

    def __init__(
        self, max_size: Optional[int] = None, default_ttl: Optional[int] = None
    ) -> None:
        """
        Args:
            max_size: Maximum number of cache entries (None = unlimited)
            default_ttl: Default TTL in seconds (None = no expiration)
        """
        self._cache: Dict[str, tuple[List[IndexEntry], Optional[float]]] = {}
        self._lock = Lock()
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._access_order: List[str] = []  # For LRU

    def get(self, key: str) -> Optional[List[IndexEntry]]:
        """Thread-safe get with TTL check."""
        with self._lock:
            if key not in self._cache:
                return None

            entries, expiry = self._cache[key]

            # Check expiration
            if expiry is not None and time.time() > expiry:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)
                return None

            # Update LRU order
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)

            return entries

    def set(self, key: str, entries: List[IndexEntry], ttl: Optional[int] = None) -> None:
        """Thread-safe set with TTL and LRU eviction."""
        with self._lock:
            # Calculate expiry
            effective_ttl = ttl if ttl is not None else self.default_ttl
            expiry = time.time() + effective_ttl if effective_ttl else None

            # LRU eviction if needed
            if self.max_size and len(self._cache) >= self.max_size:
                if key not in self._cache:  # Don't evict if updating existing
                    oldest_key = self._access_order.pop(0)
                    del self._cache[oldest_key]

            # Store
            self._cache[key] = (entries, expiry)

            # Update LRU order
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)

    def delete(self, key: str) -> None:
        """Thread-safe delete."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            if key in self._access_order:
                self._access_order.remove(key)

    def clear(self) -> None:
        """Thread-safe clear all."""
        with self._lock:
            self._cache.clear()
            self._access_order.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = len(self._cache)
            expired = 0
            now = time.time()

            for _, expiry in self._cache.values():
                if expiry and now > expiry:
                    expired += 1

            return {
                "entries": total,
                "expired": expired,
                "active": total - expired,
                "max_size": self.max_size,
            }


class RedisIndexCache(IndexCacheBackend):
    """
    Redis-backed cache for index entries.

    Features:
    - JSON serialization of IndexEntry list
    - Native Redis TTL
    - Optional key prefix for namespacing

    Requires: redis-py (pip install redis)
    """

    def __init__(
        self,
        redis_client: Any,
        key_prefix: str = "des:index:",
        default_ttl: Optional[int] = None,
    ) -> None:
        """
        Args:
            redis_client: Redis client instance (redis.Redis)
            key_prefix: Prefix for all cache keys (namespacing)
            default_ttl: Default TTL in seconds (None = no expiration)
        """
        self.redis = redis_client
        self.key_prefix = key_prefix
        self.default_ttl = default_ttl

    def _make_key(self, key: str) -> str:
        """Generate prefixed Redis key."""
        return f"{self.key_prefix}{key}"

    def _serialize(self, entries: List[IndexEntry]) -> str:
        """Serialize IndexEntry list to JSON."""
        data = [
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
        return json.dumps(data, separators=(",", ":"))

    def _deserialize(self, raw: str) -> List[IndexEntry]:
        """Deserialize JSON to IndexEntry list."""
        data = json.loads(raw)
        return [
            IndexEntry(
                name=item["name"],
                data_offset=item["data_offset"],
                data_length=item["data_length"],
                meta_offset=item["meta_offset"],
                meta_length=item["meta_length"],
                flags=item["flags"],
            )
            for item in data
        ]

    def get(self, key: str) -> Optional[List[IndexEntry]]:
        """Get from Redis with deserialization."""
        redis_key = self._make_key(key)
        raw = self.redis.get(redis_key)

        if raw is None:
            return None

        try:
            return self._deserialize(raw.decode("utf-8"))
        except (json.JSONDecodeError, KeyError, ValueError):
            # Corrupted cache entry
            self.redis.delete(redis_key)
            return None

    def set(self, key: str, entries: List[IndexEntry], ttl: Optional[int] = None) -> None:
        """Set to Redis with serialization and TTL."""
        redis_key = self._make_key(key)
        serialized = self._serialize(entries)

        effective_ttl = ttl if ttl is not None else self.default_ttl

        if effective_ttl:
            self.redis.setex(redis_key, effective_ttl, serialized)
        else:
            self.redis.set(redis_key, serialized)

    def delete(self, key: str) -> None:
        """Delete from Redis."""
        redis_key = self._make_key(key)
        self.redis.delete(redis_key)

    def clear(self) -> None:
        """Clear all keys with prefix (use with caution!)."""
        pattern = f"{self.key_prefix}*"
        cursor = 0

        while True:
            cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
            if keys:
                self.redis.delete(*keys)
            if cursor == 0:
                break

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics (approximate)."""
        pattern = f"{self.key_prefix}*"
        cursor = 0
        count = 0

        while True:
            cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
            count += len(keys)
            if cursor == 0:
                break

        return {
            "entries": count,
            "prefix": self.key_prefix,
        }


class NullCache(IndexCacheBackend):
    """
    No-op cache backend (always misses).

    Useful for testing or disabling cache without changing code.
    """

    def get(self, key: str) -> Optional[List[IndexEntry]]:
        return None

    def set(self, key: str, entries: List[IndexEntry], ttl: Optional[int] = None) -> None:
        pass

    def delete(self, key: str) -> None:
        pass

    def clear(self) -> None:
        pass

    def get_stats(self) -> Dict[str, Any]:
        return {"type": "null", "enabled": False}
