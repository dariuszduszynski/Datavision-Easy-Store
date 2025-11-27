import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pytest

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core.cache import InMemoryIndexCache, RedisIndexCache  # noqa: E402
from des.core.models import IndexEntry  # noqa: E402


def _entries() -> List[IndexEntry]:
    return [
        IndexEntry(
            name="file1",
            data_offset=0,
            data_length=5,
            meta_offset=100,
            meta_length=10,
            flags=0,
        ),
        IndexEntry(
            name="file2",
            data_offset=5,
            data_length=3,
            meta_offset=110,
            meta_length=12,
            flags=1,
        ),
    ]


@pytest.mark.unit
def test_in_memory_index_cache_round_trip() -> None:
    cache = InMemoryIndexCache()
    key = "k1"
    entries = _entries()

    cache.set(key, entries)
    result = cache.get(key)

    assert result is not None
    assert len(result) == len(entries)
    for a, b in zip(result, entries, strict=False):
        assert a.name == b.name
        assert a.data_offset == b.data_offset
        assert a.data_length == b.data_length
        assert a.meta_offset == b.meta_offset
        assert a.meta_length == b.meta_length
        assert a.flags == b.flags


@pytest.mark.unit
def test_in_memory_index_cache_round_trip_with_ttl() -> None:
    cache = InMemoryIndexCache(default_ttl=1)
    key = "k2"
    entries = _entries()

    cache.set(key, entries)
    result = cache.get(key)
    assert result is not None

    # Expire by sleeping past TTL
    time.sleep(1.1)
    assert cache.get(key) is None


class _FakeRedis:
    def __init__(self):
        self._store: Dict[str, Tuple[bytes, float | None]] = {}

    def set(self, key: str, value: str):
        self._store[key] = (value.encode("utf-8"), None)

    def setex(self, key: str, ttl: int, value: str):
        self._store[key] = (value.encode("utf-8"), time.time() + ttl)

    def get(self, key: str):
        if key not in self._store:
            return None
        val, exp = self._store[key]
        if exp and time.time() > exp:
            del self._store[key]
            return None
        return val

    def delete(self, *keys):
        for k in keys:
            self._store.pop(k, None)

    def scan(self, cursor: int, match: str, count: int = 100):
        # Ignore cursor/count for simplicity
        keys = [k for k in list(self._store.keys()) if k.startswith(match.rstrip("*"))]
        return 0, keys


@pytest.mark.unit
def test_redis_index_cache_round_trip() -> None:
    fake = _FakeRedis()
    cache = RedisIndexCache(fake, key_prefix="test:index:")
    key = "k3"
    entries = _entries()

    cache.set(key, entries)
    result = cache.get(key)

    assert result is not None
    assert len(result) == len(entries)
    for a, b in zip(result, entries, strict=False):
        assert a.name == b.name
        assert a.data_offset == b.data_offset
        assert a.data_length == b.data_length
        assert a.meta_offset == b.meta_offset
        assert a.meta_length == b.meta_length
        assert a.flags == b.flags


@pytest.mark.unit
def test_redis_index_cache_with_ttl() -> None:
    fake = _FakeRedis()
    cache = RedisIndexCache(fake, key_prefix="test:index:", default_ttl=1)
    key = "k4"
    entries = _entries()

    cache.set(key, entries)
    assert cache.get(key) is not None

    time.sleep(1.1)
    assert cache.get(key) is None


@pytest.mark.unit
def test_in_memory_cache_lru_eviction():
    """Test LRU eviction with max_size."""
    cache = InMemoryIndexCache(max_size=3)
    entries = _entries()

    cache.set("k1", entries)
    cache.set("k2", entries)
    cache.set("k3", entries)

    cache.get("k1")  # make k1 most recently used

    cache.set("k4", entries)  # should evict k2 (oldest)

    assert cache.get("k1") is not None
    assert cache.get("k2") is None
    assert cache.get("k3") is not None
    assert cache.get("k4") is not None


@pytest.mark.unit
def test_in_memory_cache_update_existing():
    """Test updating existing cache entry doesn't count toward max_size."""
    cache = InMemoryIndexCache(max_size=2)
    entries1 = _entries()
    entries2 = [
        IndexEntry(
            name="new",
            data_offset=0,
            data_length=1,
            meta_offset=0,
            meta_length=1,
            flags=0,
        )
    ]

    cache.set("k1", entries1)
    cache.set("k2", entries1)

    cache.set("k1", entries2)  # update existing

    assert cache.get("k1") is not None
    assert cache.get("k2") is not None


@pytest.mark.unit
def test_in_memory_cache_delete():
    """Test cache deletion."""
    cache = InMemoryIndexCache()
    entries = _entries()

    cache.set("k1", entries)
    assert cache.get("k1") is not None

    cache.delete("k1")
    assert cache.get("k1") is None

    cache.delete("nonexistent")  # Should not raise


@pytest.mark.unit
def test_in_memory_cache_clear():
    """Test cache clear."""
    cache = InMemoryIndexCache()
    entries = _entries()

    cache.set("k1", entries)
    cache.set("k2", entries)

    cache.clear()

    assert cache.get("k1") is None
    assert cache.get("k2") is None


@pytest.mark.unit
def test_in_memory_cache_stats():
    """Test cache statistics."""
    cache = InMemoryIndexCache(max_size=10, default_ttl=1)
    entries = _entries()

    cache.set("k1", entries)
    cache.set("k2", entries, ttl=None)

    stats = cache.get_stats()
    assert stats["entries"] == 2
    assert stats["max_size"] == 10

    time.sleep(1.1)

    stats = cache.get_stats()
    assert stats["expired"] >= 1


@pytest.mark.unit
def test_redis_cache_delete():
    """Test Redis cache deletion."""
    fake = _FakeRedis()
    cache = RedisIndexCache(fake)
    entries = _entries()

    cache.set("k1", entries)
    assert cache.get("k1") is not None

    cache.delete("k1")
    assert cache.get("k1") is None


@pytest.mark.unit
def test_redis_cache_clear():
    """Test Redis cache clear."""
    fake = _FakeRedis()
    cache = RedisIndexCache(fake, key_prefix="test:")
    entries = _entries()

    cache.set("k1", entries)
    cache.set("k2", entries)

    cache.clear()

    assert cache.get("k1") is None
    assert cache.get("k2") is None


@pytest.mark.unit
def test_redis_cache_stats():
    """Test Redis cache statistics."""
    fake = _FakeRedis()
    cache = RedisIndexCache(fake, key_prefix="test:")
    entries = _entries()

    cache.set("k1", entries)
    cache.set("k2", entries)

    stats = cache.get_stats()
    assert stats["entries"] == 2
    assert stats["prefix"] == "test:"


@pytest.mark.unit
def test_redis_cache_corrupted_data():
    """Test Redis cache with corrupted JSON."""
    fake = _FakeRedis()
    cache = RedisIndexCache(fake, key_prefix="test:")

    fake.set("test:k1", "not valid json")

    assert cache.get("k1") is None
    assert fake.get("test:k1") is None
