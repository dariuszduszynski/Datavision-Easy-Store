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
        IndexEntry(name="file1", data_offset=0, data_length=5, meta_offset=100, meta_length=10, flags=0),
        IndexEntry(name="file2", data_offset=5, data_length=3, meta_offset=110, meta_length=12, flags=1),
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
    for a, b in zip(result, entries):
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
    for a, b in zip(result, entries):
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
