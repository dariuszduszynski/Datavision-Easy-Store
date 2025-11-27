import sys
from pathlib import Path
from typing import List

import pytest

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core.cache import InMemoryIndexCache, RedisIndexCache  # noqa: E402
from des.core.models import IndexEntry  # noqa: E402


def _sample_entries() -> List[IndexEntry]:
    return [
        IndexEntry(
            name="a.txt",
            data_offset=0,
            data_length=5,
            meta_offset=100,
            meta_length=10,
            flags=0,
        ),
        IndexEntry(
            name="b.bin",
            data_offset=5,
            data_length=3,
            meta_offset=110,
            meta_length=12,
            flags=1,
        ),
    ]


def test_in_memory_index_cache_round_trip() -> None:
    cache = InMemoryIndexCache()
    key = "bucket/key/etag"
    entries = _sample_entries()

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


@pytest.mark.integration
def test_redis_index_cache_round_trip(redis_client) -> None:
    """Requires a running Redis and a 'redis_client' fixture providing redis.Redis."""
    cache = RedisIndexCache(redis_client, key_prefix="test:des:index:")
    key = "bucket/key/etag"
    entries = _sample_entries()

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
