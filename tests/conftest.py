import asyncio
from collections import defaultdict
from typing import AsyncIterator, Dict, List

import boto3
import pytest
import pytest_asyncio
from moto import mock_aws
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from des.db.connector import Base, DesDbConnector
from des.packer.multi_shard_packer import PendingFile


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "unit: fast unit tests")
    config.addinivalue_line(
        "markers",
        "integration: tests that require external services or are slower (S3, Redis, DB, etc.)",
    )
    config.addinivalue_line("markers", "s3: tests that interact with S3 or moto S3")
    config.addinivalue_line("markers", "slow: slow-running tests")


@pytest.fixture
def redis_client():
    """
    Provide fake Redis client for testing.
    Uses fakeredis if available, otherwise skips tests requiring Redis.
    """
    try:
        import fakeredis

        return fakeredis.FakeRedis()
    except ImportError:
        pytest.skip("fakeredis not installed (pip install fakeredis)")


@pytest_asyncio.fixture
async def async_db_engine() -> AsyncIterator[AsyncEngine]:
    """Async SQLite engine for tests (isolated per test)."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS source_files (
                    id INTEGER PRIMARY KEY,
                    shard_id INTEGER,
                    name TEXT,
                    data BLOB,
                    status TEXT,
                    claimed_by TEXT,
                    claimed_at DATETIME
                )
                """
            )
        )
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def des_db(async_db_engine: AsyncEngine) -> AsyncIterator[DesDbConnector]:
    """DES DB connector backed by the async test engine."""
    connector = DesDbConnector(db_url=str(async_db_engine.url))
    original_engine = connector.engine
    connector.engine = async_db_engine
    connector.session_factory = async_sessionmaker(
        async_db_engine, expire_on_commit=False
    )
    await original_engine.dispose()
    await connector.init_models()
    try:
        yield connector
    finally:
        await connector.engine.dispose()


@pytest.fixture
def source_db_mock():
    """In-memory source DB mock preloaded with sample files."""

    class SourceDbMock:
        def __init__(self) -> None:
            self._files: Dict[int, List[PendingFile]] = defaultdict(list)
            self._seed()

        def _seed(self) -> None:
            self._files[1] = [
                PendingFile(
                    shard_id=1, name="file1.txt", data=b"alpha", meta={"source": "test"}
                ),
                PendingFile(
                    shard_id=1, name="file2.txt", data=b"bravo", meta={"source": "test"}
                ),
            ]
            self._files[2] = [
                PendingFile(
                    shard_id=2,
                    name="file3.txt",
                    data=b"charlie",
                    meta={"source": "test"},
                )
            ]

        async def get_pending_files(
            self, shard_id: int, limit: int
        ) -> List[PendingFile]:
            await asyncio.sleep(0)
            items = self._files[shard_id][:limit]
            self._files[shard_id] = self._files[shard_id][limit:]
            return items

    return SourceDbMock()


@pytest.fixture
def s3_client_mock():
    """Moto-backed S3 client with test buckets."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-source")
        s3.create_bucket(Bucket="test-archive")
        yield s3


@pytest.fixture
def test_config(tmp_path):
    """Configuration used by packer E2E tests."""
    return {
        "batch_size": 5,
        "lock_ttl_seconds": 5,
        "checkpoint_every_files": 1,
        "checkpoint_every_seconds": 1,
        "loop_sleep_seconds": 0,
        "work_dir": str(tmp_path / "work"),
        "holder_id": "test-holder",
        "dest_prefix": "test/prefix",
    }
