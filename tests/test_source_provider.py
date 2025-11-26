"""Tests for source file provider."""

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.db.source_config import (  # noqa: E402
    ColumnMapping,
    DatabaseConnection,
    DatabaseType,
    MultiSourceConfig,
    SourceDatabaseConfig,
    SourceTableConfig,
)
from des.db.source_connector import SourceFile  # noqa: E402
from des.packer.source_provider import (  # noqa: E402
    MultiSourceFileProvider,
    PendingFile,
)


def make_source_config(
    name: str = "source1", enabled: bool = True
) -> SourceDatabaseConfig:
    return SourceDatabaseConfig(
        name=name,
        enabled=enabled,
        connection=DatabaseConnection(
            type=DatabaseType.POSTGRES,
            host="localhost",
            port=5432,
            database="db",
            username="user",
            password="pass",
            schema="public",
            charset="utf8mb4",
            pool_size=5,
            pool_recycle=3600,
            pool_pre_ping=True,
            driver_options={},
        ),
        table=SourceTableConfig(
            name="files",
            schema="public",
            columns=ColumnMapping(
                id="id",
                s3_bucket="bucket",
                s3_key="key",
                size_bytes="size",
                status="status",
                created_at="created_at",
                metadata_columns={"owner": "owner"},
            ),
            where_clause=None,
            status_pending_value="pending",
            status_claimed_value="claimed",
            shard_key_column="id",
        ),
        batch_size=100,
        claim_timeout_seconds=300,
        shard_bits=8,
    )


@pytest.fixture
def sample_source_config():
    return make_source_config()


@pytest.mark.unit
def test_pending_file_basic():
    """Test PendingFile dataclass."""
    pf = PendingFile(
        id=1,
        shard_id=5,
        name="test.txt",
        data=b"hello world",
        meta={"user_id": 123},
    )

    assert pf.id == 1
    assert pf.shard_id == 5
    assert pf.name == "test.txt"
    assert pf.data == b"hello world"
    assert pf.meta["user_id"] == 123


@pytest.mark.unit
def test_provider_initialization():
    """Test provider initialization."""
    config = MultiSourceConfig(sources=[])
    s3_mock = Mock()

    provider = MultiSourceFileProvider(
        config=config,
        s3_client=s3_mock,
        holder_id="test-pod-1",
    )

    assert provider.config == config
    assert provider.s3 == s3_mock
    assert provider.holder_id == "test-pod-1"
    assert len(provider.connectors) == 0  # No enabled sources


@pytest.mark.asyncio
@pytest.mark.unit
async def test_provider_connect_and_disconnect_all(sample_source_config, monkeypatch):
    """Test connect/disconnect lifecycle and context manager."""

    class StubConnector:
        def __init__(self, cfg):
            self.config = cfg
            self.connected = False

        def connect(self):
            self.connected = True

        def disconnect(self):
            self.connected = False

    monkeypatch.setattr(
        "des.packer.source_provider.SourceDatabaseConnector",
        StubConnector,
    )

    provider = MultiSourceFileProvider(
        config=MultiSourceConfig(sources=[sample_source_config]),
        s3_client=Mock(),
        holder_id="holder-1",
    )

    with provider as p:
        assert all(conn.connected for conn in p.connectors.values())

    assert all(not conn.connected for conn in provider.connectors.values())


@pytest.mark.asyncio
@pytest.mark.unit
async def test_provider_get_pending_files_builds_pending_file(
    sample_source_config, monkeypatch
):
    """Ensure provider claims, downloads, and wraps files."""

    class StubConnector:
        def __init__(self, cfg):
            self.config = cfg
            self.failed = []
            self.claim_args = None

        def claim_pending_files(self, shard_id, holder_id, limit):
            self.claim_args = (shard_id, holder_id, limit)
            return [
                SourceFile(
                    id=1,
                    s3_bucket="bucket",
                    s3_key="folder/file1.txt",
                    size_bytes=10,
                    shard_id=shard_id,
                    metadata={"user_id": 1},
                ),
                SourceFile(
                    id=2,
                    s3_bucket="bucket",
                    s3_key="folder/file2.txt",
                    size_bytes=20,
                    shard_id=shard_id,
                    metadata={"mime_type": "text/plain"},
                ),
            ]

        def mark_files_failed(self, file_ids, error_message):
            self.failed.append((file_ids, error_message))

    monkeypatch.setattr(
        "des.packer.source_provider.SourceDatabaseConnector",
        StubConnector,
    )

    s3_mock = Mock()
    s3_mock.get_object.return_value = {"Body": Mock(read=Mock(return_value=b"content"))}

    provider = MultiSourceFileProvider(
        config=MultiSourceConfig(sources=[sample_source_config]),
        s3_client=s3_mock,
        holder_id="holder-1",
    )

    files = await provider.get_pending_files(shard_id=3, limit=2)

    assert len(files) == 2
    assert files[0].name == "file1.txt"
    assert files[0].meta["source_db"] == sample_source_config.name
    assert files[0].meta["source_file_id"] == 1
    assert files[0].meta["original_s3_bucket"] == "bucket"
    assert "user_id" in files[0].meta
    assert provider.connectors[sample_source_config.name].claim_args == (
        3,
        "holder-1",
        2,
    )
    assert provider.connectors[sample_source_config.name].failed == []
    assert s3_mock.get_object.call_count == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_provider_download_error_marks_failed(sample_source_config, monkeypatch):
    """Download failures should mark files as failed."""

    class StubConnector:
        def __init__(self, cfg):
            self.config = cfg
            self.failed = []

        def claim_pending_files(self, shard_id, holder_id, limit):
            return [
                SourceFile(
                    id=99,
                    s3_bucket="bucket",
                    s3_key="folder/fail.txt",
                    size_bytes=10,
                    shard_id=shard_id,
                    metadata={},
                )
            ]

        def mark_files_failed(self, file_ids, error_message):
            self.failed.append((file_ids, error_message))

    monkeypatch.setattr(
        "des.packer.source_provider.SourceDatabaseConnector",
        StubConnector,
    )

    s3_mock = Mock()
    s3_mock.get_object.side_effect = RuntimeError("boom")

    provider = MultiSourceFileProvider(
        config=MultiSourceConfig(sources=[sample_source_config]),
        s3_client=s3_mock,
        holder_id="holder-err",
    )

    files = await provider.get_pending_files(shard_id=1, limit=1)

    assert files == []
    stub = provider.connectors[sample_source_config.name]
    assert stub.failed == [([99], "boom")]


@pytest.mark.asyncio
@pytest.mark.unit
async def test_provider_mark_files_packed_delegates(sample_source_config, monkeypatch):
    """Ensure mark_files_packed delegates to connector."""

    class StubConnector:
        def __init__(self, cfg):
            self.config = cfg
            self.called = None

        def mark_files_packed(self, file_ids, des_names, container_id):
            self.called = (file_ids, des_names, container_id)

    monkeypatch.setattr(
        "des.packer.source_provider.SourceDatabaseConnector",
        StubConnector,
    )

    provider = MultiSourceFileProvider(
        config=MultiSourceConfig(sources=[sample_source_config]),
        s3_client=Mock(),
        holder_id="holder-1",
    )

    await provider.mark_files_packed(
        source_db=sample_source_config.name,
        file_ids=[1, 2],
        des_names=["a", "b"],
        container_id=7,
    )

    stub = provider.connectors[sample_source_config.name]
    assert stub.called == ([1, 2], ["a", "b"], 7)


@pytest.mark.unit
def test_provider_get_all_stats(monkeypatch):
    """Stats should be aggregated and errors reported."""

    class StubConnector:
        def __init__(self, cfg):
            self.config = cfg

        def get_stats(self):
            if self.config.name == "bad":
                raise RuntimeError("failed")
            return {"pending": 5}

    monkeypatch.setattr(
        "des.packer.source_provider.SourceDatabaseConnector",
        StubConnector,
    )

    config = MultiSourceConfig(
        sources=[
            make_source_config("good"),
            make_source_config("bad"),
        ]
    )

    provider = MultiSourceFileProvider(
        config=config,
        s3_client=Mock(),
        holder_id="holder-1",
    )

    stats = provider.get_all_stats()

    assert stats["good"] == {"pending": 5}
    assert "error" in stats["bad"]


@pytest.mark.asyncio
@pytest.mark.unit
async def test_provider_download_from_s3():
    """Test S3 download."""
    config = MultiSourceConfig(sources=[])

    s3_mock = Mock()
    s3_mock.get_object.return_value = {
        "Body": Mock(read=Mock(return_value=b"file content"))
    }

    provider = MultiSourceFileProvider(
        config=config,
        s3_client=s3_mock,
        holder_id="test",
    )

    data = await provider._download_from_s3("bucket", "key")

    assert data == b"file content"
    s3_mock.get_object.assert_called_once_with(Bucket="bucket", Key="key")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_provider_get_pending_files_integration():
    """Test getting pending files (integration with mock connector)."""
    pytest.skip("Requires complex connector mocking")
