"""Tests for source database connector."""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    BigInteger,
    MetaData,
    String,
    Table,
    create_engine,
    select,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.db.source_config import (  # noqa: E402
    ColumnMapping,
    DatabaseConnection,
    DatabaseType,
    SourceDatabaseConfig,
    SourceTableConfig,
)
from des.db.source_connector import SourceDatabaseConnector, SourceFile  # noqa: E402


@pytest.fixture
def sqlite_config():
    """Create SQLite-based test configuration."""
    return SourceDatabaseConfig(
        name="test-sqlite",
        enabled=True,
        connection=DatabaseConnection(
            type=DatabaseType.MSSQL,  # Use MSSQL branch to avoid FOR UPDATE in SQLite
            host="localhost",
            port=1433,
            database=":memory:",
            username="test",
            password="test",
        ),
        table=SourceTableConfig(
            name="test_files",
            columns=ColumnMapping(
                id="id",
                s3_bucket="bucket",
                s3_key="s3_key",
                size_bytes="size_bytes",
                status="status",
                created_at="created_at",
                metadata_columns={
                    "user_id": "user_id",
                    "mime_type": "mime_type",
                },
            ),
            status_pending_value="pending",
            status_claimed_value="claimed",
        ),
        batch_size=10,
        shard_bits=4,  # 16 shards
    )


@pytest.fixture
def sqlite_db():
    """Create in-memory SQLite database with test table."""
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()

    test_files = Table(
        "test_files",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("bucket", String(256)),
        Column("s3_key", String(2048)),
        Column("size_bytes", BigInteger),
        Column("status", String(32)),
        Column("created_at", DateTime),
        Column("user_id", Integer),
        Column("mime_type", String(128)),
        Column("claimed_by", String(128)),
        Column("claimed_at", DateTime),
        Column("error_message", String(500)),
    )

    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            test_files.insert(),
            [
                {
                    "bucket": "test-bucket",
                    "s3_key": f"files/file_{i}.txt",
                    "size_bytes": 1000 + i,
                    "status": "pending",
                    "created_at": datetime.now(timezone.utc),
                    "user_id": i % 5,
                    "mime_type": "text/plain",
                }
                for i in range(50)
            ],
        )

    return engine


@pytest.mark.unit
def test_source_file_basic():
    """Test SourceFile dataclass."""
    sf = SourceFile(
        id=1,
        s3_bucket="test-bucket",
        s3_key="path/file.txt",
        size_bytes=1024,
        shard_id=5,
        metadata={"user_id": 123},
    )

    assert sf.id == 1
    assert sf.s3_bucket == "test-bucket"
    assert sf.shard_id == 5
    assert sf.metadata["user_id"] == 123


@pytest.mark.unit
def test_connector_initialization(sqlite_config):
    """Test connector initialization."""
    connector = SourceDatabaseConnector(sqlite_config)

    assert connector.config.name == "test-sqlite"
    assert connector.engine is None
    assert connector._table is None


@pytest.mark.unit
def test_connector_connect_disconnect(sqlite_config, sqlite_db, monkeypatch):
    """Test connect and disconnect."""
    connector = SourceDatabaseConnector(sqlite_config)

    def mock_create_engine(*args, **kwargs):
        return sqlite_db

    monkeypatch.setattr("des.db.source_connector.create_engine", mock_create_engine)

    connector.connect()

    assert connector.engine is not None
    assert connector._table is not None
    assert connector._table.name == "test_files"

    connector.disconnect()
    assert connector.engine is None


@pytest.mark.unit
def test_connector_map_row_to_source_file(sqlite_config, sqlite_db, monkeypatch):
    """Test mapping database row to SourceFile."""
    connector = SourceDatabaseConnector(sqlite_config)

    def mock_create_engine(*args, **kwargs):
        return sqlite_db

    monkeypatch.setattr("des.db.source_connector.create_engine", mock_create_engine)

    connector.connect()

    row = {
        "id": 1,
        "bucket": "test-bucket",
        "s3_key": "path/file.txt",
        "size_bytes": 2048,
        "status": "pending",
        "created_at": datetime.now(timezone.utc),
        "user_id": 42,
        "mime_type": "application/pdf",
    }

    source_file = connector._map_row_to_source_file(row)

    assert source_file.id == 1
    assert source_file.s3_bucket == "test-bucket"
    assert source_file.s3_key == "path/file.txt"
    assert source_file.size_bytes == 2048
    assert 0 <= source_file.shard_id < 16  # 4 bits = 16 shards
    assert source_file.metadata["user_id"] == 42
    assert source_file.metadata["mime_type"] == "application/pdf"
    assert "created_at" in source_file.metadata


@pytest.mark.unit
def test_connector_compute_shard_id(sqlite_config, sqlite_db, monkeypatch):
    """Test shard ID computation."""
    connector = SourceDatabaseConnector(sqlite_config)

    def mock_create_engine(*args, **kwargs):
        return sqlite_db

    monkeypatch.setattr("des.db.source_connector.create_engine", mock_create_engine)

    connector.connect()

    row1 = {"bucket": "b", "s3_key": "path/file1.txt", "size_bytes": 100}
    row2 = {"bucket": "b", "s3_key": "path/file1.txt", "size_bytes": 100}
    row3 = {"bucket": "b", "s3_key": "path/file2.txt", "size_bytes": 100}

    shard1 = connector._compute_shard_id(row1)
    shard2 = connector._compute_shard_id(row2)
    shard3 = connector._compute_shard_id(row3)

    assert shard1 == shard2
    assert 0 <= shard1 < 16
    assert 0 <= shard3 < 16


@pytest.mark.integration
def test_connector_claim_pending_files(sqlite_config, sqlite_db, monkeypatch):
    """Test claiming pending files."""
    connector = SourceDatabaseConnector(sqlite_config)

    def mock_create_engine(*args, **kwargs):
        return sqlite_db

    monkeypatch.setattr("des.db.source_connector.create_engine", mock_create_engine)

    connector.connect()

    # Find a shard that has at least one row
    shards = {}
    table = connector._table
    with sqlite_db.connect() as conn:
        rows = conn.execute(select(table)).fetchall()
    for row in rows:
        row_dict = dict(row._mapping)
        shard = connector._compute_shard_id(row_dict)
        shards.setdefault(shard, []).append(row_dict["id"])

    target_shard = next(iter(shards.keys()))

    files = connector.claim_pending_files(
        shard_id=target_shard,
        holder_id="test-pod-1",
        limit=5,
    )

    assert isinstance(files, list)
    assert files  # Should claim some files

    for f in files:
        assert f.shard_id == target_shard
        assert f.s3_bucket == "test-bucket"
        assert f.size_bytes > 0


@pytest.mark.integration
def test_connector_claim_respects_limit(sqlite_config, sqlite_db, monkeypatch):
    """Test that claim respects limit."""
    connector = SourceDatabaseConnector(sqlite_config)

    def mock_create_engine(*args, **kwargs):
        return sqlite_db

    monkeypatch.setattr("des.db.source_connector.create_engine", mock_create_engine)

    connector.connect()

    # Find a shard with data
    table = connector._table
    with sqlite_db.connect() as conn:
        rows = conn.execute(select(table)).fetchall()

    target_shard = None
    for row in rows:
        shard = connector._compute_shard_id(dict(row._mapping))
        target_shard = shard
        break

    assert target_shard is not None

    files = connector.claim_pending_files(
        shard_id=target_shard,
        holder_id="test-pod-1",
        limit=3,
    )

    assert len(files) <= 3


@pytest.mark.integration
def test_connector_mark_files_failed(sqlite_config, sqlite_db, monkeypatch):
    """Test marking files as failed."""
    connector = SourceDatabaseConnector(sqlite_config)

    def mock_create_engine(*args, **kwargs):
        return sqlite_db

    monkeypatch.setattr("des.db.source_connector.create_engine", mock_create_engine)

    connector.connect()
    connector.mark_files_failed([1, 2], "boom error happened")

    table = connector._table
    with sqlite_db.connect() as conn:
        rows = conn.execute(select(table).where(table.c.id.in_([1, 2]))).fetchall()

    for row in rows:
        assert row._mapping["status"] == "failed"
        assert row._mapping["error_message"].startswith("boom error")


@pytest.mark.integration
def test_connector_get_stats(sqlite_config, sqlite_db, monkeypatch):
    """Test getting statistics."""
    connector = SourceDatabaseConnector(sqlite_config)

    def mock_create_engine(*args, **kwargs):
        return sqlite_db

    monkeypatch.setattr("des.db.source_connector.create_engine", mock_create_engine)

    connector.connect()

    stats = connector.get_stats()

    assert isinstance(stats, dict)
    assert "pending" in stats
    assert stats["pending"] == 50  # Inserted 50 pending files


@pytest.mark.integration
def test_connector_context_manager(sqlite_config, sqlite_db, monkeypatch):
    """Test using connector as context manager."""

    def mock_create_engine(*args, **kwargs):
        return sqlite_db

    monkeypatch.setattr("des.db.source_connector.create_engine", mock_create_engine)

    with SourceDatabaseConnector(sqlite_config) as connector:
        assert connector.engine is not None
        stats = connector.get_stats()
        assert isinstance(stats, dict)

    assert connector.engine is None
