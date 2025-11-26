"""End-to-end integration test for source database system."""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)

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
from des.db.source_connector import SourceDatabaseConnector  # noqa: E402


@pytest.mark.integration
def test_full_workflow_sqlite():
    """Test complete workflow: config → connect → claim → mark packed."""

    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()

    files_table = Table(
        "source_files",
        metadata,
        Column("file_id", Integer, primary_key=True),
        Column("bucket_name", String(256)),
        Column("object_key", String(2048)),
        Column("file_size", BigInteger),
        Column("processing_status", String(32)),
        Column("upload_date", DateTime),
        Column("user_email", String(256)),
        Column("claimed_by", String(128)),
        Column("claimed_at", DateTime),
        Column("des_name", String(256)),
        Column("packed_at", DateTime),
        Column("des_container_id", Integer),
    )

    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            files_table.insert(),
            [
                {
                    "bucket_name": "uploads",
                    "object_key": f"2025/11/{i:04d}.jpg",
                    "file_size": 1024 * (i + 1),
                    "processing_status": "uploaded",
                    "upload_date": datetime.now(timezone.utc),
                    "user_email": f"user{i}@example.com",
                }
                for i in range(20)
            ],
        )

    config = SourceDatabaseConfig(
        name="test-source",
        enabled=True,
        connection=DatabaseConnection(
            type=DatabaseType.MSSQL,  # Avoid FOR UPDATE with SQLite
            host="localhost",
            port=1433,
            database=":memory:",
            username="test",
            password="test",
        ),
        table=SourceTableConfig(
            name="source_files",
            columns=ColumnMapping(
                id="file_id",
                s3_bucket="bucket_name",
                s3_key="object_key",
                size_bytes="file_size",
                status="processing_status",
                created_at="upload_date",
                metadata_columns={
                    "user_email": "user_email",
                },
            ),
            status_pending_value="uploaded",
            status_claimed_value="packing",
        ),
        batch_size=5,
        shard_bits=3,  # 8 shards
    )

    connector = SourceDatabaseConnector(config)
    connector.engine = engine
    connector._reflect_table()

    # Determine a shard with data
    with engine.connect() as conn:
        first_row = conn.execute(files_table.select()).fetchone()
    assert first_row is not None
    target_shard = connector._compute_shard_id(dict(first_row._mapping))

    claimed_files = connector.claim_pending_files(
        shard_id=target_shard,
        holder_id="test-pod-1",
        limit=5,
    )

    assert claimed_files
    for f in claimed_files:
        assert f.shard_id == target_shard
        assert f.s3_bucket == "uploads"
        assert "user_email" in f.metadata

    stats = connector.get_stats()
    assert "packing" in stats
    assert stats["packing"] == len(claimed_files)

    file_ids = [f.id for f in claimed_files]
    des_names = [f"DES_20251126_{i}" for i in range(len(claimed_files))]

    connector.mark_files_packed(
        file_ids=file_ids,
        des_names=des_names,
        container_id=1,
    )

    stats_after = connector.get_stats()
    assert "packed" in stats_after
    assert stats_after["packed"] == len(claimed_files)

    # Verify DES name write when column exists
    with engine.connect() as conn:
        row = conn.execute(
            files_table.select().where(files_table.c.file_id == file_ids[0])
        ).fetchone()
    assert row is not None
    assert row._mapping["des_name"] == des_names[0]
    assert row._mapping["processing_status"] == "packed"


@pytest.mark.integration
def test_multi_source_config_loading(tmp_path):
    """Test loading and using multi-source configuration."""

    yaml_content = """
sources:
  - name: "source-a"
    enabled: true
    connection:
      type: postgres
      host: localhost
      port: 5432
      database: db_a
      username: user
      password: pass
    table:
      name: files_a
      columns:
        id: id
        s3_bucket: bucket
        s3_key: key
        size_bytes: size
        status: status
    shard_bits: 4
    batch_size: 10

  - name: "source-b"
    enabled: false
    connection:
      type: mysql
      host: localhost
      port: 3306
      database: db_b
      username: user
      password: pass
    table:
      name: files_b
      columns:
        id: file_id
        s3_bucket: s3_bucket
        s3_key: s3_path
        size_bytes: bytes
        status: state
    shard_bits: 6
"""

    yaml_file = tmp_path / "sources.yaml"
    yaml_file.write_text(yaml_content)

    config = MultiSourceConfig.from_yaml(str(yaml_file))

    assert len(config.sources) == 2
    assert len(config.get_enabled_sources()) == 1

    source_a = config.get_source_by_name("source-a")
    assert source_a is not None
    assert source_a.enabled is True
    assert source_a.get_shard_count() == 16

    source_b = config.get_source_by_name("source-b")
    assert source_b is not None
    assert source_b.enabled is False
