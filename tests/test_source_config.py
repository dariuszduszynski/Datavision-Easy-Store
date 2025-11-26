"""Tests for source configuration models."""
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

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


@pytest.mark.unit
def test_database_type_enum():
    """Test all supported database types."""
    assert DatabaseType.ORACLE == "oracle"
    assert DatabaseType.MSSQL == "mssql"
    assert DatabaseType.MYSQL == "mysql"
    assert DatabaseType.POSTGRES == "postgres"
    assert DatabaseType.MARIADB == "mariadb"


@pytest.mark.unit
def test_column_mapping_basic():
    """Test basic column mapping."""
    mapping = ColumnMapping(
        id="file_id",
        s3_bucket="bucket_name",
        s3_key="object_key",
        size_bytes="file_size",
    )
    assert mapping.id == "file_id"
    assert mapping.metadata_columns == {}


@pytest.mark.unit
def test_column_mapping_with_metadata():
    """Test column mapping with metadata columns."""
    mapping = ColumnMapping(
        id="id",
        s3_bucket="bucket",
        s3_key="key",
        size_bytes="size",
        metadata_columns={
            "user_id": "uploaded_by",
            "mime_type": "content_type",
        },
    )
    assert len(mapping.metadata_columns) == 2
    assert mapping.metadata_columns["user_id"] == "uploaded_by"


@pytest.mark.unit
def test_column_mapping_metadata_none_defaults_empty():
    """Metadata columns None should become empty dict."""
    mapping = ColumnMapping(
        id="id",
        s3_bucket="bucket",
        s3_key="key",
        size_bytes="size",
        metadata_columns=None,
    )
    assert mapping.metadata_columns == {}


@pytest.mark.unit
def test_column_mapping_missing_required():
    """Test that missing required fields raise error."""
    with pytest.raises(ValidationError):
        ColumnMapping(
            id="id",
            s3_bucket="bucket",
            # Missing s3_key and size_bytes
        )


@pytest.mark.unit
def test_database_connection_postgres():
    """Test PostgreSQL connection config."""
    conn = DatabaseConnection(
        type=DatabaseType.POSTGRES,
        host="localhost",
        port=5432,
        database="testdb",
        username="user",
        password="pass",
    )
    assert conn.type == DatabaseType.POSTGRES
    url = conn.get_connection_url()
    assert "postgresql+psycopg2://" in url
    assert "user:pass@localhost:5432/testdb" in url


@pytest.mark.unit
def test_database_connection_oracle():
    """Test Oracle connection config."""
    conn = DatabaseConnection(
        type=DatabaseType.ORACLE,
        host="oracle.example.com",
        port=1521,
        database="ORCL",
        username="system",
        password="oracle",
    )
    url = conn.get_connection_url()
    assert "oracle+cx_oracle://" in url


@pytest.mark.unit
def test_database_connection_mssql():
    """Test MSSQL connection config."""
    conn = DatabaseConnection(
        type=DatabaseType.MSSQL,
        host="mssql.example.com",
        port=1433,
        database="TestDB",
        username="sa",
        password="Password123",
    )
    url = conn.get_connection_url()
    assert "mssql+pymssql://" in url


@pytest.mark.unit
def test_database_connection_mysql():
    """Test MySQL connection config."""
    conn = DatabaseConnection(
        type=DatabaseType.MYSQL,
        host="mysql.example.com",
        port=3306,
        database="testdb",
        username="root",
        password="mysql",
        charset="utf8mb4",
    )
    url = conn.get_connection_url()
    assert "mysql+pymysql://" in url
    assert "charset=utf8mb4" in url


@pytest.mark.unit
def test_database_connection_default_port_validation_error():
    """Port is required; None should raise ValidationError in current model."""
    with pytest.raises(ValidationError):
        DatabaseConnection(
            type=DatabaseType.POSTGRES,
            host="localhost",
            port=None,  # Pydantic should reject None for int field
            database="test",
            username="user",
            password="pass",
        )


@pytest.mark.unit
def test_source_table_config_basic():
    """Test basic table configuration."""
    table = SourceTableConfig(
        name="files",
        columns=ColumnMapping(
            id="id",
            s3_bucket="bucket",
            s3_key="key",
            size_bytes="size",
        ),
    )
    assert table.name == "files"
    assert table.full_table_name == "files"


@pytest.mark.unit
def test_source_table_config_with_schema():
    """Test table config with schema."""
    table = SourceTableConfig(
        name="files",
        schema="dbo",
        columns=ColumnMapping(
            id="id",
            s3_bucket="bucket",
            s3_key="key",
            size_bytes="size",
        ),
    )
    assert table.full_table_name == "dbo.files"


@pytest.mark.unit
def test_source_table_config_with_where_clause():
    """Test table config with WHERE clause."""
    table = SourceTableConfig(
        name="files",
        columns=ColumnMapping(
            id="id",
            s3_bucket="bucket",
            s3_key="key",
            size_bytes="size",
        ),
        where_clause="created_at >= '2025-01-01'",
    )
    assert table.where_clause == "created_at >= '2025-01-01'"


@pytest.mark.unit
def test_source_database_config_basic():
    """Test complete source database config."""
    config = SourceDatabaseConfig(
        name="test-source",
        enabled=True,
        connection=DatabaseConnection(
            type=DatabaseType.POSTGRES,
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        ),
        table=SourceTableConfig(
            name="files",
            columns=ColumnMapping(
                id="id",
                s3_bucket="bucket",
                s3_key="key",
                size_bytes="size",
            ),
        ),
        shard_bits=8,
    )
    assert config.name == "test-source"
    assert config.enabled is True
    assert config.get_shard_count() == 256  # 2^8


@pytest.mark.unit
def test_source_database_config_shard_count():
    """Test shard count calculation."""
    config = SourceDatabaseConfig(
        name="test",
        connection=DatabaseConnection(
            type=DatabaseType.POSTGRES,
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        ),
        table=SourceTableConfig(
            name="files",
            columns=ColumnMapping(
                id="id",
                s3_bucket="bucket",
                s3_key="key",
                size_bytes="size",
            ),
        ),
        shard_bits=6,
    )
    assert config.get_shard_count() == 64  # 2^6


@pytest.mark.unit
def test_multi_source_config_basic():
    """Test multi-source configuration."""
    config1 = SourceDatabaseConfig(
        name="source1",
        enabled=True,
        connection=DatabaseConnection(
            type=DatabaseType.POSTGRES,
            host="localhost",
            port=5432,
            database="db1",
            username="user",
            password="pass",
        ),
        table=SourceTableConfig(
            name="files1",
            columns=ColumnMapping(
                id="id",
                s3_bucket="bucket",
                s3_key="key",
                size_bytes="size",
            ),
        ),
    )

    config2 = SourceDatabaseConfig(
        name="source2",
        enabled=False,
        connection=DatabaseConnection(
            type=DatabaseType.MYSQL,
            host="localhost",
            port=3306,
            database="db2",
            username="user",
            password="pass",
        ),
        table=SourceTableConfig(
            name="files2",
            columns=ColumnMapping(
                id="id",
                s3_bucket="bucket",
                s3_key="key",
                size_bytes="size",
            ),
        ),
    )

    multi = MultiSourceConfig(sources=[config1, config2])

    assert len(multi.sources) == 2
    assert len(multi.get_enabled_sources()) == 1
    assert multi.get_enabled_sources()[0].name == "source1"


@pytest.mark.unit
def test_multi_source_config_get_by_name():
    """Test getting source by name."""
    config = SourceDatabaseConfig(
        name="my-source",
        connection=DatabaseConnection(
            type=DatabaseType.POSTGRES,
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        ),
        table=SourceTableConfig(
            name="files",
            columns=ColumnMapping(
                id="id",
                s3_bucket="bucket",
                s3_key="key",
                size_bytes="size",
            ),
        ),
    )

    multi = MultiSourceConfig(sources=[config])

    found = multi.get_source_by_name("my-source")
    assert found is not None
    assert found.name == "my-source"

    not_found = multi.get_source_by_name("nonexistent")
    assert not_found is None


@pytest.mark.unit
def test_source_database_config_from_yaml(tmp_path):
    """Test loading config from YAML file."""
    yaml_content = """
name: "test-source"
enabled: true
connection:
  type: postgres
  host: localhost
  port: 5432
  database: testdb
  username: user
  password: pass
table:
  name: files
  columns:
    id: file_id
    s3_bucket: bucket
    s3_key: key
    size_bytes: size
shard_bits: 8
"""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(yaml_content)

    config = SourceDatabaseConfig.from_yaml(str(yaml_file))

    assert config.name == "test-source"
    assert config.connection.type == DatabaseType.POSTGRES
    assert config.table.name == "files"


@pytest.mark.unit
def test_source_database_config_from_dict():
    """Test loading config from dictionary."""
    data = {
        "name": "dict-source",
        "enabled": True,
        "connection": {
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "dictdb",
            "username": "user",
            "password": "pass",
        },
        "table": {
            "name": "files",
            "columns": {
                "id": "id",
                "s3_bucket": "bucket",
                "s3_key": "key",
                "size_bytes": "size",
            },
        },
        "shard_bits": 4,
    }

    config = SourceDatabaseConfig.from_dict(data)

    assert config.name == "dict-source"
    assert config.connection.database == "dictdb"
    assert config.get_shard_count() == 16


@pytest.mark.unit
def test_multi_source_config_from_yaml(tmp_path):
    """Test loading multi-source config from YAML."""
    yaml_content = """
sources:
  - name: "source1"
    enabled: true
    connection:
      type: postgres
      host: localhost
      port: 5432
      database: db1
      username: user
      password: pass
    table:
      name: files1
      columns:
        id: id
        s3_bucket: bucket
        s3_key: key
        size_bytes: size
    shard_bits: 8

  - name: "source2"
    enabled: false
    connection:
      type: mysql
      host: localhost
      port: 3306
      database: db2
      username: user
      password: pass
    table:
      name: files2
      columns:
        id: id
        s3_bucket: bucket
        s3_key: key
        size_bytes: size
    shard_bits: 6
"""
    yaml_file = tmp_path / "multi_config.yaml"
    yaml_file.write_text(yaml_content)

    config = MultiSourceConfig.from_yaml(str(yaml_file))

    assert len(config.sources) == 2
    assert len(config.get_enabled_sources()) == 1
