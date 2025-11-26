# src/des/db/source_config.py

"""Configuration models for source database connections."""
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, field_validator


class DatabaseType(str, Enum):
    """Supported database types."""
    ORACLE = "oracle"
    MSSQL = "mssql"
    MYSQL = "mysql"
    POSTGRES = "postgres"
    MARIADB = "mariadb"


class ColumnMapping(BaseModel):
    """
    Mapping between source DB columns and SourceFile fields.
    
    Example:
        ColumnMapping(
            id="file_id",
            s3_bucket="bucket_name", 
            s3_key="s3_path",
            size_bytes="file_size"
        )
    """
    # Required fields (must map to SourceFile columns)
    id: str = Field(..., description="Primary key column name")
    s3_bucket: str = Field(..., description="Column with S3 bucket name")
    s3_key: str = Field(..., description="Column with S3 object key")
    size_bytes: str = Field(..., description="Column with file size in bytes")
    
    # Optional fields
    status: Optional[str] = Field(None, description="Status column name")
    created_at: Optional[str] = Field(None, description="Creation timestamp column")
    
    # Custom metadata columns (will be stored in JSON)
    metadata_columns: Dict[str, str] = Field(
        default_factory=dict,
        description="Additional columns to store in metadata"
    )
    
    @field_validator('metadata_columns', mode='before')
    @classmethod
    def validate_metadata_columns(cls, v):
        """Ensure metadata column names are valid."""
        if v is None:
            return {}
        return v


class DatabaseConnection(BaseModel):
    """Database connection configuration."""
    
    type: DatabaseType = Field(..., description="Database type")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    
    # Optional connection parameters
    db_schema: Optional[str] = Field(None, description="Schema name (Oracle, PostgreSQL)", alias="schema")
    charset: str = Field("utf8mb4", description="Character set")
    
    # Connection pool settings
    pool_size: int = Field(5, ge=1, le=100, description="Connection pool size")
    pool_recycle: int = Field(3600, description="Connection recycle time (seconds)")
    pool_pre_ping: bool = Field(True, description="Test connections before use")
    
    # Additional driver-specific options
    driver_options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Driver-specific connection options"
    )
    
    @field_validator('port')
    @classmethod
    def validate_port(cls, v, info):
        """Set default port based on database type if not provided."""
        if v is None:
            db_type = info.data.get('type')
            defaults = {
                DatabaseType.ORACLE: 1521,
                DatabaseType.MSSQL: 1433,
                DatabaseType.MYSQL: 3306,
                DatabaseType.POSTGRES: 5432,
                DatabaseType.MARIADB: 3306,
            }
            return defaults.get(db_type, 5432)
        return v
    
    def get_connection_url(self) -> str:
        """
        Generate SQLAlchemy connection URL.
        
        Returns:
            Connection string for SQLAlchemy
        """
        # Driver mapping
        drivers = {
            DatabaseType.ORACLE: "oracle+oracledb",
            DatabaseType.MSSQL: "mssql+pymssql",
            DatabaseType.MYSQL: "mysql+pymysql",
            DatabaseType.POSTGRES: "postgresql+psycopg2",
            DatabaseType.MARIADB: "mysql+pymysql",
        }
        
        driver = drivers[self.type]
        
        # Build connection URL
        url = (
            f"{driver}://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        
        # Add charset for MySQL/MariaDB
        if self.type in (DatabaseType.MYSQL, DatabaseType.MARIADB):
            url += f"?charset={self.charset}"
        
        return url


class SourceTableConfig(BaseModel):
    """Configuration for source table."""
    
    name: str = Field(..., description="Table name")
    table_schema: Optional[str] = Field(None, description="Schema name (if applicable)", alias="schema")
    
    # Column mapping
    columns: ColumnMapping = Field(..., description="Column mapping to SourceFile")
    
    # Query filters
    where_clause: Optional[str] = Field(
        None,
        description="Optional WHERE clause for filtering (without WHERE keyword)"
    )
    
    # Claim logic
    status_pending_value: str = Field(
        "pending",
        description="Value in status column indicating pending files"
    )
    status_claimed_value: str = Field(
        "claimed",
        description="Value to set when claiming files"
    )
    
    # Shard routing
    shard_key_column: Optional[str] = Field(
        None,
        description="Column to use for shard routing (if not using s3_key)"
    )
    
    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        if self.table_schema:
            return f"{self.table_schema}.{self.name}"
        return self.name


class SourceDatabaseConfig(BaseModel):
    """Complete source database configuration."""
    
    name: str = Field(..., description="Configuration name (for logging)")
    enabled: bool = Field(True, description="Whether this source is enabled")
    
    connection: DatabaseConnection
    table: SourceTableConfig
    
    # Batch processing
    batch_size: int = Field(100, ge=1, le=10000, description="Batch size for claiming")
    claim_timeout_seconds: int = Field(
        300,
        description="Timeout for claimed files (before they can be reclaimed)"
    )
    
    # Shard routing
    shard_bits: int = Field(8, ge=1, le=16, description="Number of shard bits (2^n shards)")
    
    def get_shard_count(self) -> int:
        """Get total number of shards."""
        return 1 << self.shard_bits
    
    @classmethod
    def from_yaml(cls, path: str) -> "SourceDatabaseConfig":
        """Load configuration from YAML file."""
        import yaml
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    @classmethod
    def from_dict(cls, data: dict) -> "SourceDatabaseConfig":
        """Load configuration from dictionary."""
        return cls(**data)


class MultiSourceConfig(BaseModel):
    """Configuration for multiple source databases."""
    
    sources: List[SourceDatabaseConfig] = Field(
        ...,
        description="List of source database configurations"
    )
    
    @classmethod
    def from_yaml(cls, path: str) -> "MultiSourceConfig":
        """Load multiple source configurations from YAML."""
        import yaml
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    def get_enabled_sources(self) -> List[SourceDatabaseConfig]:
        """Get list of enabled sources."""
        return [s for s in self.sources if s.enabled]
    
    def get_source_by_name(self, name: str) -> Optional[SourceDatabaseConfig]:
        """Get source configuration by name."""
        for source in self.sources:
            if source.name == name:
                return source
        return None
