"""Configuration management."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse


@dataclass
class DatabaseConfig:
    type: str
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class MigrationConfig:
    n_bits: int
    num_pods: int
    batch_size: int
    max_workers: int
    source_bucket: str
    archive_bucket: str


class Config:
    """Main config loader - supports YAML, ENV, etc."""

    def __init__(self) -> None:
        self.database: Optional[DatabaseConfig] = None
        self.migration: Optional[MigrationConfig] = None

    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        raise NotImplementedError("Config.from_yaml is not implemented in this stub.")

    @classmethod
    def from_env(cls) -> "Config":
        db_url = os.getenv("DES_DB_URL", "postgresql+asyncpg://des:des@db/des")
        archive_bucket = os.getenv("DES_ARCHIVE_BUCKET", "des-archive")

        node_id = int(os.getenv("DES_NODE_ID", "1"))
        wrap_bits = int(os.getenv("DES_WRAP_BITS", "10"))
        shard_bits = int(os.getenv("DES_SHARD_BITS", "8"))
        packer_workdir = os.getenv("DES_PACKER_WORKDIR", "/app/workdir")
        assign_host = os.getenv("DES_ASSIGN_HOST", "http://localhost:8000")

        parsed = urlparse(db_url)
        db_type = parsed.scheme.split("+", 1)[0] if parsed.scheme else ""
        db_host = parsed.hostname or ""
        db_port = parsed.port or 0
        db_name = parsed.path.lstrip("/") if parsed.path else ""
        db_user = parsed.username or ""
        db_password = parsed.password or ""

        database_cfg = DatabaseConfig(
            type=db_type,
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )

        migration_cfg = MigrationConfig(
            n_bits=shard_bits,
            num_pods=1,
            batch_size=100,
            max_workers=1,
            source_bucket=archive_bucket,
            archive_bucket=archive_bucket,
        )

        config = cls()
        config.database = database_cfg
        config.migration = migration_cfg

        config.db_url: str = db_url
        config.archive_bucket: str = archive_bucket
        config.node_id: int = node_id
        config.wrap_bits: int = wrap_bits
        config.shard_bits: int = shard_bits
        config.packer_workdir: str = packer_workdir
        config.assign_host: str = assign_host

        return config
