"""Configuration management."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


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
        raise NotImplementedError("Config.from_env is not implemented in this stub.")
