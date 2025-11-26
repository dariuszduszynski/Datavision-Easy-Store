"""
Configuration management.
"""

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
    """
    Main config loader - supports YAML, ENV, etc.
    """

    def __init__(self):
        self.database: Optional[DatabaseConfig] = None
        self.migration: Optional[MigrationConfig] = None

    @classmethod
    def from_yaml(cls, path: str): ...

    @classmethod
    def from_env(cls): ...
