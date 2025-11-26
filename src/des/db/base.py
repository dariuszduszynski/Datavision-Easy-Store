"""Abstract database connector."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DatabaseConnector(ABC):
    """Abstract base dla database connectors."""

    @abstractmethod
    def connect(self, config: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def get_files_to_migrate(self, limit: int) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def update_status(self, file_id: int, status: str, **kwargs: Any) -> None:
        ...
