"""PostgreSQL implementation."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List

from des.db.base import DatabaseConnector

logger = logging.getLogger(__name__)


class PostgresConnector(DatabaseConnector):
    """PostgreSQL specific implementation."""

    def connect(self, config: Dict[str, Any]) -> None:
        self._config = config
        self._engine = None
        self._sessionmaker = None
        logger.warning(
            "PostgresConnector.connect() stub invoked; no real DB connection established",
            extra={"config": config},
        )

    def claim_files_atomic(
        self, shard_ids: Iterable[int], pod_name: str, limit: int
    ) -> List[Dict[str, Any]]:
        """Atomic claim z FOR UPDATE SKIP LOCKED."""
        raise NotImplementedError

    def get_files_to_migrate(self, limit: int) -> List[Dict[str, Any]]:
        """Stub: return no files to migrate."""
        logger.info("PostgresConnector.get_files_to_migrate stub invoked", extra={"limit": limit})
        return []

    def update_status(self, file_id: int, status: str, **kwargs: Any) -> None:
        """Stub: log status update request."""
        logger.info(
            "PostgresConnector.update_status stub invoked",
            extra={"file_id": file_id, "status": status, "extra": kwargs},
        )
