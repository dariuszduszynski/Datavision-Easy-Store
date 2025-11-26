"""PostgreSQL implementation."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from des.db.base import DatabaseConnector


class PostgresConnector(DatabaseConnector):
    """PostgreSQL specific implementation."""

    def connect(self, config: Dict[str, Any]) -> None:
        raise NotImplementedError

    def claim_files_atomic(
        self, shard_ids: Iterable[int], pod_name: str, limit: int
    ) -> List[Dict[str, Any]]:
        """Atomic claim z FOR UPDATE SKIP LOCKED."""
        raise NotImplementedError
