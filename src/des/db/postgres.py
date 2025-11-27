"""PostgreSQL implementation."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Iterable, List

from des.db.base import DatabaseConnector

logger = logging.getLogger(__name__)


class PostgresConnector(DatabaseConnector):
    """PostgreSQL specific implementation."""

    def connect(self, config: Dict[str, Any]) -> None:
        self._config = config
        self._engine = None
        self._sessionmaker = None
        logger.debug(
            "PostgresConnector.connect() stub invoked; "
            "no real DB connection established",
            extra={"config": config},
        )

    def claim_files_atomic(
        self, shard_ids: Iterable[int], pod_name: str, limit: int
    ) -> List[Dict[str, Any]]:
        """Atomic claim z FOR UPDATE SKIP LOCKED."""
        raise NotImplementedError

    @asynccontextmanager
    async def session_factory(self):
        """Dev stub session factory providing a dummy async session."""
        logger.debug(
            "PostgresConnector.session_factory stub invoked; returning dummy session"
        )

        class DummyResult:
            def __init__(self) -> None:
                self.rowcount = 0

            def scalar_one(self) -> int:
                logger.debug("DummyResult.scalar_one stub invoked; returning 0")
                return 0

            def scalar_one_or_none(self) -> None:
                logger.debug(
                    "DummyResult.scalar_one_or_none stub invoked; returning None"
                )
                return None

            def first(self) -> None:
                logger.debug("DummyResult.first stub invoked; returning None")
                return None

            def all(self) -> List[Any]:
                logger.debug("DummyResult.all stub invoked; returning []")
                return []

            def scalars(self) -> "DummyResult":
                logger.debug("DummyResult.scalars stub invoked; returning empty list")
                return self

        class DummyDialect:
            name = "stub"

        class DummyBind:
            def __init__(self) -> None:
                self.dialect = DummyDialect()

        class DummyTransaction:
            def __init__(self, session: "DummySession") -> None:
                self.session = session

            async def __aenter__(self) -> "DummySession":
                logger.debug("DummyTransaction.__aenter__ stub invoked")
                return self.session

            async def __aexit__(self, exc_type, exc, tb) -> bool:
                logger.debug("DummyTransaction.__aexit__ stub invoked")
                return False

        class DummySession:
            def __init__(self) -> None:
                self._bind = DummyBind()

            def get_bind(self):
                return self._bind

            async def execute(self, *args: Any, **kwargs: Any) -> DummyResult:
                logger.debug(
                    "DummySession.execute stub invoked; sql_args=%r sql_kwargs=%r",
                    args,
                    kwargs,
                )
                return DummyResult()

            async def scalar(self, *args: Any, **kwargs: Any) -> None:
                logger.debug("DummySession.scalar stub invoked")
                return None

            async def scalars(self, *args: Any, **kwargs: Any) -> List[Any]:
                logger.debug("DummySession.scalars stub invoked; returning []")
                return []

            async def commit(self) -> None:
                logger.debug("DummySession.commit stub invoked")

            async def rollback(self) -> None:
                logger.debug("DummySession.rollback stub invoked")

            def add(self, *args: Any, **kwargs: Any) -> None:
                logger.debug(
                    "DummySession.add stub invoked; add_args=%r add_kwargs=%r",
                    args,
                    kwargs,
                )

            def begin(self) -> DummyTransaction:
                logger.debug("DummySession.begin stub invoked")
                return DummyTransaction(self)

        session = DummySession()
        try:
            yield session
        finally:
            logger.debug("PostgresConnector.session_factory context exited")

    def get_files_to_migrate(self, limit: int) -> List[Dict[str, Any]]:
        """Stub: return no files to migrate."""
        logger.debug(
            "PostgresConnector.get_files_to_migrate stub invoked",
            extra={"limit": limit},
        )
        return []

    async def try_acquire_shard_lock(
        self, shard_id: int, holder_id: str, ttl_seconds: int
    ) -> bool:
        """Dev stub: pretend shard lock is always acquired."""
        logger.debug(
            "PostgresConnector.try_acquire_shard_lock stub invoked; returning True",
            extra={
                "shard_id": shard_id,
                "holder_id": holder_id,
                "ttl_seconds": ttl_seconds,
            },
        )
        return True

    def update_status(self, file_id: int, status: str, **kwargs: Any) -> None:
        """Stub: log status update request."""
        logger.debug(
            "PostgresConnector.update_status stub invoked",
            extra={"file_id": file_id, "status": status, "extra": kwargs},
        )
