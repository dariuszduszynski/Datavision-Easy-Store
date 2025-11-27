# src/des/db/source_connector.py

"""Universal database connector for source databases."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, cast

from des.assignment.hash_routing import consistent_hash
from des.db.source_config import DatabaseType, SourceDatabaseConfig
from sqlalchemy import MetaData, Table, and_, create_engine, select, text, update
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.sql.elements import ColumnElement

logger = logging.getLogger(__name__)


class SourceFile:
    """Simple dataclass for source file info (not SQLAlchemy model)."""

    def __init__(
        self,
        id: int,
        s3_bucket: str,
        s3_key: str,
        size_bytes: int,
        shard_id: int,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.id = id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.size_bytes = size_bytes
        self.shard_id = shard_id
        self.metadata = metadata or {}


class SourceDatabaseConnector:
    """
    Universal connector for source databases.

    Supports: Oracle, MSSQL, MySQL, PostgreSQL via SQLAlchemy Core.
    """

    def __init__(self, config: SourceDatabaseConfig):
        """
        Initialize connector.

        Args:
            config: Source database configuration
        """
        self.config = config
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
        self._table: Optional[Table] = None

    def connect(self) -> None:
        """Establish database connection."""
        if self.engine:
            logger.warning(f"Already connected to {self.config.name}")
            return

        conn_url = self.config.connection.get_connection_url()

        logger.info(
            f"Connecting to {self.config.connection.type.value} database: "
            f"{self.config.connection.host}:{self.config.connection.port}/"
            f"{self.config.connection.database}"
        )

        # Create engine with connection pooling
        self.engine = create_engine(
            conn_url,
            poolclass=QueuePool,
            pool_size=self.config.connection.pool_size,
            pool_recycle=self.config.connection.pool_recycle,
            pool_pre_ping=self.config.connection.pool_pre_ping,
            echo=False,  # Set to True for SQL debugging
            **self.config.connection.driver_options,
        )

        # Reflect table structure
        self._reflect_table()

        logger.info(f"✓ Connected to {self.config.name}")

    def disconnect(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info(f"Disconnected from {self.config.name}")

    def _reflect_table(self) -> None:
        """Reflect table structure from database."""
        table_name = self.config.table.name
        schema = self.config.table.table_schema

        # Reflect table
        self._table = Table(
            table_name, self.metadata, autoload_with=self.engine, schema=schema
        )

        logger.info(
            f"Reflected table: {self.config.table.full_table_name} "
            f"with {len(self._table.columns)} columns"
        )

    def _compute_shard_id(self, row: Dict[str, Any]) -> int:
        """
        Compute shard ID for a file.

        Args:
            row: Database row as dict

        Returns:
            Shard ID (0 to 2^shard_bits - 1)
        """
        # Use custom shard key column if specified
        if self.config.table.shard_key_column:
            key = row[self.config.table.shard_key_column]
        else:
            # Default: use S3 key
            key = row[self.config.table.columns.s3_key]

        return consistent_hash(str(key), n_bits=self.config.shard_bits)

    def _map_row_to_source_file(self, row: Dict[str, Any]) -> SourceFile:
        """
        Map database row to SourceFile object.

        Args:
            row: Database row as dict

        Returns:
            SourceFile instance
        """
        cols = self.config.table.columns

        # Extract required fields
        file_id = row[cols.id]
        s3_bucket = row[cols.s3_bucket]
        s3_key = row[cols.s3_key]
        size_bytes = row[cols.size_bytes]

        # Compute shard
        shard_id = self._compute_shard_id(row)

        # Extract metadata
        metadata = {}
        for meta_key, col_name in cols.metadata_columns.items():
            if col_name in row:
                metadata[meta_key] = row[col_name]

        # Add created_at if available
        if cols.created_at and cols.created_at in row:
            created = row[cols.created_at]
            if created:
                metadata["created_at"] = (
                    created.isoformat()
                    if isinstance(created, datetime)
                    else str(created)
                )

        return SourceFile(
            id=file_id,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            size_bytes=size_bytes,
            shard_id=shard_id,
            metadata=metadata,
        )

    def claim_pending_files(
        self, shard_id: int, holder_id: str, limit: int = 100
    ) -> List[SourceFile]:
        """
        Atomically claim pending files for a shard.

        Uses database-specific locking mechanisms:
        - PostgreSQL: FOR UPDATE SKIP LOCKED
        - MySQL/MariaDB: FOR UPDATE SKIP LOCKED (8.0+)
        - MSSQL: WITH (ROWLOCK, UPDLOCK, READPAST)
        - Oracle: FOR UPDATE SKIP LOCKED

        Args:
            shard_id: Target shard ID
            holder_id: Identifier of claiming process (pod name)
            limit: Maximum number of files to claim

        Returns:
            List of claimed SourceFile objects
        """
        if not self.engine or self._table is None:
            raise RuntimeError("Not connected to database")

        cols = self.config.table.columns
        table = self._table

        # Build WHERE conditions
        if cols.status is None:
            raise ValueError("status column must be configured for claiming files")
        conditions: List[ColumnElement[bool]] = [
            table.c[cols.status] == self.config.table.status_pending_value
        ]

        # Add custom WHERE clause if specified
        if self.config.table.where_clause:
            custom_where = text(self.config.table.where_clause)
            conditions.append(cast(ColumnElement[bool], custom_where))

        with self.engine.begin() as conn:
            # Step 1: SELECT with row locking
            select_stmt = select(table).where(and_(*conditions)).limit(limit)

            # Add database-specific locking
            db_type = self.config.connection.type

            if db_type == DatabaseType.POSTGRES:
                select_stmt = select_stmt.with_for_update(skip_locked=True)

            elif db_type in (DatabaseType.MYSQL, DatabaseType.MARIADB):
                select_stmt = select_stmt.with_for_update(skip_locked=True)

            elif db_type == DatabaseType.MSSQL:
                # MSSQL uses table hints
                select_stmt = select_stmt.with_hint(
                    table, "WITH (ROWLOCK, UPDLOCK, READPAST)"
                )

            elif db_type == DatabaseType.ORACLE:
                select_stmt = select_stmt.with_for_update(skip_locked=True)

            # Execute SELECT
            result = conn.execute(select_stmt)
            rows = result.fetchall()

            if not rows:
                logger.debug(f"No pending files found for shard {shard_id}")
                return []

            # Filter by shard_id (post-query because we can't compute hash in SQL)
            source_files = []
            claimed_ids = []

            for row in rows:
                row_dict = dict(row._mapping)
                source_file = self._map_row_to_source_file(row_dict)

                # Check if this file belongs to target shard
                if source_file.shard_id == shard_id:
                    source_files.append(source_file)
                    claimed_ids.append(source_file.id)

                # Stop if we have enough
                if len(source_files) >= limit:
                    break

            if not claimed_ids:
                logger.debug(f"No files matched shard {shard_id} in batch")
                return []

            # Step 2: UPDATE claimed files
            now = datetime.now(timezone.utc)

            update_stmt = (
                update(table)
                .where(table.c[cols.id].in_(claimed_ids))
                .values(
                    {
                        cols.status: self.config.table.status_claimed_value,
                        # Add claimed_by/claimed_at if columns exist
                    }
                )
            )

            # Add claimed_by/claimed_at if table has these columns
            if hasattr(table.c, "claimed_by"):
                update_stmt = update_stmt.values(claimed_by=holder_id)
            if hasattr(table.c, "claimed_at"):
                update_stmt = update_stmt.values(claimed_at=now)

            conn.execute(update_stmt)

            logger.info(
                f"Claimed {len(source_files)} files from {self.config.name} "
                f"for shard {shard_id} by {holder_id}"
            )

            return source_files

    def mark_files_packed(
        self, file_ids: List[int], des_names: List[str], container_id: int
    ) -> None:
        """
        Mark files as successfully packed.

        Args:
            file_ids: List of source file IDs
            des_names: List of DES names (snowflake IDs)
            container_id: DES container ID
        """
        if not self.engine or self._table is None:
            raise RuntimeError("Not connected to database")

        cols = self.config.table.columns
        table = self._table

        if cols.status is None:
            raise ValueError(
                "status column must be configured for marking packed files"
            )

        now = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            update_stmt = (
                update(table)
                .where(table.c[cols.id].in_(file_ids))
                .values({cols.status: "packed"})
            )

            # Add packed_at if column exists
            if hasattr(table.c, "packed_at"):
                update_stmt = update_stmt.values(packed_at=now)

            # Add des_name if column exists
            if hasattr(table.c, "des_name"):
                # Map file_id → des_name
                # For simplicity, update in loop (or use CASE WHEN for batch)
                for file_id, des_name in zip(file_ids, des_names, strict=False):
                    stmt = (
                        update(table)
                        .where(table.c[cols.id] == file_id)
                        .values({cols.status: "packed", "des_name": des_name})
                    )
                    if hasattr(table.c, "packed_at"):
                        stmt = stmt.values(packed_at=now)
                    if hasattr(table.c, "des_container_id"):
                        stmt = stmt.values(des_container_id=container_id)

                    conn.execute(stmt)
            else:
                # Batch update without des_name
                conn.execute(update_stmt)

        logger.info(f"Marked {len(file_ids)} files as packed in {self.config.name}")

    def mark_files_failed(self, file_ids: List[int], error_message: str) -> None:
        """
        Mark files as failed.

        Args:
            file_ids: List of file IDs
            error_message: Error description
        """
        if not self.engine or self._table is None:
            raise RuntimeError("Not connected to database")

        cols = self.config.table.columns
        table = self._table

        if cols.status is None:
            raise ValueError(
                "status column must be configured for marking failed files"
            )

        with self.engine.begin() as conn:
            update_stmt = (
                update(table)
                .where(table.c[cols.id].in_(file_ids))
                .values({cols.status: "failed"})
            )

            # Add error_message if column exists
            if hasattr(table.c, "error_message"):
                update_stmt = update_stmt.values(error_message=error_message[:500])

            conn.execute(update_stmt)

        logger.warning(
            f"Marked {len(file_ids)} files as failed in {self.config.name}: "
            f"{error_message}"
        )

    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about source files.

        Returns:
            Dict with counts by status
        """
        if not self.engine or self._table is None:
            raise RuntimeError("Not connected to database")

        cols = self.config.table.columns
        table = self._table

        if cols.status is None:
            raise ValueError("status column must be configured for statistics")

        with self.engine.connect() as conn:
            # Count by status
            from sqlalchemy import func

            stmt = select(table.c[cols.status], func.count().label("count")).group_by(
                table.c[cols.status]
            )

            result = conn.execute(stmt)
            stats = {row[0]: row[1] for row in result}

        return stats

    def __enter__(self) -> "SourceDatabaseConnector":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Context manager exit."""
        self.disconnect()
