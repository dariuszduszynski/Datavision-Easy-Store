# Components

This document summarizes the main modules and public-facing classes/functions in the repository.

## Core (`src/des/core`)

- **`constants.py`** — DES magic numbers, struct layouts, size limits, flags, default thresholds (big file size, batch gaps), and `_bigFiles` directory name.
- **`models.py`** — Data classes for `IndexEntry`, `DesStats`, `DesFooter`, and `ExternalFileInfo`, plus helpers for flag inspection and human-readable stats.
- **`cache.py`** — Cache interface `IndexCacheBackend` with implementations: `InMemoryIndexCache` (thread-safe, optional TTL, LRU), `RedisIndexCache` (JSON serialization with TTL support), and `NullCache` (no-op).
- **`des_writer.py`** — Append-only writer that validates filenames, writes DATA/META/INDEX/FOOTER regions, externalizes big files to S3 when configured, and exposes `get_stats()` / `get_external_files()`.
- **`des_reader.py`** — Local reader that validates the footer, lazily loads and caches the index, retrieves files/metadata, and performs batch reads with gap merging.
- **`s3_des_reader.py`** — S3-oriented reader that mirrors `DesReader` API using Range GETs, external file fetches, and optional index cache keyed by bucket/key/etag.

## Utilities (`src/des/utils`)

- **`snowflake_name.py`** — `SnowflakeNameGenerator` and `SnowflakeNameConfig` for deterministic IDs: `<prefix>_YYYYMMDD_(F..._CC)`.
- **`retry.py`** — Async retry decorator with configurable attempts and backoff, used around packer storage/DB operations.
- **`logging.py`** — Thin wrappers for structlog-compatible logging setup used by workers.
- **`signals.py`** — Signal helpers for graceful shutdown of long-running processes.

## Assignment & Routing (`src/des/assignment`)

- **`hash_routing.py`** — `consistent_hash(value, n_bits)` returning shard IDs from Snowflake names.
- **`shard_router.py`** — `ShardAssignment` helper to map pods to shard sets and compute shard IDs.
- **`service.py`** — FastAPI application exposing `/assign`, `/health/*`, `/metrics`; uses Snowflake generator and hashing to return `{name, shard_id, day}`. Includes `NameAssignmentService` runner wrapper.

## API (`src/des/api`)

- **`server.py`** — Minimal FastAPI app exposing `/health` and `/files/{file_id}` stub endpoint (returns placeholder payload for `file_id="demo"`).

## Database & Sources (`src/des/db`)

- **`connector.py`** — Async PostgreSQL metadata connector (`DesDbConnector`) and models: `ShardLock` (distributed lock) and `DesContainer` (container metadata). Provides lock acquisition/renewal/release and table creation.
- **`source_config.py`** — Pydantic models for describing upstream databases (`SourceDatabaseConfig`, `MultiSourceConfig`), connection details, shard routing bits, and column mappings.
- **`source_connector.py`** — `SourceDatabaseConnector` that reflects source tables, claims rows with dialect-specific locking, computes shard IDs, and updates statuses (`packed`/`failed`). Supports Oracle, MSSQL, MySQL/MariaDB, and PostgreSQL drivers.
- **`catalog.py`** — SQLAlchemy model for catalog rows used by the marker worker.
- **`postgres.py`** — Stub for legacy Postgres connector API (not implemented).

## Packer & Storage (`src/des/packer`)

- **`daily_sharded_store.py`** — `DailyShardedDesStore` convenience builder that rolls containers per day/shard using `DesWriter`.
- **`multi_shard_packer.py`** — Main orchestration loop: acquires shard locks, keeps per-shard writers, claims pending files from a source provider, batches writes, checkpoints metadata, uploads archives, and rolls over daily. Includes heartbeat manager and retry classification for DB/S3 errors.
- **`source_provider.py`** — `MultiSourceFileProvider` that connects to multiple source DBs, claims pending files (via `SourceDatabaseConnector`), downloads S3 objects, and returns `PendingFile` records to the packer. Can mark files as packed/failed.
- **`storage.py`** — `S3StorageBackend` async wrapper around boto3 uploads with optional key prefixing.
- **`health.py`** — `HealthChecker` producing readiness reports across DB, S3, shard locks, and source connectors.
- **`recovery.py`** — `CrashRecoveryManager` to unclaim stale rows, clean partial containers, release expired locks, validate DES objects in S3, and delete orphaned uploads.
- **`heartbeat.py`, `rollover.py`, `worker.py`** — Lightweight helpers for heartbeats/rollover scaffolding used by packer tests and future extensions.

## Marker Worker (`src/des/marker`)

- **`file_marker.py`** — `FileMarkerWorker` that scans catalog rows, assigns Snowflake names/hashes/shard IDs, and marks rows for DES packing (`des_status = DES_TODO`), respecting max age and batch size. Intended to run continuously (`des-marker` entrypoint).

## Scripts & Entry Points (`scripts/`, `setup.py`)

- **`run_multi_shard_packer.py`** — Wires `MultiShardPacker` with S3 clients, metadata DB, and YAML source config for production ingestion.
- **`run_name_assignment.py`** — Legacy wrapper starting `NameAssignmentService` using `Config.from_env()` and a stub `PostgresConnector`.
- **`run_packer.py`** — Legacy packer entrypoint targeting an older constructor signature; kept for reference.
- **`run_api.py`** — Starts the FastAPI stub API via uvicorn on port 8000.
- **Console scripts (setup.py):** `des-marker` (marker worker), `des-name-assignment` (name service), `des-packer` (legacy), and `des` (CLI stub; `des/cli/main.py` is not present).
