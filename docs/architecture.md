# Architecture

The project is centered on a compact binary container format and a set of services that orchestrate ingest, naming, and storage across shards and days.

## Core Layers

- **Format & I/O (`des.core`):** Defines DES v1 constants, flags, and struct layouts. `DesWriter` appends files, writes metadata, indexes, and footer, optionally externalizing oversized payloads to S3 under `_bigFiles/`. `DesReader` reads local archives with batch-aware range loading; `S3DesReader` mirrors the API using Range GETs and external file fetches.
- **Caching:** `InMemoryIndexCache` and `RedisIndexCache` store parsed index entries keyed by archive identity (bucket/key/etag or file mtime) to avoid repeated footer/index reads.
- **Naming & routing:** `SnowflakeNameGenerator` creates IDs with prefix, date, node ID, and checksum bits; `consistent_hash` turns IDs into shard numbers using a configurable bit width.

## Distributed Ingest Pipeline

1. **Source discovery:** `MultiSourceFileProvider` connects to one or more upstream databases described in `configs/source_databases.yaml`. Each connector claims pending rows (database-specific locking), derives shard IDs from S3 keys or configured shard columns, and downloads referenced S3 objects.
2. **Shard coordination:** `DesDbConnector` (async SQLAlchemy) stores shard locks (`des_shard_locks`) and container metadata (`des_containers`). `MultiShardPacker` iterates through configured shard IDs, acquiring locks with TTL-based heartbeats.
3. **Packing:** For each shard/day, the packer keeps an open `DesWriter`, appends claimed files, updates counters, and periodically checkpoints metadata rows. When a day rolls or a writer is finalized, it uploads the archive via `S3StorageBackend` and marks the container uploaded.
4. **Recovery & health:** `CrashRecoveryManager` can release stale claims, clean up partial containers, and validate uploaded archives. `HealthChecker` probes DB, S3, shard locks, and source connectors for readiness reports and Prometheus metrics.

## Services and Workers

- **Name assignment service:** FastAPI app (`des.assignment.service`) exposes `/assign` to return a Snowflake name, shard ID, and day; `/metrics` for Prometheus; `/health/*` for probes. Configured via environment variables for node ID, shard bits, DB URL, and optional S3 bucket.
- **Marker worker:** `FileMarkerWorker` updates catalog rows with DES names/hashes and marks them for packing, intended to run as a periodic worker (`des-marker` entrypoint).
- **Packer runners:** `scripts/run_multi_shard_packer.py` wires the packer with S3 clients, metadata DB, and YAML source config for long-running ingestion. Legacy `run_packer.py` references an older Config/Postgres interface and is currently a stub.

## Data Flow

```
Source DB rows -> claimed by SourceDatabaseConnector -> file bodies fetched from S3
  -> MultiShardPacker (per shard lock) -> DesWriter builds archive
    -> upload to archive bucket via S3StorageBackend
    -> metadata recorded in des_containers (PostgreSQL)
    -> clients read via DesReader (local) or S3DesReader (Range GET + cache)
```

## Design Considerations

- **Append-only containers:** Simplifies integrity checking; footer holds byte offsets for validation.
- **External big files:** Large objects are stored separately but indexed, keeping DES archives compact.
- **Shard locks with heartbeat:** Prevents concurrent writers on the same shard; TTL-based renewal tolerates transient failures.
- **Batching and retries:** Packer batches files per shard, checkpoints progress, and wraps S3/DB operations with retry hooks for transient errors.
- **Pluggable sources:** Source connectors rely on SQLAlchemy Core reflection and configurable column mappings, making the packer adaptable to heterogeneous databases.
