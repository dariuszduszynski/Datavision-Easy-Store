# CLI and Services

This project exposes several entry points for running services and workers. Some legacy stubs remain; prefer the commands marked as current.

## Name Assignment Service (current)

FastAPI app that generates Snowflake-like names and shard IDs.

```bash
export DES_DB_URL="postgresql+asyncpg://des:des@localhost/des"
export DES_NODE_ID=1
export DES_SHARD_BITS=8
uvicorn des.assignment.service:app --host 0.0.0.0 --port 8000
```

Endpoints:
- `POST /assign` → `{"name": ..., "shard_id": ..., "day": "YYYY-MM-DD"}`
- `GET /health`, `/health/live`, `/health/ready` for probes
- `GET /metrics` for Prometheus scraping

## Marker Worker (current)

Marks catalog rows with DES metadata and shard assignments.

```bash
export DES_DB_URL="postgresql+asyncpg://des:des@localhost/des"
export DES_NAME_PREFIX=DES
des-marker  # console script -> des.cli.des_marker:main
```

Environment knobs: `DES_MARKER_BATCH_SIZE`, `DES_MARKER_MAX_AGE_DAYS`, `DES_MARKER_INTERVAL_SECONDS`, `DES_NODE_ID`.

## Multi-Source Packer (current)

Long-running packer that claims shards, downloads files from source DBs/S3, writes DES archives, and uploads them.

```bash
export DES_DB_URL="postgresql+asyncpg://des:des@localhost/des"
export DES_ARCHIVE_BUCKET="des-archives"
python scripts/run_multi_shard_packer.py
```

Configuration:
- Sources defined in `configs/source_databases.yaml`.
- Uses `DES_PACKER_WORKDIR` for local staging (default: temp dir).
- Uses S3 credentials from environment.

## Legacy/Stub Entry Points

- `des-name-assignment` (setup.py) → wraps `scripts/run_name_assignment.py`, which depends on `Config.from_env()` and an unimplemented `PostgresConnector` stub. Use the uvicorn command above instead.
- `des-packer` (setup.py) → points to `des.cli.main:cli`/`scripts/run_packer.py` targeting an earlier `MultiShardPacker` signature. It is currently a placeholder and may not run with the present code.
- `des` (setup.py) → references `des.cli.main:cli`, but `src/des/cli/main.py` is not present in the repository.

## Utility Script

- `buffer_to_des.py` – Demonstration script that packs files from a local directory into daily sharded DES containers using `DailyShardedDesStore`.
