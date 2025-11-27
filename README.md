# Datavision Easy Store (DES)

Datavision Easy Store (DES) bundles large numbers of small objects into compact, range-readable archive files designed for S3/HCP-style object storage. It targets workloads that need minimal object count, predictable shard naming, and fast random access via HTTP range requests.

## Features

- DES v1 binary format (`HEADER + DATA + META + INDEX + FOOTER`) with magic/version guards.
- Append-only writer (`DesWriter`) with optional external storage for big payloads, and local reader (`DesReader`) with batch reads.
- S3 reader (`S3DesReader`) that performs Range GET batching and handles external `_bigFiles/` content.
- Pluggable index caching (in-memory, Redis), plus Snowflake-like name generator for deterministic IDs.
- Daily sharded helpers (`DailyShardedDesStore`) and distributed packer pipeline (shard locks, S3 uploads, crash recovery).
- FastAPI name assignment service with Prometheus metrics and health probes; async PostgreSQL metadata layer.

## Architecture Overview

The core package (`des.core`) defines the DES file format, readers, writers, and cache backends. Around it sit:

- **Naming & routing:** Snowflake name generator (`des.utils.snowflake_name`) and consistent hashing utilities (`des.assignment`) to derive shard IDs.
- **Metadata & locking:** Async PostgreSQL connector (`des.db.connector`) manages shard locks and DES container metadata for worker coordination.
- **Packer pipeline:** Multi-shard packer (`des.packer.multi_shard_packer`) claims shard locks, fetches pending files from source DBs/S3 (`des.packer.source_provider`), writes DES containers, uploads to S3, and records checkpoints. Crash recovery (`des.packer.recovery`) cleans stale locks/containers.
- **Services & workers:** FastAPI name assignment service (`des.assignment.service`) and marker worker (`des.marker.file_marker`) that tags source catalog rows ready for packing.
- **Monitoring:** Prometheus metrics (`des.monitoring.metrics`) and health checks (`des.packer.health`).

## Requirements

- Python 3.11+
- AWS S3/HCP-compatible storage (for Range GETs and uploads)
- PostgreSQL (metadata DB for shard locks and container records)
- Optional caches/DB drivers: Redis, Oracle (`oracledb`), MSSQL (`pymssql`), MySQL/MariaDB (`PyMySQL`)
- Dependencies installed via `requirements.txt` (boto3, FastAPI, SQLAlchemy, asyncpg/psycopg2-binary, Prometheus client, etc.)

## Installation

From source:

```bash
git clone https://github.com/dariuszduszynski/Datavision-Easy-Store.git
cd Datavision-Easy-Store
python -m venv .venv && .\.venv\Scripts\activate  # or source .venv/bin/activate
pip install -e .
```

With developer extras:

```bash
pip install -r requirements-dev.txt
```

Docker build (packer-focused image):

```bash
docker build -t datavision-easy-store .
```

## Quickstart

Create and read a DES archive locally:

```python
from des import DesWriter, DesReader, InMemoryIndexCache

with DesWriter("2025-11-25.des") as writer:
    writer.add_file("report.pdf", pdf_bytes, meta={"mime": "application/pdf"})
    writer.add_file("log.txt", b"hello world", meta={"mime": "text/plain"})

reader = DesReader("2025-11-25.des", cache=InMemoryIndexCache())
print(reader.list_files())
assert reader.get_meta("report.pdf")["mime"] == "application/pdf"
data = reader.get_file("report.pdf")
```

Read straight from object storage using Range GET batching:

```python
from des import S3DesReader, InMemoryIndexCache

s3_reader = S3DesReader("my-bucket", "2025-11-25.des", cache=InMemoryIndexCache())
files = s3_reader.get_files_batch(["report.pdf", "log.txt"])
```

Run the name assignment service (FastAPI + Prometheus):

```bash
export DES_DB_URL="postgresql+asyncpg://des:des@localhost/des"
uvicorn des.assignment.service:app --host 0.0.0.0 --port 8000
```

Launch the multi-source packer (requires S3 and metadata DB configured):

```bash
export DES_DB_URL="postgresql+asyncpg://des:des@localhost/des"
export DES_ARCHIVE_BUCKET="des-archives"
python scripts/run_multi_shard_packer.py
```

## Configuration

- Environment variables:
  - `DES_DB_URL` (required for metadata DB), `DES_ARCHIVE_BUCKET` (S3 destination), `DES_NODE_ID`/`DES_WRAP_BITS`/`DES_SHARD_BITS` (naming and shard hash), `DES_NAME_PREFIX` (marker worker), `DES_MARKER_BATCH_SIZE`, `DES_MARKER_MAX_AGE_DAYS`, `DES_MARKER_INTERVAL_SECONDS`, `DES_PACKER_WORKDIR`, `DES_ASSIGN_HOST`.
  - AWS credentials are picked up by boto3 for S3 access.
- Source database configuration: `configs/source_databases.yaml` defines Oracle/MSSQL/MySQL/PostgreSQL sources, column mappings, claim statuses, and shard bits for the multi-source packer.
- DES format constants (magic, flags, big file thresholds) live in `src/des/core/constants.py`.

See `docs/configuration.md` for details and examples.

## Usage Scenarios

- **Library-only:** Use `DesWriter`/`DesReader` (or `S3DesReader`) to produce and consume DES archives in local tools or batch jobs.
- **Daily sharding:** `DailyShardedDesStore` groups writes by date and shard, producing files like `YYYY-MM-DD/00.des`.
- **Distributed ingestion:** Run the name assignment service to mint IDs, mark source catalog rows with `FileMarkerWorker`, and run the multi-shard packer to claim shards, fetch source files from S3 via database mappings, and upload DES containers to an archive bucket.
- **Operations:** Use `HealthChecker` and `/metrics` for readiness/liveness; `CrashRecoveryManager` to clean stale locks, claims, and partial containers after failures.

## Development

- Run tests: `pytest`
- Coverage: `pytest --cov=src/des --cov-report=term-missing`
- Linting: `ruff .`
- Type checks: `mypy`

## Status and Contributions

The package is marked **Alpha** (APIs and services may change). Issues and pull requests are welcome; please accompany changes with tests where possible.
