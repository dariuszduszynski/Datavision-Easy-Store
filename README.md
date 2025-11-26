# Datavision Easy Store (DES)

Datavision Easy Store packs many small files into compact DES archive containers optimized for S3/HCP-style object storage. The focus is minimal request count, fast random access via byte ranges, and a simple, language-agnostic format.

## Overview

- **Purpose:** Merge thousands of tiny files into large DES objects so they can be stored and read efficiently with HTTP Range GET.
- **Architecture:** Core Python library for the DES v1 format (writer/reader, S3 range reader, caching, sharded helpers). Optional distributed pieces include a FastAPI name assignment service with consistent hashing, an async Postgres layer for shard locks and DES metadata, multi-shard packer workers, Prometheus metrics, and Kubernetes manifests for deployment.

## Implemented Features

- **DES v1 binary format:** HEADER + DATA + META + INDEX + FOOTER with magic/version.
- **Writers/Readers:** `DesWriter` (append-only creation) and `DesReader` (local reads).
- **S3 range reader:** `S3DesReader` batches adjacent Range GETs.
- **Index cache backends:** In-memory and Redis adapters.
- **Daily sharding helper:** `DailyShardedDesStore` for day-based rollover and sharded containers.
- **Snowflake-like naming:** `SnowflakeNameGenerator` with configurable prefix/node/wrap bits.
- **External "big file" support:** `FLAG_IS_EXTERNAL` keeps large payloads outside the main DES blob while preserving index entries.

## Repository Structure

```text
configs/                       # Configuration samples
docs/                          # Documentation assets
scripts/                       # CLI entrypoints and helpers
k8s/                           # Example Kubernetes manifests
src/des/                       # DES package (src layout)
  assignment/                  # Name assignment service + hashing
  cli/                         # CLI entrypoints
  config/                      # Config helpers
  core/                        # DES core readers/writers and models
  db/                          # Async Postgres connector and models
  monitoring/                  # Prometheus metrics
  packer/                      # Shard packer logic
  storage/                     # Storage helpers
  utils/                       # Snowflake name generator
tests/                         # Test suite
buffer_to_des.py               # Example buffer -> DES pipeline
setup.py                       # Packaging (src layout)
README.md                      # You are here
```

## Usage Example

Create and read a DES archive locally:

```python
from des import DesWriter, DesReader, InMemoryIndexCache

# Write
with DesWriter("2025-11-25.des") as w:
    w.add_file("report.pdf", pdf_bytes, meta={"mime": "application/pdf"})
    w.add_file("log.txt", b"hello world", meta={"mime": "text/plain"})

# Read
r = DesReader("2025-11-25.des", cache=InMemoryIndexCache())
print(r.list_files())
meta = r.get_meta("report.pdf")
data = r.get_file("report.pdf")
```

Read from object storage with Range GET:

```python
from des import S3DesReader, InMemoryIndexCache

s3r = S3DesReader("my-bucket", "2025-11-25.des", cache=InMemoryIndexCache())
batch = s3r.get_files_batch(["report.pdf", "log.txt"])
```

## Roadmap

- Harden the `src/des/` package structure and public API surface.
- NameAssignmentService (FastAPI) with consistent hashing for shard routing.
- Database layer for shard locks and DES container metadata (PostgreSQL/async SQLAlchemy).
- MultiShardPacker worker flow for continuous packing and day rollover.
- Prometheus metrics and Kubernetes deployment examples for assignment and packer services.

## License

MIT.
