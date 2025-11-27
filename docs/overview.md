# Overview

Datavision Easy Store (DES) is a Python toolkit for turning large volumes of small objects into compact DES archives that are efficient to store and read via HTTP range requests on S3/HCP-class storage. The project combines a minimal binary container format with distributed helpers for naming, shard routing, and continuous packing from multiple upstream databases.

## What DES Does

- Bundles many small files into append-only DES containers while preserving per-file metadata.
- Supports external storage of oversized payloads (`_bigFiles/`) without losing index entries.
- Reads archives locally or directly from S3 with batched Range GETs.
- Generates deterministic Snowflake-like IDs and consistent shard assignments for horizontal scaling.
- Coordinates shard locks and container metadata in PostgreSQL, exposing metrics and health endpoints for operations.

## Typical Use Cases

- Reducing object counts for S3/HCP by merging millions of tiny files into daily shard archives.
- Providing fast random access to archived files (logs, documents, media derivatives) through DES readers.
- Running ingestion pipelines where multiple source databases feed a shared archive bucket via the multi-shard packer.
- Operating services that mint unique names (`/assign` API), monitor packer health, and recover from crashes or stale locks.

## Relation to External Systems

- **Object storage:** Uses S3 (or compatible) for Range GET reads and uploads; stores external large files alongside archives.
- **Databases:** PostgreSQL stores shard locks and container metadata; optional source connectors support Oracle, MSSQL, MySQL/MariaDB, and PostgreSQL as feeders.
- **Caching:** Redis can back the index cache for S3 readers; in-memory cache is available for single-process deployments.
- **Monitoring:** Prometheus metrics and FastAPI health endpoints integrate with Kubernetes-style probes.
