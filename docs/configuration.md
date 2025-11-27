# Configuration

DES components are configured primarily via environment variables and YAML files for source databases. This document lists the available knobs and defaults inferred from the codebase.

## Environment Variables

### Common

- `DES_DB_URL` (required) – Async SQLAlchemy URL for the metadata database (PostgreSQL recommended), e.g. `postgresql+asyncpg://user:pass@host:5432/des`.
- `AWS_*` / `AWS_PROFILE` – Standard boto3 credentials for S3/HCP access.
- `DES_ARCHIVE_BUCKET` – Target bucket for DES archives (used by packer and recovery); optional in name service health checks.
- `DES_PACKER_WORKDIR` – Local working directory for packer writers (default: OS temp `des_packer`).

### Naming and Sharding

- `DES_NODE_ID` – Node identifier byte for Snowflake names (default: `0`).
- `DES_WRAP_BITS` – Lower bits of epoch ms to embed in names (default: `32`).
- `DES_SHARD_BITS` – Number of bits used for shard hashing (default: `8` in name service; varies by source configs).
- `DES_NAME_PREFIX` – Prefix for generated names in the marker worker (defaults to `SnowflakeNameConfig.prefix`).

### Marker Worker (`des-marker`)

- `DES_MARKER_BATCH_SIZE` – Rows processed per batch (default: `100`).
- `DES_MARKER_MAX_AGE_DAYS` – Only mark rows older than this age (default: `1`).
- `DES_MARKER_INTERVAL_SECONDS` – Sleep when idle (default: `5`).

### Name Assignment Service

- `DES_ASSIGN_HOST` – Bind address for uvicorn (default: `127.0.0.1` if unset).

### Packer and Recovery

- `DES_ARCHIVE_BUCKET` – Destination bucket for uploads (required by `run_multi_shard_packer.py`).
- `DES_PACKER_WORKDIR` – Temporary working directory for per-shard writers.
- `DES_DB_URL` – Metadata DB URL (as above).
- Retry/backoff values are provided in code (`lock_ttl_seconds`, `batch_size`, `checkpoint` intervals) via the config dict passed to `MultiShardPacker`.

## YAML Configuration

### Source Databases (`configs/source_databases.yaml`)

Defines multiple upstream databases that feed the packer. Each entry contains:

- Connection parameters (`type`, `host`, `port`, `database`, `username`, `password`, optional `schema`/`charset`/pool settings).
- Table configuration (`name`, optional `schema`, column mappings for `id`, `s3_bucket`, `s3_key`, `size_bytes`, `status`, `created_at`, plus `metadata_columns`).
- Claim workflow (`status_pending_value`, `status_claimed_value`, optional `where_clause`, `claim_timeout_seconds`).
- Shard routing (`shard_bits`, optional `shard_key_column` to override hashing key).
- Batch size for claiming rows.

Example snippet:

```yaml
sources:
  - name: "documents-postgres"
    enabled: true
    connection:
      type: postgres
      host: postgres.example.com
      port: 5432
      database: documents
      username: des_readonly
      password: "${POSTGRES_PASSWORD}"
      schema: public
    table:
      name: document_storage
      schema: public
      columns:
        id: doc_id
        s3_bucket: bucket
        s3_key: s3_path
        size_bytes: size_bytes
        status: status
        created_at: created_at
        metadata_columns:
          document_type: doc_type
          category: category
      status_pending_value: "uploaded"
      status_claimed_value: "claimed"
    batch_size: 200
    shard_bits: 8
```

### Example Migration Config (`configs/example_config.yaml`)

Contains draft migration settings (number of shards/pods, batch size, worker counts, big file threshold, source and archive buckets). The file includes Polish comments and is intended as a sketch rather than a fully parsed config.

## DES Format Parameters

- Header/Footer magic and version are fixed in `des.core.constants`.
- Big file threshold defaults to `100 MB`; external files are uploaded to `_bigFiles/` under the same prefix.
- Index/cache sizes and batch gap thresholds are configurable per API call (`DesWriter` constructor, `S3DesReader.get_files_batch`).

## Configuration Tips

- Ensure `DES_DB_URL` points to a PostgreSQL instance with permissions to create/alter `des_shard_locks` and `des_containers`.
- When using Redis caching, initialize `RedisIndexCache` with an existing client and an explicit TTL to avoid stale indexes after archive updates.
- For multi-source packing, keep `shard_bits` consistent between the name service, marker worker, and source configs to avoid mismatched shard routing.
