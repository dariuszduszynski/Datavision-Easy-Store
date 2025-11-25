# DatavisionEasyStore (DES)

**DatavisionEasyStore (DES)** is a lightweight, binary container format for storing thousands of small files inside a single object (S3/HCP/Ceph RGW-friendly). It delivers fast point-lookups, minimal I/O, and needs no external database. Great for daily batches where many tiny files are merged into a compact, self-contained archive.

---

## Key Features

- **Fast point lookup** — binary index with absolute byte ranges enables direct `Range GET`.
- **Two-region layout** — DATA (raw bytes), META (JSON per file + binary index), FOOTER (offsets/lengths).
- **Zero external deps** — all index/metadata live inside the file.
- **S3/HCP friendly** — optimized for HTTP range requests.
- **Simple Python API** — `add_file`, `get_file`, `list_files`, `get_index`, `get_files_batch`.
- **Append-only** — immutable container, ideal for daily batch creation.
- **Index cache** — in-memory cache or pluggable backend (Redis adapter provided).
- **Daily sharded builder** — helper to shard many files into daily containers.

---

## File Structure

```text
+----------------------+   offset = 0
|       HEADER         |   (magic, version)
+----------------------+   offset = data_start
|                      |
|      DATA REGION     |   raw file bytes (concatenated)
|                      |
+----------------------+   offset = meta_start
|                      |
|      META REGION     |   metadata + binary index
|                      |
+----------------------+   offset = footer_start
|        FOOTER        |   absolute offsets & lengths
+----------------------+   EOF
```

Footer contains: `data_start`, `data_length`, `meta_start`, `meta_length`, `index_start`, `index_length`, `file_count`, magic/version.

Thanks to the footer, a reader can locate the index using **one final range request**.

---

## Quick Example

### Writing a DES file

```python
from des_core import DesWriter

with DesWriter("2025-11-25.des") as w:
    w.add_file("report.pdf", pdf_bytes, meta={"mime": "application/pdf"})
    w.add_file("log.txt", b"hello world", meta={"mime": "text/plain"})
```

### Reading from a DES file

```python
from des_core import DesReader, InMemoryIndexCache

r = DesReader("2025-11-25.des", cache=InMemoryIndexCache())

print(r.list_files())  # ['report.pdf', 'log.txt']
data = r.get_file("report.pdf")
meta = r.get_meta("report.pdf")
index = r.get_index()
```

### S3 range reader + batch fetch

```python
from s3_des_reader import S3DesReader, InMemoryIndexCache

s3r = S3DesReader("my-bucket", "2025-11-25.des", cache=InMemoryIndexCache())
batch = s3r.get_files_batch(["report.pdf", "log.txt"])
# batch uses one range when files sit next to each other
```

---

## Python API

### `DesWriter(path)`

| Method                           | Description                                                |
| -------------------------------- | ---------------------------------------------------------- |
| `add_file(name, bytes, meta={})` | Append a file to the DATA region and its metadata to META. |
| `close()`                        | Finalize META, build INDEX, write FOOTER.                  |
| Context manager                  | Automatically calls `close()`.                             |

### `DesReader(path, cache=None)`

- `list_files()`, `get_file(name)`, `get_meta(name)`, `get_index()`, `__contains__`.
- Optional `cache`/`cache_key` to reuse the binary index (in-memory or pluggable backend).

### `S3DesReader(bucket, key, cache=None)`

- Same surface as `DesReader`, but operates via S3 `Range GET`.
- `get_files_batch(names, max_gap_size=1MB)` groups adjacent files to minimize requests.
- Validates footer magic/version and object size.

### Cache backends

- `InMemoryIndexCache(compress=False)` — simple in-process cache; can gzip JSON to reduce footprint.
- `RedisIndexCache(client, ttl_seconds=None, compress=True)` — adapter for a Redis client (optional dependency), JSON-serialized index; gzip on by default to shrink payload. TTL optional.

### Daily sharded store

- `DailyShardedDesStore` — shards files by hash into daily directories (`YYYY-MM-DD/<shard>.des`), uses Snowflake-like names per file.
- `iter_daily_des_files(base_dir, day)` — iterate DES files for a given day.

### Snowflake-like name generator

- `SnowflakeNameGenerator` — generates `<PREFIX>_YYYYMMDD_(FFFFFFFFFFFF_CC)` with node_id/wrap_bits; prefix now validated to ASCII letters/digits only.

---

## Performance Characteristics

- **O(1) lookup**: binary, fixed-layout index.
- **Minimal I/O**:
  - First access → 2 range reads (footer + index)
  - Subsequent reads → 1 range per file
  - Batch reads → 1 range per group of adjacent files
- Ideal for object storage pricing where request count matters more than bandwidth.

---

## Format Versioning

- `HEADER_MAGIC = "DESHEAD1"`
- `FOOTER_MAGIC = "DESFOOT1"`
- `VERSION = 1`

Future versions remain backwards-compatible via magic/version fields.

---

## Deletion?

v1 is append-only. Logical deletion (`flags`) and compaction are planned for v2.

---

## License

MIT.
