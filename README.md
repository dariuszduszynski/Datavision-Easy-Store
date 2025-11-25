# DatavisionEasyStore (DES)

**DatavisionEasyStore (DES)** is a lightweight, binary container format designed for efficiently storing **thousands of small files** inside a **single object** (e.g., on S3, HCP, Ceph RGW).
It provides **fast point-lookups**, minimal I/O overhead, and requires **no external database**.
Perfect for workflows where “daily batches” of files are merged into a compact, self-contained archive.

---

## ✨ Key Features

* **Fast point lookup**
  DES stores a binary index with absolute byte ranges, allowing direct `Range GET` access.

* **Two-region layout:**

  * **DATA Region** — raw bytes of all files
  * **META Region** — JSON metadata per file + binary index
  * **FOOTER** — global offsets for O(1) access to index and meta

* **Zero external dependencies**
  All index and metadata are inside the file. No DB required.

* **S3/HCP friendly**
  DES works perfectly with HTTP range requests — ideal for object stores with high request cost.

* **Simple Python API**
  `AddFile`, `GetFile`, `ListFiles`, `GetIndex`.

* **Append-only, immutable container**
  Designed for daily batch creation and fast read access.

---

## 📦 File Structure

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

### Footer contains:

* `data_start`, `data_length`
* `meta_start`, `meta_length`
* `index_start`, `index_length`
* `file_count`
* magic/version for validation

Thanks to the footer, a reader can locate the index using **one final-range request**.

---

## 🚀 Quick Example

### Writing a DES file

```python
from dv_easystore import DesWriter

with DesWriter("2025-11-25.des") as w:
    w.add_file("report.pdf", pdf_bytes, meta={"mime": "application/pdf"})
    w.add_file("log.txt", b"hello world", meta={"mime": "text/plain"})
```

### Reading from a DES file

```python
from dv_easystore import DesReader

r = DesReader("2025-11-25.des")

print(r.list_files())
# ['report.pdf', 'log.txt']

data = r.get_file("report.pdf")
meta = r.get_meta("report.pdf")

index = r.get_index()
```

---

## 🧩 Python API

### `DesWriter(path)`

| Method                           | Description                                                |
| -------------------------------- | ---------------------------------------------------------- |
| `add_file(name, bytes, meta={})` | Append a file to the DATA region and its metadata to META. |
| `close()`                        | Finalize META, build INDEX, write FOOTER.                  |
| Context manager                  | Automatically calls `close()`.                             |

### `DesReader(path)`

| Method               | Description                               |
| -------------------- | ----------------------------------------- |
| `list_files()`       | Return all filenames in the index.        |
| `get_file(name)`     | Read file bytes using stored byte ranges. |
| `get_meta(name)`     | Retrieve JSON metadata.                   |
| `get_index()`        | Return full index entries.                |
| `__contains__(name)` | Check if a file exists in the container.  |

---

## 🏎 Performance Characteristics

* **O(1) lookup**: index is binary and fixed-layout.
* **Minimal I/O**:

  * First access → 2 range reads (footer + index)
  * Subsequent reads → 1 range per file
* **Ideal for object storage** pricing models
  where request count matters more than bandwidth.

---

## 🧱 Format Versioning

* `HEADER_MAGIC = "DESHEAD1"`
* `FOOTER_MAGIC = "DESFOOT1"`
* `VERSION = 1`

Future versions will remain backwards-compatible using magic/version fields.

---

## 🗑 What About Deletion?

v1 is append-only.
Logical deletion (`flags`) and compaction will be added in v2.

---

## 📜 License

MIT (or choose your preferred license).
