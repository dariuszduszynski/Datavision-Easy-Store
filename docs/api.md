# API

Minimal FastAPI applications live under `des.assignment.service` (name assignment) and `des.api.server` (demo file API). This document focuses on the stub file API introduced for development and Docker Desktop testing.

## Endpoints (`des.api.server`)

- `GET /health` — Returns `{"status": "ok"}` for readiness probes.
- `GET /files/{file_id}` — Stub lookup. Returns a placeholder payload for `file_id="demo"`, otherwise `404 {"detail": "file not found"}`.

These endpoints are intentionally lightweight; they do not yet read from storage or a database.

## Running Locally

```bash
python scripts/run_api.py
# or
uvicorn des.api.server:app --host 0.0.0.0 --port 8000
```

Then open `http://localhost:8000/docs` for Swagger UI.

## Running with Docker Compose

The repo includes an `api` service in `docker-compose.yml`:

```yaml
api:
  build: .
  command: python scripts/run_api.py
  ports:
    - "8000:8000"
  environment:
    DES_DB_URL:
    DES_ARCHIVE_BUCKET:
    DES_NODE_ID:
    DES_SHARD_BITS:
    DES_WRAP_BITS:
    DES_PACKER_WORKDIR:
    DES_ASSIGN_HOST:
```

Start it with:

```bash
docker-compose up api
```

Docker Desktop should display the `8000:8000` mapping, and the docs will be available at `http://localhost:8000/docs`.
