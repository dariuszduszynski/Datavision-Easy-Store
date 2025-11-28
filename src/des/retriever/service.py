from __future__ import annotations

import asyncio
import os
from typing import Optional

import boto3
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import Counter, Histogram, generate_latest

from des.config.retriever_config import RetrieverConfig
from des.retriever.cache_manager import build_cache
from des.retriever.file_handler import FileHandler
from des.utils.logging import get_logger

logger = get_logger(__name__)

app = FastAPI(title="DES Retriever Service")

REQUESTS = Counter(
    "des_retriever_requests_total",
    "Retriever requests",
    ["method", "status"],
)
CACHE_HITS = Counter(
    "des_retriever_cache_hits_total",
    "Cache hits",
)
CACHE_MISSES = Counter(
    "des_retriever_cache_misses_total",
    "Cache misses",
)
S3_LATENCY = Histogram(
    "des_retriever_s3_latency_seconds",
    "S3 latency seconds",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
FILE_SIZE = Histogram(
    "des_retriever_file_size_bytes",
    "File size bytes",
    buckets=(512, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304),
)


def _build_handler() -> FileHandler:
    try:
        cfg = RetrieverConfig.from_env()
    except Exception:
        # Fallback for local tests; bucket is required for real usage.
        cfg = RetrieverConfig(
            s3_bucket="dummy-bucket",
            s3_prefix=None,
            cache_backend="memory",
            redis_url=None,
            cache_ttl=3600,
            shard_bits=8,
        )
    cache = build_cache(cfg)
    s3_client = boto3.client("s3")
    return FileHandler(s3_client=s3_client, config=cfg, cache_backend=cache)


FILE_HANDLER = _build_handler()
CONFIG = FILE_HANDLER.config


@app.get("/files/{file_name}")
async def get_file(file_name: str):
    try:
        content, info = await FILE_HANDLER.get_file(file_name)
    except FileNotFoundError:
        REQUESTS.labels(method="GET", status="404").inc()
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as exc:  # noqa: BLE001
        REQUESTS.labels(method="GET", status="503").inc()
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    REQUESTS.labels(method="GET", status="200").inc()
    FILE_SIZE.observe(info["size"])
    headers = {
        "X-DES-Container": info["container"],
        "X-DES-Shard-Id": str(info["shard_id"]),
        "X-DES-Size-Bytes": str(info["size"]),
        "X-DES-Is-External": str(info.get("is_external", False)).lower(),
    }
    return Response(
        content=content,
        headers=headers,
        media_type="application/octet-stream",
    )


@app.head("/files/{file_name}")
async def check_file_exists(file_name: str):
    try:
        exists, info = await FILE_HANDLER.file_exists(file_name)
    except Exception as exc:  # noqa: BLE001
        REQUESTS.labels(method="HEAD", status="503").inc()
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    status = "200" if exists else "404"
    REQUESTS.labels(method="HEAD", status=status).inc()
    headers = {
        "X-DES-Exists": str(exists).lower(),
    }
    if info.get("container"):
        headers["X-DES-Container"] = info["container"]
    if info.get("size") is not None:
        headers["X-DES-Size-Bytes"] = str(info["size"])
    return Response(status_code=200 if exists else 404, headers=headers)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
async def ready() -> dict[str, str]:
    # Minimal readiness: ensure S3 can be listed
    try:
        await asyncio.to_thread(
            lambda: boto3.client("s3").list_buckets()
        )
        return {"status": "ready"}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type="text/plain")
