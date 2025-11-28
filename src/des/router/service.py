from __future__ import annotations

import asyncio
import hashlib
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from des.config.retriever_config import RouterConfig
from des.router.routing_table import RoutingStrategy, RoutingTable
from des.router.health import readiness
from des.utils.logging import get_logger

logger = get_logger(__name__)

app = FastAPI(title="DES Router API")

# Metrics
ROUTER_REQUESTS = Counter(
    "des_router_requests_total",
    "Router requests",
    ["method", "status"],
)
ROUTER_FAILURES = Counter(
    "des_router_retriever_failures_total",
    "Retriever failures",
    ["retriever_id"],
)
ROUTER_LATENCY = Histogram(
    "des_router_latency_seconds",
    "Router latency seconds",
    ["method", "retriever_id"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
ROUTER_RETRIES = Histogram(
    "des_router_retry_count",
    "Retry attempts per request",
    buckets=(0, 1, 2, 3),
)
ROUTER_HEALTHY = Gauge(
    "des_router_healthy_retrievers",
    "Number of healthy retrievers",
)


def _load_table() -> tuple[RoutingTable, RouterConfig]:
    try:
        cfg = RouterConfig.from_env()
    except Exception:
        # Fallback for local tests without env
        cfg = RouterConfig(
            retriever_endpoints=["http://localhost:8001"],
            routing_strategy="hash_byte",
            request_timeout=30,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout=30,
            max_retries=3,
        )
    strategy = RoutingStrategy(cfg.routing_strategy)
    table = RoutingTable(
        retrievers=cfg.retriever_endpoints,
        strategy=strategy,
        circuit_breaker_threshold=cfg.circuit_breaker_threshold,
        circuit_breaker_timeout=cfg.circuit_breaker_timeout,
    )
    return table, cfg


ROUTING_TABLE, CONFIG = _load_table()


def _hash_byte(file_name: str, hash_hex: Optional[str], hash_byte_param: Optional[str]) -> int:
    if hash_byte_param:
        try:
            return int(hash_byte_param, 0)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid hash_byte") from exc
    if hash_hex:
        if len(hash_hex) < 2:
            raise HTTPException(status_code=400, detail="hash too short")
        return int(hash_hex[:2], 16)
    digest = hashlib.sha256(file_name.encode("utf-8")).hexdigest()
    return int(digest[:2], 16)


async def _proxy_request(
    method: str, url: str, timeout: float, headers: dict[str, str]
) -> httpx.Response:
    async with httpx.AsyncClient(timeout=timeout) as client:
        if method == "GET":
            return await client.get(url)
        return await client.head(url)


async def _route(
    method: str, file_name: str, hash_val: Optional[str], hash_byte_val: Optional[str]
) -> Response:
    target_byte = _hash_byte(file_name, hash_val, hash_byte_val)
    primary = ROUTING_TABLE.get_target_retriever(
        file_name=file_name,
        hash_value=hash_val,
        hash_byte=target_byte,
    )
    retrievers = [primary] + ROUTING_TABLE.get_fallback_retrievers(exclude=primary.id)

    last_exc: Optional[Exception] = None
    for attempt, retriever in enumerate(retrievers, start=1):
        try:
            url = f"{retriever.url}/files/{file_name}"
            resp = await _proxy_request(method, url, CONFIG.request_timeout, headers={})
            ROUTER_LATENCY.labels(method=method, retriever_id=retriever.id).observe(
                resp.elapsed.total_seconds()
            )
            if resp.status_code >= 500:
                ROUTER_FAILURES.labels(retriever_id=retriever.id).inc()
                ROUTING_TABLE.mark_failure(retriever.id)
                last_exc = HTTPException(
                    status_code=503, detail=f"Retriever {retriever.id} unavailable"
                )
                continue

            ROUTING_TABLE.mark_success(retriever.id)
            ROUTER_RETRIES.observe(attempt - 1)
            ROUTER_HEALTHY.set(len(ROUTING_TABLE.get_fallback_retrievers()) + 1)

            return Response(
                content=resp.content if method == "GET" else b"",
                status_code=resp.status_code,
                headers=dict(resp.headers),
                media_type=resp.headers.get("content-type"),
            )
        except Exception as exc:  # noqa: BLE001
            ROUTER_FAILURES.labels(retriever_id=retriever.id).inc()
            ROUTING_TABLE.mark_failure(retriever.id)
            last_exc = exc
            await asyncio.sleep(min(2 ** (attempt - 1), 3))
            continue

    raise HTTPException(status_code=503, detail=str(last_exc) if last_exc else "Unavailable")


@app.get("/files/{file_name}")
async def get_file(
    file_name: str,
    hash: Optional[str] = Query(None, description="Pre-computed SHA-256 hash"),
    hash_byte: Optional[str] = Query(None, description="First byte of hash (hex)"),
):
    resp = await _route("GET", file_name, hash, hash_byte)
    ROUTER_REQUESTS.labels(method="GET", status=str(resp.status_code)).inc()
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="File not found")
    return resp


@app.head("/files/{file_name}")
async def check_file_exists(
    file_name: str,
    hash: Optional[str] = Query(None),
    hash_byte: Optional[str] = Query(None),
):
    resp = await _route("HEAD", file_name, hash, hash_byte)
    ROUTER_REQUESTS.labels(method="HEAD", status=str(resp.status_code)).inc()
    return resp


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
async def ready() -> dict[str, bool]:
    snapshot = await readiness(ROUTING_TABLE)
    ROUTER_HEALTHY.set(sum(1 for ok in snapshot.values() if ok))
    return snapshot


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type="text/plain")


@app.get("/routing-table")
async def routing_table() -> dict:
    return {"retrievers": [r.__dict__ for r in ROUTING_TABLE.retrievers]}
