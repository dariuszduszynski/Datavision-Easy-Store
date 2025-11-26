"""FastAPI microservice for assigning DES snowflake names and shard IDs."""
import json
import logging
import os
from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

from des.assignment.hash_routing import consistent_hash
from des.monitoring import metrics as des_metrics
from des.utils.snowflake_name import SnowflakeNameConfig, SnowflakeNameGenerator


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        log_event("invalid_env_value", env=name, value=raw, default=default)
        return default


NODE_ID = _env_int("DES_NODE_ID", 0)
WRAP_BITS = _env_int("DES_WRAP_BITS", 32)
SHARD_BITS = _env_int("DES_SHARD_BITS", 8)


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("des.assignment.service")


def log_event(event: str, **fields) -> None:
    """Emit structured JSON logs."""
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, ensure_ascii=False))


class AssignRequest(BaseModel):
    prefix: str
    meta_hint: Optional[dict] = None  # reserved for future routing logic


class AssignResponse(BaseModel):
    name: str
    shard_id: int
    day: str


app = FastAPI(title="DES Name Assignment Service")


@app.get("/health")
def health() -> dict:
    """Health probe endpoint."""
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(des_metrics.generate_latest(), media_type=des_metrics.CONTENT_TYPE_LATEST)


@app.post("/assign", response_model=AssignResponse)
def assign(request: AssignRequest) -> AssignResponse:
    """Assign a snowflake name and shard id."""
    day = date.today()
    try:
        generator = SnowflakeNameGenerator(
            SnowflakeNameConfig(node_id=NODE_ID, prefix=request.prefix, wrap_bits=WRAP_BITS)
        )
    except ValueError as exc:
        log_event("invalid_request", error=str(exc))
        raise HTTPException(status_code=400, detail=str(exc))

    name = generator.next_name(day=day)
    try:
        shard_id = consistent_hash(name, SHARD_BITS)
    except ValueError as exc:
        log_event("invalid_shard_bits", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))

    response = AssignResponse(name=name, shard_id=shard_id, day=day.isoformat())
    log_event("assigned", name=name, shard_id=shard_id, day=response.day)
    return response
