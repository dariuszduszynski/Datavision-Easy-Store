"""FastAPI microservice for assigning DES snowflake names and shard IDs."""

import json
import logging
import os
from datetime import date, datetime, timezone
from typing import Any, Optional

import boto3
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from des.assignment.hash_routing import consistent_hash
from des.db.connector import DesDbConnector
from des.monitoring import metrics as des_metrics
from des.packer.health import HealthChecker
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


def log_event(event: str, **fields: Any) -> None:
    """Emit structured JSON logs."""
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, ensure_ascii=False))


class AssignRequest(BaseModel):
    prefix: str
    meta_hint: Optional[dict[str, Any]] = None  # reserved for future routing logic


class AssignResponse(BaseModel):
    name: str
    shard_id: int
    day: str


app = FastAPI(title="DES Name Assignment Service")

START_TIME = datetime.now(timezone.utc)


def _build_health_checker() -> Optional[HealthChecker]:
    """Initialize HealthChecker with optional S3 support."""
    try:
        db_connector = DesDbConnector(db_url=os.getenv("DES_DB_URL"))
    except Exception as exc:  # noqa: BLE001
        log_event("health_checker_init_failed", error=str(exc))
        return None

    s3_bucket = os.getenv("DES_ARCHIVE_BUCKET")
    s3_client = None
    if s3_bucket:
        try:
            s3_client = boto3.client("s3")
        except Exception as exc:  # noqa: BLE001
            log_event("s3_client_init_failed", error=str(exc))

    return HealthChecker(db_connector, s3_client=s3_client, s3_bucket=s3_bucket)


HEALTH_CHECKER = _build_health_checker()


@app.get("/health")
def health() -> dict[str, str]:
    """Health probe endpoint."""
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(
        des_metrics.generate_latest(), media_type=des_metrics.CONTENT_TYPE_LATEST
    )


@app.get("/health/live")
def health_live() -> dict[str, int | str]:
    """Liveness probe with uptime reporting."""
    uptime = int((datetime.now(timezone.utc) - START_TIME).total_seconds())
    return {"status": "ok", "uptime": uptime}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    """Readiness probe using detailed health checks."""
    if not HEALTH_CHECKER:
        payload = {"status": "unhealthy", "error": "health_checker_unavailable"}
        return JSONResponse(status_code=503, content=payload)

    report = await HEALTH_CHECKER.get_health_status()
    status_code = 200 if report.get("status") == "healthy" else 503
    return JSONResponse(status_code=status_code, content=report)


@app.post("/assign", response_model=AssignResponse)
def assign(request: AssignRequest) -> AssignResponse:
    """Assign a snowflake name and shard id."""
    day = date.today()
    try:
        generator = SnowflakeNameGenerator(
            SnowflakeNameConfig(
                node_id=NODE_ID, prefix=request.prefix, wrap_bits=WRAP_BITS
            )
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
