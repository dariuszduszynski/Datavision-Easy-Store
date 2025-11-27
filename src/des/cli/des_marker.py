from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

from des.db.connector import DesDbConnector
from des.marker.advanced_marker import AdvancedFileMarker
from des.marker.models import MarkerConfig
from des.monitoring.metrics import CONTENT_TYPE_LATEST, generate_latest
from des.utils.logging import configure_logging, get_logger
from des.utils.signals import setup_signal_handlers
from des.utils.snowflake_name import SnowflakeNameConfig


def _load_snowflake_config() -> SnowflakeNameConfig:
    prefix = os.getenv("DES_NAME_PREFIX")
    node_id = int(os.getenv("DES_NODE_ID", "0"))
    base = SnowflakeNameConfig()
    return SnowflakeNameConfig(
        node_id=node_id,
        prefix=prefix or base.prefix,
        wrap_bits=base.wrap_bits,
    )


def _env_float(name: str) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _load_marker_config() -> MarkerConfig:
    return MarkerConfig(
        batch_size=int(os.getenv("DES_MARKER_BATCH_SIZE", "100")),
        max_age_days=int(os.getenv("DES_MARKER_MAX_AGE_DAYS", "1")),
        max_retries=int(os.getenv("DES_MARKER_MAX_RETRIES", "3")),
        retry_backoff_base=float(os.getenv("DES_MARKER_RETRY_BACKOFF_BASE", "2.0")),
        rate_limit_per_second=_env_float("DES_MARKER_RATE_LIMIT"),
        enable_dead_letter_queue=os.getenv("DES_MARKER_ENABLE_DLQ", "true").lower()
        == "true",
        dlq_table=os.getenv("DES_MARKER_DLQ_TABLE", "des_marker_dlq"),
        health_check_interval=int(os.getenv("DES_HEALTH_CHECK_INTERVAL", "30")),
        metrics_port=int(os.getenv("DES_METRICS_PORT", "9101")),
    )


def _build_handler(marker: AdvancedFileMarker, start_time: float):
    """Create HTTP handler exposing health and Prometheus metrics."""

    class MarkerHandler(BaseHTTPRequestHandler):
        def _write_json(self, payload: dict, status: int = 200) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            if self.path in ("/health", "/health/live"):
                uptime = int(time.time() - start_time)
                self._write_json({"status": "ok", "uptime_seconds": uptime})
                return

            if self.path == "/health/ready":
                status = "ok" if not marker._shutdown.is_set() else "stopping"
                code = 200 if status == "ok" else 503
                self._write_json({"status": status}, status=code)
                return

            if self.path == "/metrics":
                output = generate_latest()
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.send_header("Content-Length", str(len(output)))
                self.end_headers()
                self.wfile.write(output)
                return

            self.send_response(404)
            self.end_headers()

        def log_message(self, _format: str, *_args):  # noqa: D401, ANN001
            """Silence default HTTP request logging."""
            return

    return MarkerHandler


def _start_http_server(marker: AdvancedFileMarker, port: int) -> ThreadingHTTPServer:
    start_time = time.time()
    handler = _build_handler(marker, start_time)
    server = ThreadingHTTPServer(("", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def main() -> None:
    """Entry point for the DES marker worker."""
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"), json_output=False)
    logger = get_logger(__name__)

    config = _load_marker_config()
    interval_seconds = int(os.getenv("DES_MARKER_INTERVAL_SECONDS", "5"))

    connector = DesDbConnector()
    worker = AdvancedFileMarker(
        connector.session_factory,
        config=config,
        snowflake_config=_load_snowflake_config(),
    )

    http_server = _start_http_server(worker, config.metrics_port)
    setup_signal_handlers(worker)

    async def _run() -> None:
        await connector.init_models()
        await worker.run_forever(interval_seconds=interval_seconds)

    logger.info(
        "starting_marker",
        config=config.__dict__,
        interval_seconds=interval_seconds,
        db_url=connector.db_url,
        metrics_port=config.metrics_port,
    )

    try:
        asyncio.run(_run())
    finally:
        http_server.shutdown()


if __name__ == "__main__":
    main()
