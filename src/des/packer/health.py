"""Health checks for DES packer services and Kubernetes probes."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import func, select, text

from des.db.connector import DesDbConnector, ShardLock
from des.packer.source_provider import MultiSourceFileProvider


def _isoformat(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


class HealthChecker:
    """Runs async health checks across DB, S3, shard locks, and source DBs."""

    def __init__(
        self,
        db: DesDbConnector,
        *,
        s3_client: Any = None,
        s3_bucket: Optional[str] = None,
        source_provider: Optional[MultiSourceFileProvider] = None,
        timeout_seconds: float = 5.0,
    ):
        self.db = db
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.source_provider = source_provider
        self.timeout = min(timeout_seconds, 5.0) if timeout_seconds else 5.0
        self.start_time = datetime.now(timezone.utc)
        self._last_processed: Optional[datetime] = None

    def record_processed(self, when: Optional[datetime] = None) -> None:
        """Update the timestamp of the last processed file/container."""
        self._last_processed = when or datetime.now(timezone.utc)

    async def check_database(self) -> Dict[str, Any]:
        """Test DB connection with SELECT 1."""
        start = time.perf_counter()
        status = "ok"
        error: Optional[str] = None

        async def _probe() -> None:
            async with self.db.session_factory() as session:
                await session.execute(text("SELECT 1"))

        try:
            await asyncio.wait_for(_probe(), timeout=self.timeout)
        except asyncio.TimeoutError:
            status = "timeout"
        except Exception as exc:  # noqa: BLE001 - surface error in payload
            status = "error"
            error = str(exc)

        latency_ms = int((time.perf_counter() - start) * 1000)
        result: Dict[str, Any] = {"status": status, "latency_ms": latency_ms}
        if error:
            result["error"] = error
        return result

    async def check_s3(self) -> Dict[str, Any]:
        """Test S3 connectivity with head_bucket."""
        start = time.perf_counter()
        status = "ok"
        error: Optional[str] = None

        async def _probe() -> None:
            if not self.s3_client or not self.s3_bucket:
                raise RuntimeError("S3 client or bucket not configured")
            await asyncio.to_thread(self.s3_client.head_bucket, Bucket=self.s3_bucket)

        try:
            await asyncio.wait_for(_probe(), timeout=self.timeout)
        except asyncio.TimeoutError:
            status = "timeout"
        except Exception as exc:  # noqa: BLE001
            status = "error"
            error = str(exc)

        latency_ms = int((time.perf_counter() - start) * 1000)
        result: Dict[str, Any] = {"status": status, "latency_ms": latency_ms}
        if error:
            result["error"] = error
        return result

    async def check_shard_locks(self) -> Dict[str, Any]:
        """Return shard lock status (held vs expired)."""
        start = time.perf_counter()
        held = 0
        expired = 0
        status = "ok"
        error: Optional[str] = None

        async def _probe() -> Tuple[int, int]:
            async with self.db.session_factory() as session:
                now = datetime.now(timezone.utc)
                held_stmt = (
                    select(func.count())
                    .select_from(ShardLock)
                    .where(ShardLock.expires_at > now)
                )
                expired_stmt = (
                    select(func.count())
                    .select_from(ShardLock)
                    .where(ShardLock.expires_at <= now)
                )
                held_count = (await session.execute(held_stmt)).scalar_one()
                expired_count = (await session.execute(expired_stmt)).scalar_one()
                return int(held_count or 0), int(expired_count or 0)

        try:
            held, expired = await asyncio.wait_for(_probe(), timeout=self.timeout)
        except asyncio.TimeoutError:
            status = "timeout"
        except Exception as exc:  # noqa: BLE001
            status = "error"
            error = str(exc)

        latency_ms = int((time.perf_counter() - start) * 1000)
        result: Dict[str, Any] = {
            "status": status,
            "held": held,
            "expired": expired,
            "latency_ms": latency_ms,
        }
        if error:
            result["error"] = error
        return result

    async def _check_single_source(
        self, name: str, connector: Any
    ) -> Tuple[str, bool, Optional[str]]:
        """Ping a single source DB connector in a thread."""

        def _ping() -> Tuple[bool, Optional[str]]:
            engine = getattr(connector, "engine", None)
            if not engine:
                return False, "not_connected"
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, None

        try:
            ok, err = await asyncio.wait_for(
                asyncio.to_thread(_ping), timeout=self.timeout
            )
            return name, ok, err
        except asyncio.TimeoutError:
            return name, False, "timeout"
        except Exception as exc:  # noqa: BLE001
            return name, False, str(exc)

    async def check_source_providers(self) -> Dict[str, Any]:
        """Return status of source DB connections."""
        start = time.perf_counter()
        connectors = getattr(self.source_provider, "connectors", {}) or {}
        enabled = len(connectors)
        connected = 0
        failures: Dict[str, str] = {}

        if not connectors:
            latency_ms = int((time.perf_counter() - start) * 1000)
            return {
                "status": "ok",
                "enabled": 0,
                "connected": 0,
                "latency_ms": latency_ms,
            }

        tasks = [
            self._check_single_source(name, connector)
            for name, connector in connectors.items()
        ]
        results: list[
            Tuple[str, bool, Optional[str]] | BaseException
        ] = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, BaseException):
                failures["unknown"] = str(result)
                continue
            name, ok, err = result
            if ok:
                connected += 1
            else:
                failures[name] = err or "unavailable"

        latency_ms = int((time.perf_counter() - start) * 1000)
        status = "ok" if connected == enabled else "degraded"
        payload: Dict[str, Any] = {
            "status": status,
            "enabled": enabled,
            "connected": connected,
            "latency_ms": latency_ms,
        }
        if failures:
            payload["failures"] = failures
        return payload

    async def get_health_status(self) -> Dict[str, Any]:
        """Aggregate health report suitable for Kubernetes probes."""
        db_result, s3_result, locks_result, sources_result = await asyncio.gather(
            self.check_database(),
            self.check_s3(),
            self.check_shard_locks(),
            self.check_source_providers(),
        )

        critical_failed = (
            db_result.get("status") != "ok" or s3_result.get("status") != "ok"
        )
        non_critical_issue = (
            locks_result.get("status") != "ok"
            or locks_result.get("expired", 0) > 0
            or sources_result.get("status") != "ok"
            or sources_result.get("connected", 0) < sources_result.get("enabled", 0)
        )

        if critical_failed:
            overall_status = "unhealthy"
        elif non_critical_issue:
            overall_status = "degraded"
        else:
            overall_status = "healthy"

        now = datetime.now(timezone.utc)
        report = {
            "status": overall_status,
            "timestamp": _isoformat(now),
            "checks": {
                "database": db_result,
                "s3": s3_result,
                "shard_locks": locks_result,
                "source_providers": sources_result,
            },
            "uptime_seconds": int((now - self.start_time).total_seconds()),
            "last_processed": _isoformat(self._last_processed),
        }
        return report


__all__ = ["HealthChecker"]
