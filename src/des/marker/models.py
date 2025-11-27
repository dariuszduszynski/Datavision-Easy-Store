"""Data models for the DES marker worker."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class MarkerStatus(str, Enum):
    """Status przetwarzania przez marker."""

    PENDING = "pending"
    MARKING = "marking"
    MARKED = "marked"
    FAILED = "failed"
    RETRY = "retry"


@dataclass
class MarkerStats:
    """Statystyki batch processing dla monitoringu."""

    batch_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None

    # Counters
    total_processed: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    retried: int = 0

    # Performance
    avg_latency_ms: float = 0.0
    min_latency_ms: float = 0.0
    max_latency_ms: float = 0.0

    # Errors
    error_breakdown: dict[str, int] = field(default_factory=dict)

    def record_success(self, latency_ms: float) -> None:
        """Record successful marking operation."""
        self.successful += 1
        self.total_processed += 1
        self._update_latency(latency_ms)

    def record_failure(self, error_type: str, latency_ms: float) -> None:
        """Record failed marking operation."""
        self.failed += 1
        self.total_processed += 1
        self.error_breakdown[error_type] = (
            self.error_breakdown.get(error_type, 0) + 1
        )
        self._update_latency(latency_ms)

    def _update_latency(self, latency_ms: float) -> None:
        """Update latency statistics."""
        if self.total_processed == 1:
            self.min_latency_ms = latency_ms
            self.max_latency_ms = latency_ms
            self.avg_latency_ms = latency_ms
        else:
            self.min_latency_ms = min(self.min_latency_ms, latency_ms)
            self.max_latency_ms = max(self.max_latency_ms, latency_ms)
            self.avg_latency_ms = (
                (self.avg_latency_ms * (self.total_processed - 1) + latency_ms)
                / self.total_processed
            )


@dataclass
class MarkerConfig:
    """Konfiguracja advanced marker worker."""

    batch_size: int = 100
    max_age_days: int = 1
    max_retries: int = 3
    retry_backoff_base: float = 2.0
    rate_limit_per_second: Optional[float] = None
    enable_dead_letter_queue: bool = True
    dlq_table: str = "des_marker_dlq"
    health_check_interval: int = 30
    metrics_port: int = 9101


__all__ = ["MarkerStatus", "MarkerStats", "MarkerConfig"]
