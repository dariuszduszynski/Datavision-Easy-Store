"""Prometheus metrics for DES Marker worker."""

from prometheus_client import Counter, Gauge, Histogram, Summary

# Counters
MARKER_ENTRIES_MARKED = Counter(
    "des_marker_entries_marked_total",
    "Total number of catalog entries marked",
    ["status"],
)

MARKER_ERRORS = Counter(
    "des_marker_errors_total",
    "Total number of marking errors",
    ["error_type"],
)

MARKER_RETRIES = Counter(
    "des_marker_retries_total",
    "Total number of retry attempts",
    ["attempt"],
)

MARKER_DLQ_ENTRIES = Counter(
    "des_marker_dlq_entries_total",
    "Entries sent to dead letter queue",
)

# Gauges
MARKER_BATCH_SIZE = Gauge(
    "des_marker_batch_size",
    "Current batch size setting",
)

MARKER_RATE_LIMIT = Gauge(
    "des_marker_rate_limit_ops_per_sec",
    "Current rate limit (ops/sec)",
)

# Histograms
MARKER_BATCH_DURATION = Histogram(
    "des_marker_batch_duration_seconds",
    "Time to process one batch",
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

MARKER_ENTRY_LATENCY = Histogram(
    "des_marker_entry_latency_ms",
    "Time to mark single entry",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
)

MARKER_BATCH_STATS = Summary(
    "des_marker_batch_stats",
    "Batch processing statistics",
    ["metric"],
)

__all__ = [
    "MARKER_ENTRIES_MARKED",
    "MARKER_ERRORS",
    "MARKER_RETRIES",
    "MARKER_DLQ_ENTRIES",
    "MARKER_BATCH_SIZE",
    "MARKER_RATE_LIMIT",
    "MARKER_BATCH_DURATION",
    "MARKER_ENTRY_LATENCY",
    "MARKER_BATCH_STATS",
]
