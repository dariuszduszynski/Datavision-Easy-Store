"""Prometheus metrics for DES components."""

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Counters
PACKED_FILES = Counter(
    "des_packer_files_packed_total", "Number of files packed", ["shard_id"]
)
PACKED_BYTES = Counter(
    "des_packer_bytes_packed_total", "Total bytes packed", ["shard_id"]
)
SHARD_LOCK_CONFLICTS = Counter(
    "des_shard_lock_conflicts_total",
    "Shard lock acquisition conflicts",
    ["shard_id"],
)

# Gauges
PACKER_LOOP_DURATION = Gauge(
    "des_packer_loop_duration_seconds",
    "Duration of packer loop iteration per shard",
    ["shard_id"],
)

# Recovery metrics
RECOVERY_STALE_CLAIMS = Counter(
    "des_recovery_stale_claims_total",
    "Number of claimed source files released during crash recovery",
)
RECOVERY_PARTIAL_CONTAINERS = Counter(
    "des_recovery_partial_containers_total",
    "Containers finalized or removed during crash recovery",
    ["action"],
)
RECOVERY_EXPIRED_LOCKS = Counter(
    "des_recovery_expired_locks_total",
    "Shard locks released after expiration during crash recovery",
)
RECOVERY_CONTAINER_INTEGRITY = Counter(
    "des_recovery_container_integrity_total",
    "Container integrity checks that required action",
    ["outcome"],
)
RECOVERY_FILES_UNCLAIMED = Counter(
    "des_recovery_files_unclaimed_total",
    "Number of source files unclaimed during recovery",
    ["source_db", "reason"],
)
RECOVERY_LOCKS_RELEASED = Counter(
    "des_recovery_locks_released_total",
    "Shard locks released during recovery",
    ["shard_id", "reason"],
)
RECOVERY_CONTAINERS_CLEANED = Counter(
    "des_recovery_containers_cleaned_total",
    "Containers cleaned up during recovery",
    ["status", "action"],
)
RECOVERY_DURATION = Histogram(
    "des_recovery_duration_seconds",
    "Duration of recovery operations",
    ["operation"],
)

__all__ = [
    "PACKED_FILES",
    "PACKED_BYTES",
    "SHARD_LOCK_CONFLICTS",
    "PACKER_LOOP_DURATION",
    "RECOVERY_STALE_CLAIMS",
    "RECOVERY_PARTIAL_CONTAINERS",
    "RECOVERY_EXPIRED_LOCKS",
    "RECOVERY_CONTAINER_INTEGRITY",
    "RECOVERY_FILES_UNCLAIMED",
    "RECOVERY_LOCKS_RELEASED",
    "RECOVERY_CONTAINERS_CLEANED",
    "RECOVERY_DURATION",
    "CONTENT_TYPE_LATEST",
    "generate_latest",
]
