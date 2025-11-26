"""Prometheus metrics for DES components."""
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

# Counters
PACKED_FILES = Counter("des_packer_files_packed_total", "Number of files packed", ["shard_id"])
PACKED_BYTES = Counter("des_packer_bytes_packed_total", "Total bytes packed", ["shard_id"])
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

__all__ = [
    "PACKED_FILES",
    "PACKED_BYTES",
    "SHARD_LOCK_CONFLICTS",
    "PACKER_LOOP_DURATION",
    "CONTENT_TYPE_LATEST",
    "generate_latest",
]
