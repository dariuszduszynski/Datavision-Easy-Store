"""Shard routing helpers for assigning shards across pods."""

from __future__ import annotations

from typing import List

from des.assignment.hash_routing import consistent_hash


class ShardAssignment:
    """Compute shard ids and pod-to-shard mappings."""

    def __init__(self, n_bits: int, num_pods: int) -> None:
        """
        Initialize shard assignment.

        Args:
            n_bits: Number of bits to use when hashing (total shards = 2 ** n_bits).
            num_pods: Number of worker pods processing shards.
        """
        if n_bits <= 0:
            raise ValueError("n_bits must be positive")
        if num_pods <= 0:
            raise ValueError("num_pods must be positive")

        self.n_bits = n_bits
        self.num_pods = num_pods
        self.total_shards = 1 << n_bits

    def get_shards_for_pod(self, pod_index: int) -> List[int]:
        """
        Return shard ids handled by the given pod.

        Shards are distributed using modulo: shard_id % num_pods == pod_index.
        """
        if pod_index < 0 or pod_index >= self.num_pods:
            raise ValueError(f"pod_index must be in [0, {self.num_pods - 1}]")

        return [
            shard_id
            for shard_id in range(self.total_shards)
            if shard_id % self.num_pods == pod_index
        ]

    def compute_shard_id(self, snowflake_name: str) -> int:
        """Hash a snowflake name to a shard id."""
        return consistent_hash(snowflake_name, self.n_bits)
