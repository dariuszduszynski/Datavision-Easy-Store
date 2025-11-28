from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional


class RoutingStrategy(str, Enum):
    HASH_BYTE = "hash_byte"
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"


@dataclass
class RetrieverEndpoint:
    id: str
    url: str
    is_healthy: bool = True
    failure_count: int = 0
    last_failure_time: Optional[float] = None


class RoutingTable:
    """
    Manages routing from hash values to retriever endpoints with circuit breaker.
    """

    def __init__(
        self,
        retrievers: List[str],
        strategy: RoutingStrategy = RoutingStrategy.HASH_BYTE,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: int = 30,
    ):
        self.strategy = strategy
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.circuit_breaker_timeout = circuit_breaker_timeout
        self._counter = 0
        self.retrievers: List[RetrieverEndpoint] = [
            RetrieverEndpoint(id=str(idx), url=url) for idx, url in enumerate(retrievers)
        ]

    def _now(self) -> float:
        return time.monotonic()

    def _hash_first_byte(self, file_name: Optional[str], hash_value: Optional[str]) -> int:
        if hash_value:
            digest = hash_value
        elif file_name:
            digest = hashlib.sha256(file_name.encode("utf-8")).hexdigest()
        else:
            raise ValueError("file_name or hash_value must be provided")
        return int(digest[:2], 16)

    def get_target_retriever(
        self,
        file_name: Optional[str] = None,
        hash_value: Optional[str] = None,
        hash_byte: Optional[int] = None,
    ) -> RetrieverEndpoint:
        healthy = [r for r in self.retrievers if self._is_healthy(r)]
        if not healthy:
            # allow all if circuit breaker timed out
            self._reset_unhealthy()
            healthy = [r for r in self.retrievers if self._is_healthy(r)]
        if not healthy:
            raise RuntimeError("No healthy retrievers available")

        if self.strategy == RoutingStrategy.ROUND_ROBIN:
            endpoint = healthy[self._counter % len(healthy)]
            self._counter += 1
            return endpoint

        target_byte = hash_byte if hash_byte is not None else self._hash_first_byte(
            file_name, hash_value
        )
        idx = target_byte % len(self.retrievers)
        primary = self.retrievers[idx]
        if self._is_healthy(primary):
            return primary

        # fallback to first healthy
        for r in healthy:
            if r.id != primary.id:
                return r
        return primary

    def get_fallback_retrievers(self, exclude: Optional[str] = None) -> List[RetrieverEndpoint]:
        healthy = [r for r in self.retrievers if self._is_healthy(r)]
        if exclude:
            healthy = [r for r in healthy if r.id != exclude]
        return healthy

    def mark_failure(self, retriever_id: str) -> None:
        endpoint = self._get_by_id(retriever_id)
        if not endpoint:
            return
        endpoint.failure_count += 1
        endpoint.last_failure_time = self._now()
        if endpoint.failure_count >= self.circuit_breaker_threshold:
            endpoint.is_healthy = False

    def mark_success(self, retriever_id: str) -> None:
        endpoint = self._get_by_id(retriever_id)
        if not endpoint:
            return
        endpoint.failure_count = 0
        endpoint.is_healthy = True
        endpoint.last_failure_time = None

    def _get_by_id(self, retriever_id: str) -> Optional[RetrieverEndpoint]:
        for r in self.retrievers:
            if r.id == retriever_id:
                return r
        return None

    def _is_healthy(self, endpoint: RetrieverEndpoint) -> bool:
        if endpoint.is_healthy:
            return True
        if endpoint.last_failure_time is None:
            return False
        # allow retry after timeout
        if self._now() - endpoint.last_failure_time > self.circuit_breaker_timeout:
            endpoint.is_healthy = True
            endpoint.failure_count = 0
            return True
        return False

    def _reset_unhealthy(self) -> None:
        now = self._now()
        for r in self.retrievers:
            if not r.is_healthy and r.last_failure_time:
                if now - r.last_failure_time > self.circuit_breaker_timeout:
                    r.is_healthy = True
                    r.failure_count = 0

    async def health_check_all(self) -> Dict[str, bool]:
        # Simple synchronous health snapshot for now
        return {r.id: self._is_healthy(r) for r in self.retrievers}


__all__ = ["RoutingStrategy", "RetrieverEndpoint", "RoutingTable"]
