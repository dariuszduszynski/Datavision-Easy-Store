from __future__ import annotations

import os
from typing import List, Optional

from pydantic import BaseModel, Field


class RouterConfig(BaseModel):
    """Router service configuration."""

    retriever_endpoints: List[str] = Field(
        ...,
        description="List of retriever URLs",
        example=["http://retriever-0:8001", "http://retriever-1:8001"],
    )
    routing_strategy: str = Field(
        "hash_byte",
        description="Routing strategy: hash_byte, round_robin, weighted",
    )
    request_timeout: int = Field(
        30,
        description="Timeout for requests to retrievers (seconds)",
    )
    circuit_breaker_threshold: int = Field(
        5,
        description="Failures before marking retriever unhealthy",
    )
    circuit_breaker_timeout: int = Field(
        30,
        description="Seconds before retrying unhealthy retriever",
    )
    max_retries: int = Field(
        3,
        description="Maximum retry attempts per request",
    )

    @classmethod
    def from_env(cls) -> "RouterConfig":
        endpoints_raw = os.getenv("DES_RETRIEVER_ENDPOINTS", "")
        endpoints = [ep.strip() for ep in endpoints_raw.split(",") if ep.strip()]
        if not endpoints:
            raise RuntimeError("DES_RETRIEVER_ENDPOINTS must be set")

        return cls(
            retriever_endpoints=endpoints,
            routing_strategy=os.getenv("DES_ROUTING_STRATEGY", "hash_byte"),
            request_timeout=int(os.getenv("DES_REQUEST_TIMEOUT", "30")),
            circuit_breaker_threshold=int(
                os.getenv("DES_CIRCUIT_BREAKER_THRESHOLD", "5")
            ),
            circuit_breaker_timeout=int(
                os.getenv("DES_CIRCUIT_BREAKER_TIMEOUT", "30")
            ),
            max_retries=int(os.getenv("DES_ROUTER_MAX_RETRIES", "3")),
        )


class RetrieverConfig(BaseModel):
    """Retriever service configuration."""

    s3_bucket: str = Field(..., description="S3 bucket for DES archives")
    s3_prefix: Optional[str] = Field(None, description="S3 prefix for archives")
    cache_backend: str = Field(
        "memory",
        description="Cache backend: memory or redis",
    )
    redis_url: Optional[str] = Field(
        None,
        description="Redis URL if cache_backend=redis",
    )
    cache_ttl: int = Field(
        3600,
        description="Cache TTL in seconds",
    )
    shard_bits: int = Field(
        8,
        description="Number of shard bits (2^n shards)",
    )

    @classmethod
    def from_env(cls) -> "RetrieverConfig":
        bucket = os.getenv("DES_S3_BUCKET")
        if not bucket:
            raise RuntimeError("DES_S3_BUCKET must be set")

        prefix = os.getenv("DES_S3_PREFIX")
        cache_backend = os.getenv("DES_CACHE_BACKEND", "memory")
        redis_url = os.getenv("DES_REDIS_URL")

        return cls(
            s3_bucket=bucket,
            s3_prefix=prefix if prefix else None,
            cache_backend=cache_backend,
            redis_url=redis_url if cache_backend == "redis" else None,
            cache_ttl=int(os.getenv("DES_CACHE_TTL", "3600")),
            shard_bits=int(os.getenv("DES_SHARD_BITS", "8")),
        )


__all__ = ["RouterConfig", "RetrieverConfig"]
