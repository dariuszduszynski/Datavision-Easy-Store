from __future__ import annotations

from des.core.cache import InMemoryIndexCache, RedisIndexCache
from des.config.retriever_config import RetrieverConfig


def build_cache(cfg: RetrieverConfig):
    if cfg.cache_backend == "redis":
        if not cfg.redis_url:
            raise RuntimeError("DES_REDIS_URL must be set when cache_backend=redis")
        return RedisIndexCache(cfg.redis_url, ttl_seconds=cfg.cache_ttl)
    return InMemoryIndexCache(ttl_seconds=cfg.cache_ttl)


__all__ = ["build_cache"]
