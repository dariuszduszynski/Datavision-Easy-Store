"""Async retry utilities with exponential backoff and jitter."""

import asyncio
import functools
import random
from typing import Awaitable, Callable, Iterable, Tuple, Type, TypeVar, ParamSpec

import structlog

logger = structlog.get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def async_retry(
    *,
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    exceptions: Iterable[Type[BaseException]] = (Exception,),
    jitter: bool = True,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """
    Decorator to retry async functions with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts before giving up.
        backoff_base: Base for exponential backoff (wait = backoff_base ** attempt).
        exceptions: Exception types that trigger a retry.
        jitter: Whether to apply random jitter to backoff waits.
    """

    exc_tuple: Tuple[Type[BaseException], ...] = tuple(exceptions)

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            attempt = 1
            while True:
                try:
                    return await func(*args, **kwargs)
                except exc_tuple as exc:
                    if attempt >= max_attempts:
                        raise exc
                    wait = backoff_base**attempt
                    if jitter:
                        wait *= random.uniform(0.5, 1.5)

                    logger.warning(
                        "retrying_operation",
                        function=func.__name__,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        wait_seconds=wait,
                        error=str(exc),
                    )

                    await asyncio.sleep(wait)
                    attempt += 1

        return wrapper

    return decorator


__all__ = ["async_retry"]
