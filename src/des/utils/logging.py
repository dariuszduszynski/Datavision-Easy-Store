from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator, cast

import structlog
from structlog.dev import ConsoleRenderer
from structlog.stdlib import BoundLogger
from structlog.types import Processor

try:
    from des import __version__ as DES_VERSION
except Exception:
    DES_VERSION = os.getenv("APP_VERSION", "unknown")


def _coerce_level(level: str | int) -> int:
    """Translate a string/int level into the numeric logging level."""
    if isinstance(level, str):
        resolved = logging.getLevelName(level.upper())
        if isinstance(resolved, str):
            raise ValueError(f"Invalid log level: {level}")
        return int(resolved)
    return int(level)


def configure_logging(level: str | int = "INFO", json_output: bool = True) -> None:
    """Configure structlog with JSON (or console) rendering and stdlib bridge."""
    numeric_level = _coerce_level(level)
    timestamper = structlog.processors.TimeStamper(fmt="iso")
    renderer = structlog.processors.JSONRenderer() if json_output else ConsoleRenderer()

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        timestamper,
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logging.basicConfig(level=numeric_level, handlers=[handler], force=True)
    logging.captureWarnings(True)


def get_logger(name: str) -> BoundLogger:
    """Return a structlog logger with default service metadata bound."""
    service_name = os.getenv("SERVICE_NAME", "des")
    version = os.getenv("APP_VERSION", DES_VERSION)
    return cast(
        BoundLogger,
        structlog.get_logger(name).bind(service_name=service_name, version=version),
    )


@contextmanager
def log_context(**kwargs: Any) -> Iterator[None]:
    """Bind contextual data for the duration of a block (works across async tasks)."""
    if not kwargs:
        yield
        return

    previous = structlog.contextvars.get_contextvars()
    structlog.contextvars.bind_contextvars(**kwargs)
    try:
        yield
    finally:
        current = structlog.contextvars.get_contextvars()
        for key in kwargs:
            if key in previous:
                current[key] = previous[key]
            else:
                current.pop(key, None)
        structlog.contextvars.clear_contextvars()
        if current:
            structlog.contextvars.bind_contextvars(**current)
