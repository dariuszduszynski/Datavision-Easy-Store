"""Signal helpers for graceful shutdown."""

from __future__ import annotations

import logging
import signal
from typing import Any, Callable

logger = logging.getLogger(__name__)


def _make_handler(callback: Callable[[], None]) -> Callable[[int, Any], None]:
    def handler(signum: int, _frame: Any) -> None:
        logger.info("received signal, shutting down", extra={"signal": signum})
        callback()

    return handler


def setup_signal_handlers(on_stop: Any) -> None:
    """
    Register SIGINT/SIGTERM handlers.

    The `on_stop` object can provide a `stop` or `shutdown` method; otherwise
    the handler only logs the signal.
    """

    def _stop() -> None:
        for method_name in ("stop", "shutdown", "close"):
            method = getattr(on_stop, method_name, None)
            if callable(method):
                method()
                break

    handler = _make_handler(_stop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)

