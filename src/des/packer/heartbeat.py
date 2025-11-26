"""Heartbeat management - maintaining heartbeats for claimed files."""

from __future__ import annotations

from typing import Any


class HeartbeatManager:
    """Manage heartbeat for claimed files."""

    def __init__(self, db_connector: Any, pod_name: str) -> None: ...

    def start(self) -> None:
        """Start heartbeat thread."""
        ...

    def stop(self) -> None:
        """Stop heartbeat gracefully."""
        ...
