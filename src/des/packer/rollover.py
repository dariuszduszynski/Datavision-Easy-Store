"""Daily rollover logic - finalizing shards at midnight."""

from __future__ import annotations

from typing import Any


class DailyRolloverManager:
    """Manage daily shard finalization and upload."""

    def __init__(self, packer: Any) -> None:
        ...

    def check_and_rollover(self) -> None:
        """Check whether a new day started and trigger rollover."""
        ...

    def finalize_shard(self, shard_id: int, writer: Any) -> None:
        """Finalize and upload a specific shard."""
        ...
