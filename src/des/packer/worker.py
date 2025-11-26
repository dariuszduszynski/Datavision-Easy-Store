"""Worker thread logic - processing batches in threads."""

from __future__ import annotations

from typing import Any, Iterable


class PackerWorker:
    """Worker that processes file batches in a separate thread."""

    def __init__(self, packer: Any, worker_id: int) -> None:
        ...

    def process_files(self, files: Iterable[Any]) -> None:
        ...
