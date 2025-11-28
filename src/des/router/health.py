from __future__ import annotations

from typing import Dict

from des.router.routing_table import RoutingTable


async def readiness(table: RoutingTable) -> Dict[str, bool]:
    """Return health snapshot for retrievers."""
    return await table.health_check_all()


__all__ = ["readiness"]
