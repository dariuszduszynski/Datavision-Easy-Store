from __future__ import annotations

from des.router.routing_table import RoutingStrategy, RoutingTable


def test_routing_hash_byte_primary():
    table = RoutingTable(["a", "b"], strategy=RoutingStrategy.HASH_BYTE)
    endpoint = table.get_target_retriever(file_name="foo.txt", hash_value=None, hash_byte=1)
    assert endpoint.id == "1"  # 1 % 2 -> second retriever


def test_routing_fallback_on_unhealthy():
    table = RoutingTable(["a", "b"], strategy=RoutingStrategy.HASH_BYTE, circuit_breaker_threshold=1)
    primary = table.get_target_retriever(file_name="foo.txt", hash_value=None, hash_byte=0)
    table.mark_failure(primary.id)
    fallback = table.get_target_retriever(file_name="foo.txt", hash_value=None, hash_byte=0)
    assert fallback.id != primary.id
