"""Hash-based shard routing utilities."""

import hashlib


def consistent_hash(value: str, n_bits: int) -> int:
    """
    Deterministic hash -> shard id using first n_bits of SHA-256 digest.

    Args:
        value: Input string (e.g., snowflake name)
        n_bits: Number of bits to keep (1-256)

    Returns:
        Integer in range [0, 2**n_bits - 1]
    """
    if n_bits <= 0 or n_bits > 256:
        raise ValueError("n_bits must be in [1, 256]")

    digest = hashlib.sha256(value.encode("utf-8")).digest()
    hash_int = int.from_bytes(digest, "big")
    # Take the most significant n_bits of the digest
    return (hash_int >> (256 - n_bits)) & ((1 << n_bits) - 1)


__all__ = ["consistent_hash"]
