"""Tests for consistent hashing utilities."""

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.assignment.hash_routing import consistent_hash  # noqa: E402


@pytest.mark.unit
def test_consistent_hash_basic():
    """Test basic hashing functionality."""
    result = consistent_hash("test_value", n_bits=8)
    assert 0 <= result < 256
    assert isinstance(result, int)


@pytest.mark.unit
def test_consistent_hash_deterministic():
    """Test that same input produces same hash."""
    value = "snowflake_id_12345"
    result1 = consistent_hash(value, n_bits=8)
    result2 = consistent_hash(value, n_bits=8)
    assert result1 == result2


@pytest.mark.unit
def test_consistent_hash_different_values():
    """Test that different inputs produce different hashes (usually)."""
    # High probability they're different (not guaranteed but very likely)
    hashes = [consistent_hash(f"value_{i}", n_bits=8) for i in range(10)]
    assert len(set(hashes)) > 5  # At least 5 unique values out of 10


@pytest.mark.unit
def test_consistent_hash_n_bits_range():
    """Test different n_bits values."""
    value = "test"

    # 1 bit: 0 or 1
    result = consistent_hash(value, n_bits=1)
    assert 0 <= result < 2

    # 4 bits: 0-15
    result = consistent_hash(value, n_bits=4)
    assert 0 <= result < 16

    # 16 bits: 0-65535
    result = consistent_hash(value, n_bits=16)
    assert 0 <= result < 65536

    # 32 bits: 0-4294967295
    result = consistent_hash(value, n_bits=32)
    assert 0 <= result < 2**32


@pytest.mark.unit
def test_consistent_hash_invalid_n_bits():
    """Test validation of n_bits parameter."""
    with pytest.raises(ValueError, match="n_bits must be in"):
        consistent_hash("test", n_bits=0)

    with pytest.raises(ValueError, match="n_bits must be in"):
        consistent_hash("test", n_bits=-1)

    with pytest.raises(ValueError, match="n_bits must be in"):
        consistent_hash("test", n_bits=257)


@pytest.mark.unit
def test_consistent_hash_unicode():
    """Test hashing with Unicode strings."""
    result1 = consistent_hash("Zażółć gęślą jaźń", n_bits=8)
    result2 = consistent_hash("日本語テキスト", n_bits=8)
    result3 = consistent_hash("🎉🎊🎈", n_bits=8)

    assert 0 <= result1 < 256
    assert 0 <= result2 < 256
    assert 0 <= result3 < 256


@pytest.mark.unit
def test_consistent_hash_empty_string():
    """Test hashing empty string."""
    result = consistent_hash("", n_bits=8)
    assert 0 <= result < 256


@pytest.mark.unit
def test_consistent_hash_distribution():
    """Test that hashing distributes values across range."""
    n_bits = 4  # 16 buckets
    num_values = 160  # 10x more values than buckets

    hashes = [consistent_hash(f"value_{i}", n_bits=n_bits) for i in range(num_values)]

    # Check we used multiple buckets (not all in one)
    unique_hashes = len(set(hashes))
    assert unique_hashes >= 12  # At least 75% of buckets used
