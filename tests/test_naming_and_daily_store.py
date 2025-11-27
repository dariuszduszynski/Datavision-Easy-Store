import sys
from datetime import date, datetime
from pathlib import Path

import pytest

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core import DesReader  # noqa: E402
from des.packer.daily_sharded_store import DailyShardedDesStore  # noqa: E402
from des.utils.snowflake_name import (  # noqa: E402
    SnowflakeNameConfig,
    SnowflakeNameGenerator,
)


def test_snowflake_basic_format(monkeypatch) -> None:
    fixed_ts_ms = int(datetime(2025, 1, 1, 12, 0, 0).timestamp() * 1000)

    def fake_epoch_ms():
        return fixed_ts_ms

    gen = SnowflakeNameGenerator(
        SnowflakeNameConfig(node_id=1, prefix="TEST", wrap_bits=16)
    )
    monkeypatch.setattr(gen, "_epoch_ms", fake_epoch_ms)

    names = [gen.next_name(day=date(2025, 1, 1)) for _ in range(3)]

    assert len(names) == len(set(names))
    for name in names:
        assert name.startswith("TEST_20250101_(")
        assert name.endswith(")")

    with pytest.raises(ValueError):
        SnowflakeNameGenerator(SnowflakeNameConfig(prefix="bad-prefix!"))


def test_snowflake_monotonicity(monkeypatch) -> None:
    base_ts = int(datetime(2025, 1, 1, 12, 0, 0).timestamp() * 1000)
    calls = {"count": 0}

    def fake_epoch_ms():
        calls["count"] += 1
        return base_ts

    gen = SnowflakeNameGenerator(
        SnowflakeNameConfig(node_id=2, prefix="NODE", wrap_bits=16)
    )
    monkeypatch.setattr(gen, "_epoch_ms", fake_epoch_ms)

    names = [gen.next_name(day=date(2025, 1, 1)) for _ in range(5)]
    assert names == sorted(names)
    assert len(set(names)) == len(names)


def test_daily_store_simple_pack(tmp_path: Path) -> None:
    store = DailyShardedDesStore(
        base_dir=tmp_path, shard_bits=4, node_id=1, prefix="TEST"
    )

    # Store original data with metadata containing original filenames
    inputs = {
        "file1.txt": b"hello",
        "file2.bin": b"\x00\x01",
        "file three.txt": b"world",
    }

    # Track generated names -> original data
    generated_names = {}
    for orig_name, data in inputs.items():
        logical_name, _ = store.add_file(
            data, meta={"original_name": orig_name}, ext=None
        )
        generated_names[logical_name] = (data, orig_name)

    store.close()

    today = date.today().isoformat()
    day_dir = tmp_path / today
    assert day_dir.exists()

    des_files = list(day_dir.glob("*.des"))
    assert des_files

    # Verify all files are present with correct data
    seen = {}
    for des_file in des_files:
        with DesReader(str(des_file)) as reader:
            for fname in reader.list_files():
                data = reader.get_file(fname)
                meta = reader.get_meta(fname)
                seen[fname] = (data, meta.get("original_name"))

    # Check we found all generated files
    assert set(seen.keys()) == set(
        generated_names.keys()
    ), f"Expected files: {set(generated_names.keys())}, found: {set(seen.keys())}"

    # Verify data matches
    for gen_name, (expected_data, orig_name) in generated_names.items():
        found_data, found_orig_name = seen[gen_name]
        assert (
            found_data == expected_data
        ), f"Data mismatch for {gen_name} (original: {orig_name})"
        assert found_orig_name == orig_name, f"Metadata mismatch for {gen_name}"


def test_daily_store_rollover_between_days(tmp_path: Path, monkeypatch) -> None:
    day1 = date(2025, 1, 1)
    day2 = date(2025, 1, 2)

    # Patch date.today used inside DailyShardedDesStore via constructor argument
    store1 = DailyShardedDesStore(
        base_dir=tmp_path, shard_bits=3, day=day1, node_id=1, prefix="D1"
    )
    store1.add_file(b"a", meta={"day": "1"})
    store1.close()

    store2 = DailyShardedDesStore(
        base_dir=tmp_path, shard_bits=3, day=day2, node_id=2, prefix="D2"
    )
    store2.add_file(b"b", meta={"day": "2"})
    store2.close()

    day1_dir = tmp_path / day1.isoformat()
    day2_dir = tmp_path / day2.isoformat()
    assert day1_dir.exists()
    assert day2_dir.exists()

    assert list(day1_dir.glob("*.des"))
    assert list(day2_dir.glob("*.des"))

    # Verify contents are segregated
    files_day1 = set()
    for f in day1_dir.glob("*.des"):
        with DesReader(str(f)) as reader:
            files_day1.update(reader.list_files())

    files_day2 = set()
    for f in day2_dir.glob("*.des"):
        with DesReader(str(f)) as reader:
            files_day2.update(reader.list_files())

    assert files_day1 and files_day2
    assert files_day1.isdisjoint(files_day2)


@pytest.mark.unit
def test_snowflake_name_empty_prefix():
    """Test that empty prefix raises error."""
    with pytest.raises(ValueError, match="prefix must be non-empty"):
        SnowflakeNameGenerator(SnowflakeNameConfig(prefix=""))


@pytest.mark.unit
def test_snowflake_name_invalid_prefix():
    """Test that invalid prefix raises error."""
    with pytest.raises(ValueError, match="prefix may only use"):
        SnowflakeNameGenerator(SnowflakeNameConfig(prefix="bad-prefix"))

    with pytest.raises(ValueError, match="prefix may only use"):
        SnowflakeNameGenerator(SnowflakeNameConfig(prefix="test_prefix"))


@pytest.mark.unit
def test_snowflake_name_invalid_node_id():
    """Test that invalid node_id raises error."""
    with pytest.raises(ValueError, match="node_id must be in"):
        SnowflakeNameGenerator(SnowflakeNameConfig(node_id=-1))

    with pytest.raises(ValueError, match="node_id must be in"):
        SnowflakeNameGenerator(SnowflakeNameConfig(node_id=256))


@pytest.mark.unit
def test_snowflake_name_invalid_wrap_bits():
    """Test that invalid wrap_bits raises error."""
    with pytest.raises(ValueError, match="wrap_bits must be in"):
        SnowflakeNameGenerator(SnowflakeNameConfig(wrap_bits=0))

    with pytest.raises(ValueError, match="wrap_bits must be in"):
        SnowflakeNameGenerator(SnowflakeNameConfig(wrap_bits=33))


@pytest.mark.unit
def test_snowflake_name_sequence_overflow(monkeypatch):
    """Test sequence overflow handling."""
    fixed_ts = int(datetime(2025, 1, 1, 12, 0, 0).timestamp() * 1000)

    gen = SnowflakeNameGenerator(SnowflakeNameConfig(node_id=1, prefix="TEST"))
    call_count = {"count": 0}

    def fake_epoch_ms():
        # After 256 calls advance time by 1 ms to break overflow wait loop
        val = fixed_ts if call_count["count"] < 256 else fixed_ts + 1
        call_count["count"] += 1
        return val

    monkeypatch.setattr(gen, "_epoch_ms", fake_epoch_ms)

    names = [gen.next_name(day=date(2025, 1, 1)) for _ in range(256)]

    # All should be unique
    assert len(set(names)) == 256


@pytest.mark.unit
def test_snowflake_checksum_calculation():
    """Test checksum calculation."""
    gen = SnowflakeNameGenerator(SnowflakeNameConfig(prefix="TEST"))

    checksum = gen._checksum_byte(0x123456789ABC)
    assert 0 <= checksum <= 255
    assert isinstance(checksum, int)

    checksum2 = gen._checksum_byte(0x123456789ABC)
    assert checksum == checksum2
