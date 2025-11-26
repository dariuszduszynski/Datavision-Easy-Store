import sys
from datetime import date, datetime
from pathlib import Path

import pytest

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.packer.daily_sharded_store import DailyShardedDesStore  # noqa: E402
from des.utils.snowflake_name import SnowflakeNameConfig, SnowflakeNameGenerator  # noqa: E402
from des.core import DesReader  # noqa: E402


def test_snowflake_basic_format(monkeypatch) -> None:
    fixed_ts_ms = int(datetime(2025, 1, 1, 12, 0, 0).timestamp() * 1000)

    def fake_epoch_ms():
        return fixed_ts_ms

    gen = SnowflakeNameGenerator(SnowflakeNameConfig(node_id=1, prefix="TEST", wrap_bits=16))
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

    gen = SnowflakeNameGenerator(SnowflakeNameConfig(node_id=2, prefix="NODE", wrap_bits=16))
    monkeypatch.setattr(gen, "_epoch_ms", fake_epoch_ms)

    names = [gen.next_name(day=date(2025, 1, 1)) for _ in range(5)]
    assert names == sorted(names)
    assert len(set(names)) == len(names)


def test_daily_store_simple_pack(tmp_path: Path) -> None:
    store = DailyShardedDesStore(base_dir=tmp_path, shard_bits=4, node_id=1, prefix="TEST")
    inputs = {
        "file1.txt": b"hello",
        "file2.bin": b"\x00\x01",
        "file three.txt": b"world",
    }

    for name, data in inputs.items():
        store.add_file(data, meta={"name": name}, ext=None)
    store.close()

    today = date.today().isoformat()
    day_dir = tmp_path / today
    assert day_dir.exists()

    des_files = list(day_dir.glob("*.des"))
    assert des_files

    seen = {}
    for des_file in des_files:
        with DesReader(str(des_file)) as reader:
            for fname in reader.list_files():
                seen[fname] = reader.get_file(fname)

    assert set(seen.keys()) == set(inputs.keys())
    for name, data in inputs.items():
        assert seen[name] == data


def test_daily_store_rollover_between_days(tmp_path: Path, monkeypatch) -> None:
    day1 = date(2025, 1, 1)
    day2 = date(2025, 1, 2)

    # Patch date.today used inside DailyShardedDesStore via constructor argument
    store1 = DailyShardedDesStore(base_dir=tmp_path, shard_bits=3, day=day1, node_id=1, prefix="D1")
    store1.add_file(b"a", meta={"day": "1"})
    store1.close()

    store2 = DailyShardedDesStore(base_dir=tmp_path, shard_bits=3, day=day2, node_id=2, prefix="D2")
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
