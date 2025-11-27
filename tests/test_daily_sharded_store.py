import sys
from datetime import date
from pathlib import Path

import pytest

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core import DesReader  # noqa: E402
from des.packer.daily_sharded_store import (  # noqa: E402
    DailyShardedDesStore,
    iter_daily_des_files,
)


@pytest.mark.integration
def test_daily_sharded_store_end_to_end(tmp_path: Path) -> None:
    target_day = date(2025, 11, 26)
    store = DailyShardedDesStore(
        base_dir=tmp_path,
        shard_bits=4,
        day=target_day,
        node_id=1,
        prefix="TEST",
    )

    files = {
        "f1": (b"alpha", {"id": 1}),
        "f2": (b"beta", {"id": 2}),
        "f3": (b"gamma", {"id": 3}),
    }

    logical_to_data = {}
    for _, (data, meta) in files.items():
        name, _ = store.add_file(data=data, meta=meta, ext=None)
        logical_to_data[name] = (data, meta)

    store.close()

    des_files = list(iter_daily_des_files(tmp_path, target_day))
    assert des_files

    found = {}
    for des_path in des_files:
        with DesReader(str(des_path)) as reader:
            for fname in reader.list_files():
                found[fname] = (reader.get_file(fname), reader.get_meta(fname))

    assert set(found.keys()) == set(logical_to_data.keys())
    for name, (data, meta) in logical_to_data.items():
        assert found[name][0] == data
        assert found[name][1] == meta
