import sys
import time
from pathlib import Path

import pytest

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core.des_core import DesReader, DesWriter  # noqa: E402

pytestmark = [pytest.mark.slow, pytest.mark.integration]


def test_perf_smoke_des_round_trip(tmp_path: Path) -> None:
    des_path = tmp_path / "perf.des"
    num_files = 1000
    data = b"x" * 64
    files = {f"file_{i:04d}.bin": (data, {"i": i}) for i in range(num_files)}

    t0 = time.perf_counter()
    with DesWriter(str(des_path)) as writer:
        for name, (blob, meta) in files.items():
            writer.add_file(name, blob, meta=meta)
    write_time = time.perf_counter() - t0

    t1 = time.perf_counter()
    with DesReader(str(des_path)) as reader:
        lst = reader.list_files()
        _ = reader.get_file(lst[0])
    read_time = time.perf_counter() - t1

    # Soft expectations for CI environments; adjust if needed
    assert write_time < 2.0
    assert read_time < 2.0

    print(f"Perf smoke: write_time={write_time:.3f}s read_time={read_time:.3f}s")
