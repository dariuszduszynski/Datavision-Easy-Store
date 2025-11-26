import sys
from pathlib import Path

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core import DesReader, DesWriter  # noqa: E402


def test_des_round_trip_basic(tmp_path: Path) -> None:
    des_path = tmp_path / "sample.des"

    files = {
        "hello.txt": (b"hello", {"mime": "text/plain"}),
        "bin.dat": (b"\x00\x01\x02", {"mime": "application/octet-stream"}),
        "notes with_space.txt": (b"notes", {"meta": {"k": "v"}, "type": "text"}),
    }

    with DesWriter(str(des_path)) as writer:
        for name, (data, meta) in files.items():
            writer.add_file(name, data, meta=meta)

    with DesReader(str(des_path)) as reader:
        assert set(reader.list_files()) == set(files.keys())
        for name, (data, meta) in files.items():
            assert name in reader
            assert reader.get_file(name) == data
            assert reader.get_meta(name) == meta


def test_des_index_sanity(tmp_path: Path) -> None:
    des_path = tmp_path / "sanity.des"
    inputs = {
        "a.txt": (b"a", {"i": 1}),
        "b.bin": (b"\x10\x20\x30", {"i": 2}),
        "c.json": (b'{"ok":true}', {"i": 3}),
    }

    with DesWriter(str(des_path)) as writer:
        for name, (data, meta) in inputs.items():
            writer.add_file(name, data, meta=meta)

    with DesReader(str(des_path)) as reader:
        entries = reader.get_index()
        data_region_end = reader.data_start + reader.data_length
        meta_region_end = reader.meta_start + reader.meta_length

        names = {e.name for e in entries}
        assert names == set(inputs.keys())

        for entry in entries:
            assert entry.data_offset >= reader.data_start
            assert entry.data_length >= 0
            assert entry.data_offset + entry.data_length <= data_region_end

            assert entry.meta_offset >= reader.meta_start
            assert entry.meta_length >= 0
            assert entry.meta_offset + entry.meta_length <= meta_region_end
