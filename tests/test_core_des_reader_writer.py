import sys
from pathlib import Path

import pytest

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core import DesReader, DesWriter  # noqa: E402


@pytest.mark.unit
def test_des_writer_reader_happy_path(tmp_path: Path) -> None:
    des_path = tmp_path / "sample.des"
    files = {
        "a.txt": (b"hello", {"mime": "text/plain"}),
        "b.bin": (b"\x00\x01\x02", {"mime": "application/octet-stream"}),
        "c.log": (b"line1\nline2", {"mime": "text/plain"}),
    }

    with DesWriter(str(des_path)) as writer:
        for name, (data, meta) in files.items():
            writer.add_file(name, data, meta=meta)
    # writer context should have closed underlying file
    assert writer._f.closed  # type: ignore[attr-defined]

    with DesReader(str(des_path)) as reader:
        assert set(reader.list_files()) == set(files.keys())

        for name, (data, meta) in files.items():
            assert name in reader
            assert reader.get_file(name) == data
            assert reader.get_meta(name) == meta

        index = reader.get_index()
        assert len(index) == len(files)
        names = {e.name for e in index}
        assert names == set(files.keys())
        for entry in index:
            assert entry.data_offset >= 0
            assert entry.data_length >= 0
            assert entry.meta_offset >= 0
            assert entry.meta_length >= 0


@pytest.mark.unit
def test_des_reader_contains(tmp_path: Path) -> None:
    des_path = tmp_path / "contains.des"

    with DesWriter(str(des_path)) as writer:
        writer.add_file("a.txt", b"hello", meta={"mime": "text/plain"})

    with DesReader(str(des_path)) as reader:
        assert "a.txt" in reader
        assert "missing.txt" not in reader


@pytest.mark.unit
def test_des_reader_corrupted_footer(tmp_path: Path) -> None:
    des_path = tmp_path / "corrupted.des"
    with DesWriter(str(des_path)) as writer:
        writer.add_file("a.txt", b"hello", meta={"mime": "text/plain"})

    # Corrupt the footer bytes
    with des_path.open("r+b") as f:
        f.seek(-16, 2)
        f.write(b"\xFF" * 16)

    with pytest.raises(Exception):
        DesReader(str(des_path))


@pytest.mark.unit
def test_des_reader_invalid_magic_version(tmp_path: Path) -> None:
    des_path = tmp_path / "badmagic.des"
    with DesWriter(str(des_path)) as writer:
        writer.add_file("a.txt", b"hello", meta={"mime": "text/plain"})

    with des_path.open("r+b") as f:
        f.seek(-72, 2)  # FOOTER size from des_core.py
        # overwrite magic and version (8sB)
        f.write(b"BADMAGIC")
        f.write(b"\xFF")  # invalid version

    with pytest.raises(Exception):
        DesReader(str(des_path))


@pytest.mark.unit
def test_des_reader_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.des"
    with pytest.raises(FileNotFoundError):
        DesReader(str(missing))
