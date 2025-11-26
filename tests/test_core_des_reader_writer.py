import sys
from pathlib import Path

import pytest

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core import DesReader, DesWriter  # noqa: E402
from des.core.constants import FOOTER_SIZE  # noqa: E402


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
    """Test that corrupted footer magic is detected."""
    des_path = tmp_path / "corrupted.des"
    with DesWriter(str(des_path)) as writer:
        writer.add_file("a.txt", b"hello", meta={"mime": "text/plain"})
    
    # Corrupt the footer magic (first 8 bytes of 72-byte footer)
    with des_path.open("r+b") as f:
        f.seek(-FOOTER_SIZE, 2)
        f.write(b"BADMAGIC")  # Overwrite magic bytes
    
    with pytest.raises(ValueError, match="Invalid DES footer magic"):
        DesReader(str(des_path))


@pytest.mark.unit
def test_des_reader_invalid_magic_version(tmp_path: Path) -> None:
    """Test that invalid version is detected."""
    des_path = tmp_path / "badmagic.des"
    with DesWriter(str(des_path)) as writer:
        writer.add_file("a.txt", b"hello", meta={"mime": "text/plain"})
    
    # Overwrite version byte (byte 8 in footer)
    with des_path.open("r+b") as f:
        f.seek(-FOOTER_SIZE + 8, 2)  # Skip magic (8 bytes), write version
        f.write(b"\xFF")  # Invalid version
    
    with pytest.raises(ValueError, match="Unsupported DES version"):
        DesReader(str(des_path))


@pytest.mark.unit
def test_des_reader_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.des"
    with pytest.raises(FileNotFoundError):
        DesReader(str(missing))


@pytest.mark.unit
def test_des_reader_negative_file_count(tmp_path: Path) -> None:
    """Test that negative file_count is detected."""
    des_path = tmp_path / "bad_count.des"
    with DesWriter(str(des_path)) as writer:
        writer.add_file("a.txt", b"hello", meta={"mime": "text/plain"})
    
    # Corrupt file_count (last 8 bytes of footer)
    with des_path.open("r+b") as f:
        f.seek(-8, 2)
        f.write(b"\xFF" * 8)  # Write -1 as signed int
    
    with pytest.raises(ValueError, match="Invalid.*footer"):
        DesReader(str(des_path))


@pytest.mark.unit
def test_des_reader_overlapping_regions(tmp_path: Path) -> None:
    """Test that overlapping regions are detected."""
    des_path = tmp_path / "overlapping.des"
    with DesWriter(str(des_path)) as writer:
        writer.add_file("a.txt", b"hello", meta={"mime": "text/plain"})
    
    # Make data_length huge so it overlaps meta region
    with des_path.open("r+b") as f:
        # data_length is at offset 24 in footer (after magic+version+reserved+data_start)
        f.seek(-FOOTER_SIZE + 24, 2)
        f.write((999999999).to_bytes(8, 'little'))
    
    with pytest.raises(ValueError, match="overlaps"):
        DesReader(str(des_path))


@pytest.mark.unit
def test_des_reader_invalid_offsets(tmp_path: Path) -> None:
    """Test that offsets exceeding file size are detected."""
    des_path = tmp_path / "bad_offset.des"
    with DesWriter(str(des_path)) as writer:
        writer.add_file("a.txt", b"hello", meta={"mime": "text/plain"})
    
    # Set meta_start beyond file size
    with des_path.open("r+b") as f:
        # meta_start is at offset 32 in footer
        f.seek(-FOOTER_SIZE + 32, 2)
        f.write((999999999).to_bytes(8, 'little'))
    
    with pytest.raises(ValueError, match="exceeds file size"):
        DesReader(str(des_path))
