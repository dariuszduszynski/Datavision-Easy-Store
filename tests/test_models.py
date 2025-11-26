"""Tests for DES data models."""
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core.models import IndexEntry, DesStats, DesFooter, ExternalFileInfo  # noqa: E402
from des.core.constants import FLAG_IS_EXTERNAL, FLAG_COMPRESSED, FLAG_ENCRYPTED  # noqa: E402


@pytest.mark.unit
def test_index_entry_basic():
    """Test basic IndexEntry creation."""
    entry = IndexEntry(
        name="test.txt",
        data_offset=100,
        data_length=50,
        meta_offset=200,
        meta_length=10,
        flags=0,
    )

    assert entry.name == "test.txt"
    assert entry.data_offset == 100
    assert entry.data_length == 50
    assert not entry.is_external()
    assert not entry.is_compressed()
    assert not entry.is_encrypted()


@pytest.mark.unit
def test_index_entry_flags():
    """Test IndexEntry flag methods."""
    entry = IndexEntry(
        name="big.dat",
        data_offset=0,
        data_length=1000000,
        meta_offset=100,
        meta_length=10,
        flags=FLAG_IS_EXTERNAL,
    )
    assert entry.is_external()
    assert not entry.is_compressed()

    entry = IndexEntry(
        name="compressed.gz",
        data_offset=100,
        data_length=500,
        meta_offset=200,
        meta_length=10,
        flags=FLAG_COMPRESSED,
    )
    assert not entry.is_external()
    assert entry.is_compressed()

    entry = IndexEntry(
        name="encrypted_compressed.dat",
        data_offset=100,
        data_length=500,
        meta_offset=200,
        meta_length=10,
        flags=FLAG_COMPRESSED | FLAG_ENCRYPTED,
    )
    assert entry.is_compressed()
    assert entry.is_encrypted()
    assert not entry.is_external()


@pytest.mark.unit
def test_index_entry_repr():
    """Test IndexEntry string representation."""
    entry = IndexEntry(
        name="test.txt",
        data_offset=100,
        data_length=50,
        meta_offset=200,
        meta_length=10,
        flags=FLAG_IS_EXTERNAL,
    )

    repr_str = repr(entry)
    assert "test.txt" in repr_str
    assert "data_offset=100" in repr_str
    assert "EXTERNAL" in repr_str


@pytest.mark.unit
def test_des_stats_basic():
    """Test DesStats creation and properties."""
    stats = DesStats(
        total_files=100,
        internal_files=90,
        external_files=10,
        internal_size_bytes=1000000,
        external_size_bytes=500000000,
        archive_size_bytes=1200000,
    )

    assert stats.total_files == 100
    assert stats.total_size_bytes == 501000000
    assert stats.compression_ratio > 0


@pytest.mark.unit
def test_des_stats_compression_ratio():
    """Test compression ratio calculation."""
    stats = DesStats(
        total_files=10,
        internal_files=10,
        external_files=0,
        internal_size_bytes=1000000,
        external_size_bytes=0,
        archive_size_bytes=500000,
    )

    assert stats.compression_ratio == 0.5  # 50% compression


@pytest.mark.unit
def test_des_stats_zero_internal_size():
    """Test compression ratio with zero internal size."""
    stats = DesStats(
        total_files=5,
        internal_files=0,
        external_files=5,
        internal_size_bytes=0,
        external_size_bytes=1000000,
        archive_size_bytes=100,
    )

    assert stats.compression_ratio == 0.0


@pytest.mark.unit
def test_des_stats_repr():
    """Test DesStats string representation."""
    stats = DesStats(
        total_files=100,
        internal_files=90,
        external_files=10,
        internal_size_bytes=1000000,
        external_size_bytes=500000000,
        archive_size_bytes=1200000,
    )

    repr_str = repr(stats)
    assert "files=100" in repr_str
    assert "90 internal" in repr_str
    assert "10 external" in repr_str


@pytest.mark.unit
def test_des_stats_human_size():
    """Test human-readable size formatting."""
    stats = DesStats(
        total_files=1,
        internal_files=1,
        external_files=0,
        internal_size_bytes=1536,  # 1.5 KB
        external_size_bytes=0,
        archive_size_bytes=1024,
    )

    assert "KB" in repr(stats) or "B" in repr(stats)


@pytest.mark.unit
def test_des_footer_basic():
    """Test DesFooter creation."""
    footer = DesFooter(
        magic=b"DESFOOT1",
        version=1,
        data_start=16,
        data_length=1000,
        meta_start=1016,
        meta_length=200,
        index_start=1216,
        index_length=500,
        file_count=10,
    )

    assert footer.magic == b"DESFOOT1"
    assert footer.version == 1
    assert footer.file_count == 10


@pytest.mark.unit
def test_des_footer_validate_success():
    """Test successful footer validation."""
    footer = DesFooter(
        magic=b"DESFOOT1",
        version=1,
        data_start=16,
        data_length=1000,
        meta_start=1016,
        meta_length=200,
        index_start=1216,
        index_length=500,
        file_count=10,
    )

    footer.validate()  # Should not raise


@pytest.mark.unit
def test_des_footer_validate_bad_magic():
    """Test footer validation with bad magic."""
    footer = DesFooter(
        magic=b"BADMAGIC",
        version=1,
        data_start=16,
        data_length=1000,
        meta_start=1016,
        meta_length=200,
        index_start=1216,
        index_length=500,
        file_count=10,
    )

    with pytest.raises(ValueError, match="Invalid footer magic"):
        footer.validate()


@pytest.mark.unit
def test_des_footer_validate_bad_version():
    """Test footer validation with unsupported version."""
    footer = DesFooter(
        magic=b"DESFOOT1",
        version=99,
        data_start=16,
        data_length=1000,
        meta_start=1016,
        meta_length=200,
        index_start=1216,
        index_length=500,
        file_count=10,
    )

    with pytest.raises(ValueError, match="Unsupported DES version"):
        footer.validate()


@pytest.mark.unit
def test_des_footer_validate_negative_offsets():
    """Test footer validation with negative offsets."""
    footer = DesFooter(
        magic=b"DESFOOT1",
        version=1,
        data_start=-1,
        data_length=1000,
        meta_start=1016,
        meta_length=200,
        index_start=1216,
        index_length=500,
        file_count=10,
    )

    with pytest.raises(ValueError, match="Invalid data region"):
        footer.validate()


@pytest.mark.unit
def test_des_footer_repr():
    """Test DesFooter string representation."""
    footer = DesFooter(
        magic=b"DESFOOT1",
        version=1,
        data_start=16,
        data_length=1000,
        meta_start=1016,
        meta_length=200,
        index_start=1216,
        index_length=500,
        file_count=10,
    )

    repr_str = repr(footer)
    assert "version=1" in repr_str
    assert "files=10" in repr_str


@pytest.mark.unit
def test_external_file_info():
    """Test ExternalFileInfo creation."""
    info = ExternalFileInfo(
        name="big_file.dat",
        s3_key="2025-01-15/_bigFiles/big_file.dat",
        size_bytes=500000000,
    )

    assert info.name == "big_file.dat"
    rep = repr(info)
    assert "big_file.dat" in rep
    assert "MB" in rep
