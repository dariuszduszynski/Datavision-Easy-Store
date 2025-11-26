"""Tests for storage backends."""

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.packer.storage import S3StorageBackend  # noqa: E402


@pytest.mark.unit
def test_s3_storage_backend_init():
    """Test S3 storage backend initialization."""
    s3_mock = Mock()
    backend = S3StorageBackend(s3_client=s3_mock, bucket="test-bucket")

    assert backend.s3 == s3_mock
    assert backend.bucket == "test-bucket"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_s3_storage_backend_upload():
    """Test S3 upload."""
    s3_mock = Mock()
    backend = S3StorageBackend(s3_client=s3_mock, bucket="test-bucket")

    await backend.upload("/tmp/test.des", "2025-11-26/shard_00.des")

    s3_mock.upload_file.assert_called_once()
    args = s3_mock.upload_file.call_args[0]
    assert args[0] == "/tmp/test.des"
    assert args[1] == "test-bucket"
    assert args[2] == "2025-11-26/shard_00.des"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_s3_storage_backend_upload_with_prefix():
    """Test S3 upload with key prefix."""
    s3_mock = Mock()
    backend = S3StorageBackend(s3_client=s3_mock, bucket="test-bucket", prefix="base")

    await backend.upload("/tmp/test.des", "shard_00.des")

    args = s3_mock.upload_file.call_args[0]
    assert args[2] == "base/shard_00.des"
