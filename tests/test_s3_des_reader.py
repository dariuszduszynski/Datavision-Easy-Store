import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_s3

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core.des_core import DesReader, DesWriter  # noqa: E402
from des.core.s3_des_reader import S3DesReader  # noqa: E402


@pytest.mark.integration
@mock_s3
def test_s3_des_reader_round_trip(tmp_path: Path) -> None:
    bucket = "test-des-bucket"
    key = "archives/sample.des"

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)

    des_path = tmp_path / "sample.des"
    files = {
        "hello.txt": (b"hello", {"mime": "text/plain"}),
        "bin.dat": (b"\x00\x01\x02", {"mime": "application/octet-stream"}),
        "notes with_space.txt": (b"notes", {"meta": {"k": "v"}, "type": "text"}),
    }

    with DesWriter(str(des_path)) as writer:
        for name, (data, meta) in files.items():
            writer.add_file(name, data, meta=meta)

    s3.upload_file(str(des_path), bucket, key)

    reader = S3DesReader(bucket, key, s3_client=s3)
    assert set(reader.list_files()) == set(files.keys())

    for name, (data, meta) in files.items():
        assert reader.get_file(name) == data
        assert reader.get_meta(name) == meta


@pytest.mark.integration
@mock_s3
def test_s3_des_reader_batch_read(tmp_path: Path) -> None:
    bucket = "test-des-bucket"
    key = "archives/batch.des"

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)

    des_path = tmp_path / "batch.des"
    files = {
        "a.txt": (b"A", {"i": 1}),
        "b.txt": (b"B", {"i": 2}),
        "c.txt": (b"C", {"i": 3}),
    }

    with DesWriter(str(des_path)) as writer:
        for name, (data, meta) in files.items():
            writer.add_file(name, data, meta=meta)

    s3.upload_file(str(des_path), bucket, key)

    reader = S3DesReader(bucket, key, s3_client=s3)
    batch = reader.get_files_batch(list(files.keys()))

    assert set(batch.keys()) == set(files.keys())
    for name, (data, _) in files.items():
        assert batch[name] == data
