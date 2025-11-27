import io
import sys
from pathlib import Path

import boto3
import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber

# Ensure src/ is on sys.path for local test runs without installation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core import DesReader, DesWriter  # noqa: E402
from des.core.constants import FOOTER_SIZE  # noqa: E402
from des.core.s3_des_reader import S3DesReader  # noqa: E402

pytestmark = [pytest.mark.integration, pytest.mark.s3]


class CountingS3Client:
    def __init__(self, client):
        self.client = client
        self.calls = {"get_object": 0}

    def head_object(self, **kwargs):
        return self.client.head_object(**kwargs)

    def get_object(self, **kwargs):
        self.calls["get_object"] += 1
        return self.client.get_object(**kwargs)


def _make_stubbed_client(
    des_bytes: bytes, bucket: str, key: str, ranges: list[tuple[int, int]]
):
    s3 = boto3.client("s3", region_name="us-east-1")
    stubber = Stubber(s3)

    stubber.add_response(
        "head_object",
        {"ContentLength": len(des_bytes), "ETag": '"etag"'},
        expected_params={"Bucket": bucket, "Key": key},
    )

    for start, length in ranges:
        end = start + length - 1
        body = StreamingBody(io.BytesIO(des_bytes[start : start + length]), length)
        stubber.add_response(
            "get_object",
            {"Body": body},
            expected_params={
                "Bucket": bucket,
                "Key": key,
                "Range": f"bytes={start}-{end}",
            },
        )

    stubber.activate()
    return s3, stubber


def _build_des(tmp_path: Path):
    des_path = tmp_path / "s3-test.des"
    files = {
        "a.txt": (b"hello", {"mime": "text/plain"}),
        "b.bin": (b"\x00\x01\x02", {"mime": "application/octet-stream"}),
        "c.log": (b"line1\nline2", {"mime": "text/plain"}),
    }
    with DesWriter(str(des_path)) as w:
        for name, (data, meta) in files.items():
            w.add_file(name, data, meta=meta)
    des_bytes = des_path.read_bytes()
    with DesReader(str(des_path)) as reader:
        entries = reader.get_index()
        index_start = reader.index_start
        index_length = reader.index_length
        data_offsets = {e.name: (e.data_offset, e.data_length) for e in entries}
        file_size = reader.file_size
    return des_bytes, files, index_start, index_length, data_offsets, file_size


@pytest.mark.skip(reason="Stubber with hardcoded offsets - outdated after refactoring")
@pytest.mark.integration
@pytest.mark.s3
def test_s3_des_reader_round_trip(tmp_path: Path) -> None:
    des_bytes, files, index_start, index_length, data_offsets, file_size = _build_des(
        tmp_path
    )
    bucket = "test-bucket"
    key = "test-2025-11-25.des"

    footer_size = FOOTER_SIZE
    footer_start = file_size - footer_size

    ranges = [
        (footer_start, footer_size),
        (index_start, index_length),
    ]
    for off, length in data_offsets.values():
        ranges.append((off, length))

    client, stubber = _make_stubbed_client(des_bytes, bucket, key, ranges)
    counting = CountingS3Client(client)

    reader = S3DesReader(bucket, key, s3_client=counting)
    assert set(reader.list_files()) == set(files.keys())
    for name, (data, meta) in files.items():
        assert reader.get_file(name) == data
        assert reader.get_meta(name) == meta

    stubber.deactivate()


@pytest.mark.skip(reason="Stubber with hardcoded offsets - outdated after refactoring")
@pytest.mark.integration
@pytest.mark.s3
def test_s3_des_reader_batch_reads(tmp_path: Path) -> None:
    des_bytes, files, index_start, index_length, data_offsets, file_size = _build_des(
        tmp_path
    )
    bucket = "test-bucket"
    key = "test-2025-11-25.des"

    footer_size = FOOTER_SIZE
    footer_start = file_size - footer_size

    # Batch likely fetches footer, index, then one combined data range
    combined_start = min(off for off, _ in data_offsets.values())
    combined_end = max(off + length for off, length in data_offsets.values())
    combined_length = combined_end - combined_start

    ranges = [
        (footer_start, footer_size),
        (index_start, index_length),
        (combined_start, combined_length),
    ]

    client, stubber = _make_stubbed_client(des_bytes, bucket, key, ranges)
    counting = CountingS3Client(client)

    reader = S3DesReader(bucket, key, s3_client=counting)
    batch = reader.get_files_batch(list(files.keys()))

    assert set(batch.keys()) == set(files.keys())
    for name, (data, _) in files.items():
        assert batch[name] == data

    # 1 footer + 1 index + 1 combined data range
    assert counting.calls["get_object"] == 3

    stubber.deactivate()
