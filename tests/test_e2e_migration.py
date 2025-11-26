import asyncio
from typing import Dict

import pytest
from sqlalchemy import select, text

from des.core.s3_des_reader import S3DesReader
from des.db.connector import DesContainer
from des.packer.multi_shard_packer import MultiShardPacker, PendingFile
from des.packer.storage import S3StorageBackend


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_full_migration_flow(
    des_db,
    s3_client_mock,
    source_db_mock,
    tmp_path,
    test_config,
):
    _ = source_db_mock  # ensure fixture is used
    shard_id = 1

    test_files: Dict[str, bytes] = {
        "file1.txt": b"content1",
        "file2.bin": b"\x00\x01\x02",
        "file3.dat": b"x" * 1000,
    }

    for name, content in test_files.items():
        s3_client_mock.put_object(Bucket="test-source", Key=f"buffer/{name}", Body=content)

    async with des_db.session_factory() as session:
        for idx, name in enumerate(test_files.keys(), start=1):
            await session.execute(
                text(
                    "INSERT INTO source_files (id, shard_id, name, status, claimed_by, claimed_at) "
                    "VALUES (:id, :shard_id, :name, :status, NULL, NULL)"
                ),
                {"id": idx, "shard_id": shard_id, "name": name, "status": "pending"},
            )
        await session.commit()

    class SourceDbProvider:
        def __init__(self, db, s3_client):
            self.db = db
            self.s3 = s3_client

        async def get_pending_files(self, shard_id: int, limit: int):
            async with self.db.session_factory() as session:
                result = await session.execute(
                    text(
                        "SELECT id, shard_id, name FROM source_files "
                        "WHERE shard_id = :shard_id AND status = 'pending' "
                        "ORDER BY id LIMIT :limit"
                    ),
                    {"shard_id": shard_id, "limit": limit},
                )
                rows = result.fetchall()
                if not rows:
                    await session.commit()
                    return []

                pending = []
                for row in rows:
                    obj = await asyncio.to_thread(
                        self.s3.get_object, Bucket="test-source", Key=f"buffer/{row.name}"
                    )
                    data = obj["Body"].read()
                    pending.append(
                        PendingFile(
                            shard_id=row.shard_id,
                            name=row.name,
                            data=data,
                            meta={"source_id": row.id},
                        )
                    )
                for row in rows:
                    await session.execute(
                        text("UPDATE source_files SET status = :status WHERE id = :id"),
                        {"status": "packed", "id": row.id},
                    )
                await session.commit()
                return pending

    storage = S3StorageBackend(s3_client_mock, bucket="test-archive")
    packer = MultiShardPacker(
        db=des_db,
        storage=storage,
        shard_ids=[shard_id],
        config=test_config,
        source_provider=SourceDbProvider(des_db, s3_client_mock),
    )

    await packer._process_shard(shard_id)
    await packer._finalize_writer(shard_id)
    if shard_id in packer._heartbeats:
        await packer._heartbeats[shard_id].stop()

    async with des_db.session_factory() as session:
        container = (
            await session.execute(select(DesContainer).where(DesContainer.shard_id == shard_id))
        ).scalar_one_or_none()
        assert container is not None
        container_id = container.id
        s3_key = container.s3_key

        status_rows = await session.execute(
            text("SELECT status FROM source_files WHERE shard_id = :shard_id"),
            {"shard_id": shard_id},
        )
        statuses = {row.status for row in status_rows.fetchall()}

    head = s3_client_mock.head_object(Bucket="test-archive", Key=s3_key)
    assert head["ContentLength"] > 0

    reader = S3DesReader("test-archive", s3_key, s3_client=s3_client_mock)
    archive_files = reader.get_files_batch(list(test_files.keys()))

    assert container_id is not None
    assert set(archive_files.keys()) == set(test_files.keys())
    assert statuses == {"packed"}
    for name, content in test_files.items():
        assert archive_files[name] == content
