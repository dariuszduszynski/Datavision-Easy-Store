import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from sqlalchemy import insert, select, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from des.core.des_writer import DesWriter
from des.db.connector import DesContainer, DesDbConnector, ShardLock
from des.packer.recovery import CrashRecoveryManager


@pytest.fixture
async def db_connector(tmp_path):
    """Async DB connector backed by temporary SQLite database."""
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'crash_recovery.db'}"
    connector = DesDbConnector(db_url)
    await connector.init_models()

    async with connector.engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS source_files (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    claimed_by TEXT,
                    claimed_at DATETIME
                )
                """
            )
        )

    yield connector

    await connector.engine.dispose()


def _write_des_file(path: Path) -> Path:
    with DesWriter(str(path)) as writer:
        writer.add_file("file1", b"hello world", meta={"source": "test"})
    return path


@pytest.mark.asyncio
async def test_recover_stale_claims_basic(db_connector):
    now = datetime.now(timezone.utc)
    stale_time = now - timedelta(seconds=1000)

    async with db_connector.session_factory() as session:
        await session.execute(
            text(
                "INSERT INTO source_files (id, status, claimed_by, claimed_at) "
                "VALUES (:id, :status, :claimed_by, :claimed_at)"
            ),
            [
                {
                    "id": 1,
                    "status": "claimed",
                    "claimed_by": "worker-1",
                    "claimed_at": stale_time,
                },
                {
                    "id": 2,
                    "status": "claimed",
                    "claimed_by": "worker-2",
                    "claimed_at": stale_time,
                },
            ],
        )
        await session.commit()

    manager = CrashRecoveryManager(
        db_connector, claim_timeout_seconds=300, cleanup_orphaned_s3=False
    )
    recovered = await manager.recover_stale_claims()
    assert recovered == 2

    async with db_connector.session_factory() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT status, claimed_by, claimed_at FROM source_files ORDER BY id"
                )
            )
        ).all()

    assert all(row.status == "pending" and row.claimed_by is None for row in rows)


@pytest.mark.asyncio
async def test_recover_stale_claims_respects_active(db_connector):
    now = datetime.now(timezone.utc)
    stale_time = now - timedelta(seconds=1000)
    fresh_time = now - timedelta(seconds=10)

    async with db_connector.session_factory() as session:
        await session.execute(
            text(
                "INSERT INTO source_files (id, status, claimed_by, claimed_at) "
                "VALUES (:id, :status, :claimed_by, :claimed_at)"
            ),
            [
                {
                    "id": 1,
                    "status": "claimed",
                    "claimed_by": "worker-1",
                    "claimed_at": stale_time,
                },
                {
                    "id": 2,
                    "status": "claimed",
                    "claimed_by": "worker-2",
                    "claimed_at": fresh_time,
                },
            ],
        )
        await session.commit()

    manager = CrashRecoveryManager(
        db_connector, claim_timeout_seconds=300, cleanup_orphaned_s3=False
    )
    recovered = await manager.recover_stale_claims()
    assert recovered == 1

    async with db_connector.session_factory() as session:
        rows = (
            await session.execute(
                text("SELECT id, status, claimed_by FROM source_files ORDER BY id")
            )
        ).all()

    assert rows[0].status == "pending"
    assert rows[1].status == "claimed"
    assert rows[1].claimed_by == "worker-2"


@pytest.mark.asyncio
async def test_cleanup_partial_containers_old_writing(db_connector, tmp_path):
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket = "des-test-bucket"
        s3_client.create_bucket(Bucket=bucket)

        manager = CrashRecoveryManager(
            db_connector,
            s3_client=s3_client,
            s3_bucket=bucket,
            container_grace_seconds=0,
            cleanup_orphaned_s3=False,
        )

        old_created = datetime.now(timezone.utc) - timedelta(hours=2)

        async with db_connector.session_factory() as session:
            container = DesContainer(
                shard_id=1,
                day=date.today(),
                status="writing",
                s3_key="missing.des",
                file_count=0,
                data_bytes=0,
                created_at=old_created,
                finalized_at=None,
            )
            session.add(container)
            await session.commit()
            container_id = container.id

        actions = await manager.cleanup_partial_containers()
        assert actions == 1

        async with db_connector.session_factory() as session:
            refreshed = await session.get(DesContainer, container_id)

        assert refreshed.status == "failed"
        assert refreshed.finalized_at is not None


@pytest.mark.asyncio
async def test_release_expired_locks(db_connector):
    now = datetime.now(timezone.utc)
    expired = now - timedelta(seconds=5)
    active = now + timedelta(seconds=60)

    async with db_connector.session_factory() as session:
        await session.execute(
            insert(ShardLock),
            [
                {
                    "shard_id": 1,
                    "holder_id": "a",
                    "acquired_at": now,
                    "heartbeat_at": now,
                    "expires_at": expired,
                    "state": "held",
                },
                {
                    "shard_id": 2,
                    "holder_id": "b",
                    "acquired_at": now,
                    "heartbeat_at": now,
                    "expires_at": active,
                    "state": "held",
                },
            ],
        )
        await session.commit()

    manager = CrashRecoveryManager(db_connector, cleanup_orphaned_s3=False)
    released = await manager.release_expired_locks()
    assert released == 1

    async with db_connector.session_factory() as session:
        locks = (await session.execute(select(ShardLock))).scalars().all()

    assert len(locks) == 1
    assert locks[0].shard_id == 2


@pytest.mark.asyncio
async def test_verify_container_integrity_success(db_connector, tmp_path):
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket = "des-test-bucket"
        s3_client.create_bucket(Bucket=bucket)

        des_path = _write_des_file(tmp_path / "shard_01.des")
        key = "2024-01-01/shard_01.des"
        s3_client.upload_file(str(des_path), bucket, key)

        async with db_connector.session_factory() as session:
            container = DesContainer(
                shard_id=1,
                day=date(2024, 1, 1),
                status="uploaded",
                s3_key=key,
                file_count=1,
                data_bytes=des_path.stat().st_size,
                created_at=datetime.now(timezone.utc),
                finalized_at=datetime.now(timezone.utc),
            )
            session.add(container)
            await session.commit()
            container_id = container.id

        manager = CrashRecoveryManager(
            db_connector,
            s3_client=s3_client,
            s3_bucket=bucket,
            cleanup_orphaned_s3=False,
        )
        actions = await manager.verify_container_integrity()
        assert actions == 0

        async with db_connector.session_factory() as session:
            refreshed = await session.get(DesContainer, container_id)

        assert refreshed.status == "uploaded"
        assert refreshed.file_count == 1


@pytest.mark.asyncio
async def test_verify_container_integrity_missing_s3(db_connector):
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket = "des-test-bucket"
        s3_client.create_bucket(Bucket=bucket)

        async with db_connector.session_factory() as session:
            container = DesContainer(
                shard_id=1,
                day=date.today(),
                status="uploaded",
                s3_key="missing.des",
                file_count=0,
                data_bytes=0,
                created_at=datetime.now(timezone.utc),
                finalized_at=None,
            )
            session.add(container)
            await session.commit()
            container_id = container.id

        manager = CrashRecoveryManager(
            db_connector,
            s3_client=s3_client,
            s3_bucket=bucket,
            cleanup_orphaned_s3=False,
        )
        actions = await manager.verify_container_integrity()
        assert actions == 1

        async with db_connector.session_factory() as session:
            refreshed = await session.get(DesContainer, container_id)

        assert refreshed.status == "failed"
