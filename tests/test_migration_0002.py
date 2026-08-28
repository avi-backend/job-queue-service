"""Upgrade path from the Phase 3 schema.

The shared test database is already at head, so these cases use a throwaway
database that is built from 0001, seeded with a pre-token PROCESSING row, and
then upgraded. That is the only way to prove the data step in 0002, rather than
the behaviour of a database that was always on the new schema.
"""

import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.db.models import JobStatus
from app.services import job_service
from tests.conftest import MAINTENANCE_DATABASE, PROJECT_ROOT, TEST_URL

MIGRATION_DATABASE = f"{TEST_URL.database}_migration_0002"
MIGRATION_URL: URL = TEST_URL.set(database=MIGRATION_DATABASE)
LEGACY_REVISION = "0001_initial_schema"
TOKEN_REVISION = "0002_execution_token"

LEGACY_JOB_ID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
PENDING_JOB_ID = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
COMPLETED_JOB_ID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")


def _alembic(*args: str) -> None:
    environment = {
        **os.environ,
        "DATABASE_URL": MIGRATION_URL.render_as_string(hide_password=False),
    }
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"alembic {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}")


async def _recreate_database(url: URL) -> None:
    """Build an empty database so each test starts from revision 0001, not head."""
    connection = await asyncpg.connect(
        user=url.username,
        password=url.password,
        host=url.host,
        port=url.port,
        database=MAINTENANCE_DATABASE,
    )
    try:
        await connection.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            url.database,
        )
        await connection.execute(f'DROP DATABASE IF EXISTS "{url.database}"')
        await connection.execute(f'CREATE DATABASE "{url.database}"')
    finally:
        await connection.close()


@pytest.fixture
async def migration_engine() -> AsyncIterator[AsyncEngine]:
    """Fresh 0001 schema, upgraded only when a test asks for it."""
    await _recreate_database(MIGRATION_URL)
    _alembic("upgrade", LEGACY_REVISION)
    engine = create_async_engine(
        MIGRATION_URL.render_as_string(hide_password=False), poolclass=NullPool
    )
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def migration_sessions(migration_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=migration_engine, expire_on_commit=False, autoflush=False)


async def _seed_phase3_rows(engine: AsyncEngine) -> None:
    """Insert jobs against the 0001 schema: no execution_token column exists yet."""
    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO jobs (
                    id, type, payload, status, priority,
                    attempt_count, max_attempts, worker_id, lease_expires_at
                )
                VALUES
                    (
                        :legacy_id, 'email',
                        '{"to":"user@example.com","subject":"Hello"}'::jsonb,
                        'processing', 0, 1, 3, 'worker-phase3', NULL
                    ),
                    (
                        :pending_id, 'email',
                        '{"to":"user@example.com","subject":"Hello"}'::jsonb,
                        'pending', 0, 0, 3, NULL, NULL
                    ),
                    (
                        :completed_id, 'email',
                        '{"to":"user@example.com","subject":"Hello"}'::jsonb,
                        'completed', 0, 1, 3, NULL, NULL
                    )
                """
            ),
            {
                "legacy_id": LEGACY_JOB_ID,
                "pending_id": PENDING_JOB_ID,
                "completed_id": COMPLETED_JOB_ID,
            },
        )


async def _load(session: AsyncSession, job_id: uuid.UUID):
    job = await job_service.get_job(session, job_id)
    assert job is not None
    await session.refresh(job)
    return job


async def test_upgrade_releases_a_pre_token_processing_job(
    migration_engine: AsyncEngine, migration_sessions: async_sessionmaker[AsyncSession]
) -> None:
    """A Phase 3 PROCESSING row must not stay PROCESSING after 0002.

    Recovery looks for `lease_expires_at < now()`. Phase 3 left that column
    NULL, so the comparison is unknown and the job would otherwise be stranded.
    """
    await _seed_phase3_rows(migration_engine)
    _alembic("upgrade", TOKEN_REVISION)

    async with migration_sessions() as session:
        legacy = await _load(session, LEGACY_JOB_ID)
        pending = await _load(session, PENDING_JOB_ID)
        completed = await _load(session, COMPLETED_JOB_ID)

        assert legacy.status is JobStatus.SCHEDULED
        assert legacy.scheduled_at is not None
        assert legacy.attempt_count == 1
        assert legacy.worker_id is None
        assert legacy.execution_token is None
        assert legacy.lease_expires_at is None

        # Other rows are not part of the stranded set and must be left alone.
        assert pending.status is JobStatus.PENDING
        assert pending.attempt_count == 0
        assert completed.status is JobStatus.COMPLETED
        assert completed.attempt_count == 1

        # Recovery still cannot see it: there is no expired lease, and that is
        # the point of the data step. The scheduler is the activation path.
        recovered = await job_service.recover_expired_leases(session, limit=10)
        assert recovered == []

        activated = await job_service.activate_due_scheduled_jobs(session, limit=10)
        assert [job.id for job in activated] == [LEGACY_JOB_ID]
        assert (await _load(session, LEGACY_JOB_ID)).status is JobStatus.PENDING


async def test_upgrade_does_not_touch_a_fenced_processing_job(
    migration_engine: AsyncEngine, migration_sessions: async_sessionmaker[AsyncSession]
) -> None:
    """A row already claimed under 4A has a token and must keep its ownership."""
    await _seed_phase3_rows(migration_engine)
    _alembic("upgrade", TOKEN_REVISION)

    live_token = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
    async with migration_engine.begin() as connection:
        await connection.execute(
            text(
                """
                UPDATE jobs
                SET status = 'processing',
                    worker_id = 'worker-4a',
                    execution_token = :token,
                    lease_expires_at = now() + interval '60 seconds',
                    attempt_count = 1
                WHERE id = :job_id
                """
            ),
            {"token": live_token, "job_id": PENDING_JOB_ID},
        )

    # Re-run the same data predicate the migration uses, as a stand-in for a
    # second apply / a later 4A claim that already has a token.
    async with migration_engine.begin() as connection:
        await connection.execute(
            text(
                """
                UPDATE jobs
                SET
                    status = 'scheduled',
                    scheduled_at = now(),
                    worker_id = NULL,
                    execution_token = NULL,
                    lease_expires_at = NULL
                WHERE status = 'processing'
                  AND execution_token IS NULL
                """
            )
        )

    async with migration_sessions() as session:
        live = await _load(session, PENDING_JOB_ID)
        assert live.status is JobStatus.PROCESSING
        assert live.worker_id == "worker-4a"
        assert live.execution_token == live_token
        assert live.lease_expires_at is not None
