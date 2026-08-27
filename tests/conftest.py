"""Test fixtures.

Tests run against a real PostgreSQL database, not SQLite, because the parts we
care about (JSONB, the native status enum, and the partial unique index that
makes idempotency race-safe) are PostgreSQL behaviour.

A dedicated `<database>_test` database is created once and migrated with
Alembic, so tests exercise the same schema the migration produces. Each test
starts from truncated tables, so the suite can be run repeatedly.
"""

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.core.redis import get_redis
from app.db.session import get_session
from app.main import app
from app.services.queue_service import ReadyQueue
from worker.runner import JobRunner

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAINTENANCE_DATABASE = "postgres"

#: Kept away from the development database index so a test run cannot wipe it.
TEST_REDIS_DB = 15


def _test_database_url() -> URL:
    override = os.getenv("TEST_DATABASE_URL")
    if override:
        return make_url(override)
    url = make_url(settings.database_url)
    return url.set(database=f"{url.database}_test")


TEST_URL = _test_database_url()


def _test_redis_url() -> str:
    override = os.getenv("TEST_REDIS_URL")
    if override:
        return override
    base = settings.redis_url.rsplit("/", 1)[0]
    return f"{base}/{TEST_REDIS_DB}"


TEST_REDIS_URL = _test_redis_url()


async def _create_database_if_missing(url: URL) -> None:
    connection = await asyncpg.connect(
        user=url.username,
        password=url.password,
        host=url.host,
        port=url.port,
        database=MAINTENANCE_DATABASE,
    )
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", url.database
        )
        if not exists:
            # CREATE DATABASE cannot run inside a transaction block.
            await connection.execute(f'CREATE DATABASE "{url.database}"')
    finally:
        await connection.close()


@pytest.fixture(scope="session", autouse=True)
def migrated_test_database() -> None:
    """Create and migrate the test database once per test session."""
    asyncio.run(_create_database_if_missing(TEST_URL))

    environment = {
        **os.environ,
        "DATABASE_URL": TEST_URL.render_as_string(hide_password=False),
    }
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"alembic upgrade failed:\n{result.stdout}\n{result.stderr}")


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    # NullPool gives every session its own connection, which is what makes the
    # concurrency tests genuinely concurrent.
    test_engine = create_async_engine(
        TEST_URL.render_as_string(hide_password=False), poolclass=NullPool
    )
    try:
        yield test_engine
    finally:
        await test_engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


@pytest.fixture(autouse=True)
async def clean_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE TABLE job_logs, jobs RESTART IDENTITY CASCADE"))


@pytest.fixture
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Session for asserting directly against the database."""
    async with session_factory() as db_session:
        yield db_session


async def instant_sleep(seconds: float) -> None:
    """Stand-in for asyncio.sleep so simulated work costs no wall-clock time."""


@pytest.fixture
def make_runner(
    session_factory: async_sessionmaker[AsyncSession], ready_queue: ReadyQueue
) -> Callable[..., JobRunner]:
    """Build workers whose sleeps are instant and whose randomness is fixed."""

    def build(worker_id: str = "worker-1", random_value: float = 0.0) -> JobRunner:
        return JobRunner(
            session_factory=session_factory,
            ready_queue=ready_queue,
            worker_id=worker_id,
            poll_interval=0.01,
            sleep=instant_sleep,
            random_source=lambda: random_value,
        )

    return build


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    """Redis on a dedicated database index, emptied before each test."""
    client = Redis.from_url(TEST_REDIS_URL, encoding="utf-8", decode_responses=True)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def ready_queue(redis: Redis) -> ReadyQueue:
    return ReadyQueue(redis)


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> AsyncIterator[AsyncClient]:
    """API client whose requests each get their own database session."""

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as request_session:
            try:
                yield request_session
            except Exception:
                await request_session.rollback()
                raise

    async def override_get_redis() -> Redis:
        return redis

    app.dependency_overrides[get_session] = override_get_session
    app.dependency_overrides[get_redis] = override_get_redis
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client
    app.dependency_overrides.clear()
