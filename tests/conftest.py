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
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.db.session import get_session
from app.main import app

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAINTENANCE_DATABASE = "postgres"


def _test_database_url() -> URL:
    override = os.getenv("TEST_DATABASE_URL")
    if override:
        return make_url(override)
    url = make_url(settings.database_url)
    return url.set(database=f"{url.database}_test")


TEST_URL = _test_database_url()


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


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    """API client whose requests each get their own database session."""

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as request_session:
            try:
                yield request_session
            except Exception:
                await request_session.rollback()
                raise

    app.dependency_overrides[get_session] = override_get_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as async_client:
        yield async_client
    app.dependency_overrides.clear()
