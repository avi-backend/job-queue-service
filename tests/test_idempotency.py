"""Idempotent submission, including the concurrency guarantee.

Idempotency here prevents duplicate job *creation*. It says nothing about
exactly-once execution.
"""

import asyncio
from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.time import utcnow
from app.db.models import Job, JobStatus
from app.schemas.jobs import JobCreateRequest
from app.services import job_service
from tests.factories import job_request

KEY = "idempotency-key-1"


async def _job_count(session: AsyncSession) -> int:
    return await session.scalar(select(func.count()).select_from(Job)) or 0


async def test_repeated_key_returns_same_job_and_creates_one_row(
    client: AsyncClient, session: AsyncSession
) -> None:
    headers = {"Idempotency-Key": KEY}
    body = job_request()

    first = await client.post("/jobs", json=body, headers=headers)
    second = await client.post("/jobs", json=body, headers=headers)

    assert first.status_code == 201, first.text
    assert second.status_code == 200, second.text
    assert first.json()["id"] == second.json()["id"]
    assert second.json() == first.json()
    assert await _job_count(session) == 1


async def test_replay_returns_original_job_even_for_a_different_body(
    client: AsyncClient, session: AsyncSession
) -> None:
    """The key identifies the submission, so the stored job is authoritative."""
    headers = {"Idempotency-Key": KEY}

    first = await client.post("/jobs", json=job_request("email"), headers=headers)
    second = await client.post("/jobs", json=job_request("webhook"), headers=headers)

    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["type"] == "email"
    assert await _job_count(session) == 1


async def test_different_keys_create_different_jobs(
    client: AsyncClient, session: AsyncSession
) -> None:
    first = await client.post("/jobs", json=job_request(), headers={"Idempotency-Key": "a"})
    second = await client.post("/jobs", json=job_request(), headers={"Idempotency-Key": "b"})

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["id"] != second.json()["id"]
    assert await _job_count(session) == 2


async def test_submissions_without_a_key_are_never_deduplicated(
    client: AsyncClient, session: AsyncSession
) -> None:
    first = await client.post("/jobs", json=job_request())
    second = await client.post("/jobs", json=job_request())

    assert first.json()["id"] != second.json()["id"]
    assert await _job_count(session) == 2


async def test_key_is_stored_with_a_24_hour_window(
    client: AsyncClient, session: AsyncSession
) -> None:
    await client.post("/jobs", json=job_request(), headers={"Idempotency-Key": KEY})

    job = (await session.execute(select(Job))).scalars().one()
    assert job.idempotency_key == KEY
    assert job.idempotency_expires_at is not None
    # At least 24h, allowing for the time taken by the request itself.
    assert job.idempotency_expires_at - job.created_at >= timedelta(hours=24) - timedelta(
        seconds=10
    )


async def test_concurrent_submissions_with_same_key_create_one_job(
    client: AsyncClient, session: AsyncSession
) -> None:
    """Two in-flight requests, one job.

    Deterministic regardless of interleaving: if the requests serialise, the
    second sees the live key; if they race, the loser hits the unique index and
    reloads the winner. Both paths give one row and one shared job id.
    """
    headers = {"Idempotency-Key": KEY}
    body = job_request()

    first, second = await asyncio.gather(
        client.post("/jobs", json=body, headers=headers),
        client.post("/jobs", json=body, headers=headers),
    )

    assert sorted([first.status_code, second.status_code]) == [200, 201], (
        first.text,
        second.text,
    )
    assert first.json()["id"] == second.json()["id"]
    assert await _job_count(session) == 1


async def test_many_concurrent_submissions_with_same_key_create_one_job(
    client: AsyncClient, session: AsyncSession
) -> None:
    headers = {"Idempotency-Key": KEY}
    body = job_request()

    responses = await asyncio.gather(
        *(client.post("/jobs", json=body, headers=headers) for _ in range(8))
    )

    assert [response.status_code for response in responses].count(201) == 1
    assert {response.json()["id"] for response in responses} == {responses[0].json()["id"]}
    assert await _job_count(session) == 1


async def test_expired_key_can_be_reused_for_a_new_job(
    client: AsyncClient, session: AsyncSession
) -> None:
    """Expiry is simulated by backdating the stored window, so no test sleeps."""
    first = await client.post("/jobs", json=job_request(), headers={"Idempotency-Key": KEY})
    original = (await session.execute(select(Job))).scalars().one()
    original.idempotency_expires_at = utcnow() - timedelta(seconds=1)
    await session.commit()

    second = await client.post("/jobs", json=job_request(), headers={"Idempotency-Key": KEY})

    assert second.status_code == 201, second.text
    assert second.json()["id"] != first.json()["id"]
    assert await _job_count(session) == 2

    # The expired key is released from the old job, so the index stays satisfied.
    holders = (
        await session.execute(select(Job.id).where(Job.idempotency_key == KEY))
    ).scalars().all()
    assert [str(holder) for holder in holders] == [second.json()["id"]]


async def test_lost_race_returns_the_winning_job(
    session_factory: async_sessionmaker[AsyncSession],
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force the exact interleaving where another request inserts first.

    The pre-insert lookup is made to miss once, which is what happens when a
    concurrent transaction commits between our check and our insert. The insert
    must then hit the unique index and the winner must be returned.
    """
    winner = Job(
        type="email",
        payload={"to": "user@example.com", "subject": "Hello", "body": None},
        status=JobStatus.PENDING,
        idempotency_key=KEY,
        idempotency_expires_at=utcnow() + timedelta(hours=24),
    )
    session.add(winner)
    await session.commit()

    real_lookup = job_service._find_active_by_key
    calls = {"count": 0}

    async def lookup_that_misses_once(db_session: AsyncSession, key: str) -> Job | None:
        calls["count"] += 1
        if calls["count"] == 1:
            return None
        return await real_lookup(db_session, key)

    monkeypatch.setattr(job_service, "_find_active_by_key", lookup_that_misses_once)

    async with session_factory() as loser_session:
        job, created = await job_service.create_job(
            loser_session,
            JobCreateRequest.model_validate(job_request()),
            idempotency_key=KEY,
        )

    assert created is False
    assert job.id == winner.id
    assert calls["count"] == 2
    assert await _job_count(session) == 1


async def test_database_rejects_a_second_live_key_across_sessions(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    """The partial unique index is the real guarantee, not the application check."""

    def build() -> Job:
        return Job(
            type="email",
            payload={"to": "user@example.com", "subject": "Hello", "body": None},
            status=JobStatus.PENDING,
            idempotency_key=KEY,
            idempotency_expires_at=utcnow() + timedelta(hours=24),
        )

    async with session_factory() as first_session, session_factory() as second_session:
        first_session.add(build())
        second_session.add(build())
        await first_session.commit()

        with pytest.raises(IntegrityError) as conflict:
            await second_session.commit()
        await second_session.rollback()

    assert job_service.IDEMPOTENCY_INDEX_NAME in str(conflict.value.orig)
    assert job_service._is_idempotency_conflict(conflict.value) is True
    assert await _job_count(session) == 1


async def test_released_keys_do_not_block_the_index(
    session_factory: async_sessionmaker[AsyncSession], session: AsyncSession
) -> None:
    """Many jobs may hold a NULL key at once; the index only covers live keys."""

    async with session_factory() as db_session:
        for _ in range(3):
            db_session.add(
                Job(
                    type="email",
                    payload={"to": "user@example.com", "subject": "Hello", "body": None},
                    status=JobStatus.PENDING,
                )
            )
        await db_session.commit()

    assert await _job_count(session) == 3


async def test_key_longer_than_the_column_is_rejected(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs", json=job_request(), headers={"Idempotency-Key": "k" * 256}
    )

    assert response.status_code == 422
