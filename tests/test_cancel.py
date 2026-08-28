"""Cancellation of PENDING and SCHEDULED jobs.

The database transition is the concurrency boundary against a worker claim.
Redis cleanup is best-effort and always targets the current entry token.
"""

import asyncio
import uuid
from collections.abc import Callable

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.errors import JobConflictError
from app.core.time import utcnow
from app.db.models import JobStatus
from app.services import job_service
from app.services.queue_service import ENTRIES_KEY, ReadyQueue
from tests.factories import insert_job, job_request, reload
from worker.runner import JobRunner


async def test_cancel_pending_job(
    client: AsyncClient, session: AsyncSession, ready_queue: ReadyQueue
) -> None:
    response = await client.post("/jobs", json=job_request())
    job_id = response.json()["id"]
    assert await ready_queue.size() == 1

    cancelled = await client.post(f"/jobs/{job_id}/cancel")

    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["status"] == "cancelled"
    stored = await reload(session, uuid.UUID(job_id))
    assert stored.status is JobStatus.CANCELLED
    assert stored.worker_id is None
    assert stored.execution_token is None
    assert stored.lease_expires_at is None
    assert await ready_queue.size() == 0


async def test_cancel_scheduled_job(
    client: AsyncClient, session: AsyncSession, ready_queue: ReadyQueue
) -> None:
    from datetime import timedelta

    response = await client.post(
        "/jobs",
        json=job_request(scheduled_at=(utcnow() + timedelta(hours=1)).isoformat()),
    )
    job_id = response.json()["id"]

    cancelled = await client.post(f"/jobs/{job_id}/cancel")

    assert cancelled.status_code == 200
    assert (await reload(session, uuid.UUID(job_id))).status is JobStatus.CANCELLED
    assert await ready_queue.size() == 0


async def test_cancel_already_cancelled_is_idempotent(client: AsyncClient) -> None:
    """A second cancel returns the existing cancelled job instead of 409."""
    job_id = (await client.post("/jobs", json=job_request())).json()["id"]
    first = await client.post(f"/jobs/{job_id}/cancel")
    second = await client.post(f"/jobs/{job_id}/cancel")

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["status"] == "cancelled"
    assert second.json()["id"] == job_id


async def test_cancel_processing_is_conflict(
    client: AsyncClient, session: AsyncSession
) -> None:
    job_id = uuid.UUID((await client.post("/jobs", json=job_request())).json()["id"])
    claimed = await job_service.claim_job(session, job_id, "worker-a")
    assert claimed is not None

    response = await client.post(f"/jobs/{job_id}/cancel")

    assert response.status_code == 409
    assert (await reload(session, job_id)).status is JobStatus.PROCESSING


async def test_cancel_completed_is_conflict(
    client: AsyncClient, session: AsyncSession, make_runner: Callable[..., JobRunner]
) -> None:
    job_id = (await client.post("/jobs", json=job_request())).json()["id"]
    await make_runner().run_once()
    assert (await reload(session, uuid.UUID(job_id))).status is JobStatus.COMPLETED

    response = await client.post(f"/jobs/{job_id}/cancel")

    assert response.status_code == 409
    assert (await reload(session, uuid.UUID(job_id))).status is JobStatus.COMPLETED


async def test_cancel_failed_is_conflict(client: AsyncClient, session: AsyncSession) -> None:
    job = await insert_job(session, status=JobStatus.FAILED, error="gave up", attempt_count=3)

    response = await client.post(f"/jobs/{job.id}/cancel")

    assert response.status_code == 409
    assert (await reload(session, job.id)).status is JobStatus.FAILED


async def test_cancel_unknown_job_is_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/jobs/{uuid.uuid4()}/cancel")
    assert response.status_code == 404


async def test_cancelled_pending_job_cannot_execute_with_a_stale_queue_entry(
    client: AsyncClient,
    session: AsyncSession,
    ready_queue: ReadyQueue,
    redis,
    make_runner: Callable[..., JobRunner],
) -> None:
    """Database state wins: a leftover Redis member cannot resurrect a cancel."""
    job_id = uuid.UUID((await client.post("/jobs", json=job_request())).json()["id"])
    await client.post(f"/jobs/{job_id}/cancel")
    stale = await ready_queue.enqueue(job_id, 0)
    assert await ready_queue.size() == 1

    assert await make_runner().run_once() is False

    stored = await reload(session, job_id)
    assert stored.status is JobStatus.CANCELLED
    assert stored.attempt_count == 0
    assert stored.started_at is None
    # The stale entry is discarded because the job is no longer claimable.
    assert await ready_queue.size() == 0
    assert await redis.hget(ENTRIES_KEY, str(job_id)) is None
    assert stale.job_id == job_id


async def test_cancel_uses_the_current_queue_token(
    client: AsyncClient, ready_queue: ReadyQueue
) -> None:
    job_id = uuid.UUID((await client.post("/jobs", json=job_request())).json()["id"])
    original = await ready_queue.peek()
    assert original is not None
    replacement = await ready_queue.enqueue(job_id, 5)

    await client.post(f"/jobs/{job_id}/cancel")

    assert await ready_queue.size() == 0
    assert await ready_queue.score(original) is None
    assert await ready_queue.score(replacement) is None


async def test_cancel_survives_a_redis_cleanup_failure(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    ready_queue: ReadyQueue,
    redis,
) -> None:
    """PostgreSQL still wins if the index cleanup raises."""
    job = await insert_job(session)
    await ready_queue.enqueue(job.id, job.priority)

    class BrokenQueue(ReadyQueue):
        async def remove_current(self, job_id: uuid.UUID) -> bool:
            raise ConnectionError("redis is down")

    async with session_factory() as cancel_session:
        cancelled = await job_service.cancel_job(
            cancel_session, BrokenQueue(redis), job.id
        )

    assert cancelled.status is JobStatus.CANCELLED
    assert (await reload(session, job.id)).status is JobStatus.CANCELLED
    # The leftover entry cannot be claimed.
    assert await job_service.claim_job(session, job.id, "worker-a") is None


async def test_cancel_versus_claim_has_exactly_one_winner(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    ready_queue: ReadyQueue,
) -> None:
    """Eight cancels and eight claims race; PostgreSQL picks one outcome."""
    job = await insert_job(session)
    await ready_queue.enqueue(job.id, job.priority)

    async def claim(index: int):
        async with session_factory() as worker_session:
            return await job_service.claim_job(worker_session, job.id, f"worker-{index}")

    async def cancel():
        async with session_factory() as cancel_session:
            try:
                return await job_service.cancel_job(cancel_session, ready_queue, job.id)
            except JobConflictError:
                return None

    claims, cancels = await asyncio.gather(
        asyncio.gather(*(claim(index) for index in range(8))),
        asyncio.gather(*(cancel() for _ in range(8))),
    )

    claimed = [row for row in claims if row is not None]
    stored = await reload(session, job.id)

    if stored.status is JobStatus.PROCESSING:
        assert len(claimed) == 1
        assert stored.worker_id == claimed[0].worker_id
        assert stored.execution_token is not None
        assert stored.attempt_count == 1
        # Every cancel lost to the claim and surfaced as a conflict.
        assert all(row is None for row in cancels)
    else:
        assert stored.status is JobStatus.CANCELLED
        assert claimed == []
        assert stored.attempt_count == 0
        assert stored.worker_id is None
        # The first cancel won; later ones are the idempotent read of CANCELLED.
        assert all(row is not None and row.status is JobStatus.CANCELLED for row in cancels)
