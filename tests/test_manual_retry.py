"""Manual retry of FAILED jobs."""

import uuid
from collections.abc import Callable

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import JobStatus
from app.services import job_service
from app.services.queue_service import ReadyQueue
from tests.factories import insert_job, job_request, reload
from worker.runner import JobRunner


async def test_retry_failed_job_becomes_pending(
    client: AsyncClient, session: AsyncSession, ready_queue: ReadyQueue
) -> None:
    job = await insert_job(
        session,
        status=JobStatus.FAILED,
        attempt_count=3,
        error="gave up",
        result={"status": "nope"},
        progress=40,
    )

    response = await client.post(f"/jobs/{job.id}/retry")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "pending"
    assert body["attempt_count"] == 0
    assert body["progress"] == 0
    assert body["error"] is None
    assert body["result"] is None
    assert body["completed_at"] is None
    assert body["started_at"] is None
    assert body["scheduled_at"] is None

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.PENDING
    assert stored.attempt_count == 0
    assert stored.max_attempts == 3
    assert stored.worker_id is None
    assert stored.execution_token is None
    assert stored.lease_expires_at is None
    assert await ready_queue.size() == 1
    candidate = await ready_queue.peek()
    assert candidate is not None and candidate.job_id == job.id


async def test_retried_job_receives_a_new_queue_entry(
    session: AsyncSession, ready_queue: ReadyQueue
) -> None:
    job = await insert_job(session, status=JobStatus.FAILED, attempt_count=3, error="gave up")
    stale = await ready_queue.enqueue(job.id, job.priority)

    retried = await job_service.retry_job(session, ready_queue, job.id)
    fresh = await ready_queue.peek()

    assert retried.status is JobStatus.PENDING
    assert fresh is not None
    assert fresh.member != stale.member
    assert await ready_queue.remove(stale) is False
    assert await ready_queue.size() == 1
    assert (await ready_queue.peek()).member == fresh.member


async def test_retried_job_is_executed_as_a_fresh_cycle(
    client: AsyncClient,
    session: AsyncSession,
    make_runner: Callable[..., JobRunner],
) -> None:
    job = await insert_job(
        session, status=JobStatus.FAILED, attempt_count=3, error="gave up", job_type="email"
    )
    await client.post(f"/jobs/{job.id}/retry")

    assert await make_runner().run_once() is True

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.COMPLETED
    assert stored.attempt_count == 1
    assert stored.error is None


async def test_retry_of_non_failed_job_is_conflict(client: AsyncClient) -> None:
    job_id = (await client.post("/jobs", json=job_request())).json()["id"]

    for path_status in ("pending",):
        response = await client.post(f"/jobs/{job_id}/retry")
        assert response.status_code == 409, path_status


async def test_retry_of_processing_completed_cancelled_is_conflict(
    client: AsyncClient, session: AsyncSession
) -> None:
    processing = await insert_job(session, status=JobStatus.PROCESSING, attempt_count=1)
    completed = await insert_job(session, status=JobStatus.COMPLETED, attempt_count=1)
    cancelled = await insert_job(session, status=JobStatus.CANCELLED)
    scheduled = await insert_job(session, status=JobStatus.SCHEDULED)

    for job in (processing, completed, cancelled, scheduled):
        response = await client.post(f"/jobs/{job.id}/retry")
        assert response.status_code == 409, job.status
        assert (await reload(session, job.id)).status is job.status


async def test_retry_unknown_job_is_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/jobs/{uuid.uuid4()}/retry")
    assert response.status_code == 404


async def test_retry_survives_a_redis_enqueue_failure(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    ready_queue: ReadyQueue,
    redis,
) -> None:
    job = await insert_job(session, status=JobStatus.FAILED, attempt_count=3, error="gave up")

    class BrokenQueue(ReadyQueue):
        async def enqueue(self, job_id: uuid.UUID, priority: int):
            raise ConnectionError("redis is down")

    async with session_factory() as retry_session:
        retried = await job_service.retry_job(retry_session, BrokenQueue(redis), job.id)

    stored = await reload(session, job.id)
    assert retried.status is JobStatus.PENDING
    assert stored.status is JobStatus.PENDING
    assert stored.attempt_count == 0
    assert await ready_queue.size() == 0
