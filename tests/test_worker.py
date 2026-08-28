"""Worker loop against real PostgreSQL and Redis.

Every runner here is built with an instant sleep and a fixed random source, so
the tests are deterministic and finish immediately.
"""

import asyncio
import uuid
from collections.abc import Callable
from datetime import timedelta

from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.time import utcnow
from app.db.models import Job, JobStatus
from app.services import job_service
from app.services.queue_service import ENTRIES_KEY, ReadyQueue
from tests.factories import expire_lease, job_request, make_due
from worker.recovery import LeaseRecovery
from worker.runner import JobRunner
from worker.scheduler import JobScheduler

# 0.0 always succeeds, 0.99 always trips the simulated webhook failure.
ALWAYS_SUCCEED = 0.0
ALWAYS_FAIL = 0.99


async def _submit(client: AsyncClient, job_type: str = "email", **overrides) -> str:
    response = await client.post("/jobs", json=job_request(job_type, **overrides))
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _load(session: AsyncSession, job_id: str) -> Job:
    job = (await session.execute(select(Job).where(Job.id == uuid.UUID(job_id)))).scalars().one()
    await session.refresh(job)
    return job


async def test_worker_completes_an_email_job(
    client: AsyncClient,
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_runner: Callable[..., JobRunner],
) -> None:
    job_id = await _submit(client)

    assert await make_runner().run_once() is True

    job = await _load(session, job_id)
    assert job.status is JobStatus.COMPLETED
    assert job.result is not None and job.result["status"] == "sent"
    assert job.progress == 100
    assert job.completed_at is not None
    assert job.started_at is not None
    assert job.attempt_count == 1
    assert job.error is None
    assert job.worker_id is None
    assert job.execution_token is None
    assert job.lease_expires_at is None
    # The index entry is dropped only after durable ownership.
    assert await ready_queue.size() == 0


async def test_worker_completes_a_report_job(
    client: AsyncClient, session: AsyncSession, make_runner: Callable[..., JobRunner]
) -> None:
    job_id = await _submit(client, "report")

    await make_runner().run_once()

    job = await _load(session, job_id)
    assert job.status is JobStatus.COMPLETED
    assert job.result is not None
    assert job.result["file_url"] == f"https://example.local/reports/{job_id}.pdf"


async def test_worker_completes_a_webhook_job(
    client: AsyncClient, session: AsyncSession, make_runner: Callable[..., JobRunner]
) -> None:
    job_id = await _submit(client, "webhook")

    await make_runner(random_value=ALWAYS_SUCCEED).run_once()

    job = await _load(session, job_id)
    assert job.status is JobStatus.COMPLETED
    assert job.result == {"status_code": 200, "delivered": True}


async def test_worker_reschedules_a_failed_webhook_for_retry(
    client: AsyncClient,
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_runner: Callable[..., JobRunner],
) -> None:
    """A first failure is a retry, not the end of the job."""
    job_id = await _submit(client, "webhook")

    await make_runner(random_value=ALWAYS_FAIL).run_once()

    job = await _load(session, job_id)
    assert job.status is JobStatus.SCHEDULED
    assert job.error is not None and "503" in job.error
    assert job.scheduled_at is not None
    assert job.completed_at is None
    assert job.attempt_count == 1
    assert job.result is None
    # A waiting retry is neither owned nor queued: the scheduler publishes it
    # when the backoff has elapsed.
    assert job.worker_id is None
    assert job.execution_token is None
    assert job.lease_expires_at is None
    assert await ready_queue.size() == 0


async def test_worker_fails_a_job_permanently_on_its_final_attempt(
    client: AsyncClient,
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_runner: Callable[..., JobRunner],
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    """Three attempts, two backoffs, then FAILED for good."""
    job_id = await _submit(client, "webhook")
    runner = make_runner(random_value=ALWAYS_FAIL)

    for expected_attempt in (1, 2, 3):
        assert await runner.run_once() is True
        job = await _load(session, job_id)
        assert job.attempt_count == expected_attempt
        if expected_attempt < 3:
            assert job.status is JobStatus.SCHEDULED
            await make_due(session, uuid.UUID(job_id))
            assert await make_scheduler().run_once() == 1

    job = await _load(session, job_id)
    assert job.status is JobStatus.FAILED
    assert job.completed_at is not None
    assert job.attempt_count == 3
    assert await ready_queue.size() == 0
    # Nothing brings it back: no fourth attempt is ever scheduled.
    assert await make_scheduler().run_once() == 0
    assert await runner.run_once() is False


async def test_batch_job_reaches_full_progress(
    client: AsyncClient, session: AsyncSession, make_runner: Callable[..., JobRunner]
) -> None:
    job_id = await _submit(client, "batch")

    await make_runner().run_once()

    job = await _load(session, job_id)
    assert job.status is JobStatus.COMPLETED
    assert job.progress == 100
    assert job.result == {"processed": 2, "failed": 0, "total": 2}


async def test_batch_progress_is_persisted_while_processing(
    client: AsyncClient, session: AsyncSession, make_runner: Callable[..., JobRunner]
) -> None:
    """Progress is observable mid-flight, not only at completion."""
    response = await client.post(
        "/jobs", json={"type": "batch", "payload": {"items": [1, 2, 3, 4]}}
    )
    job_id = response.json()["id"]
    observed: list[int] = []

    runner = make_runner()
    original = runner._progress_reporter

    def tracking_reporter(progress_session, attempt):
        report = original(progress_session, attempt)

        async def wrapped(processed: int, total: int) -> None:
            await report(processed, total)
            observed.append(
                await progress_session.scalar(
                    select(Job.progress).where(Job.id == attempt.job_id)
                )
            )

        return wrapped

    runner._progress_reporter = tracking_reporter
    await runner.run_once()

    assert observed == [25, 50, 75, 100]
    job = await _load(session, job_id)
    assert job.progress == 100


async def test_worker_does_nothing_when_the_queue_is_empty(
    make_runner: Callable[..., JobRunner],
) -> None:
    assert await make_runner().run_once() is False


async def test_worker_clears_the_entry_mapping_it_consumed(
    client: AsyncClient, ready_queue: ReadyQueue, redis: Redis, make_runner
) -> None:
    """Removing by exact token also compare-and-deletes the job's mapping."""
    await _submit(client)

    await make_runner().run_once()

    assert await ready_queue.size() == 0
    assert await redis.hlen(ENTRIES_KEY) == 0


async def test_worker_removal_spares_a_newer_entry_for_the_same_job(
    client: AsyncClient, ready_queue: ReadyQueue, redis: Redis
) -> None:
    """The exact scenario token-scoped removal exists to prevent.

    A worker observes a candidate, the job is re-queued afterwards, and the
    worker's removal then arrives late. It must drop only what it observed.
    """
    job_id = uuid.UUID(await _submit(client))
    observed = await ready_queue.peek()
    assert observed is not None

    requeued = await ready_queue.enqueue(job_id, 5)
    assert await ready_queue.remove(observed) is False

    assert await ready_queue.size() == 1
    remaining = await ready_queue.peek()
    assert remaining is not None and remaining.member == requeued.member
    assert await redis.hget(ENTRIES_KEY, str(job_id)) == requeued.member


async def test_scheduled_job_is_never_picked_up(
    client: AsyncClient,
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_runner: Callable[..., JobRunner],
) -> None:
    job_id = await _submit(client, scheduled_at=(utcnow() + timedelta(hours=1)).isoformat())

    assert await ready_queue.size() == 0
    assert await make_runner().run_once() is False

    job = await _load(session, job_id)
    assert job.status is JobStatus.SCHEDULED
    assert job.worker_id is None
    assert job.attempt_count == 0


async def test_stale_queue_entry_is_discarded_without_executing(
    session: AsyncSession, ready_queue: ReadyQueue, make_runner: Callable[..., JobRunner]
) -> None:
    """A queued job that is no longer PENDING must not run."""
    job = Job(
        type="email",
        payload={"to": "user@example.com", "subject": "Hello", "body": None},
        status=JobStatus.CANCELLED,
    )
    session.add(job)
    await session.commit()
    await ready_queue.enqueue(job.id, 0)

    assert await make_runner().run_once() is False

    await session.refresh(job)
    assert job.status is JobStatus.CANCELLED
    assert job.attempt_count == 0
    assert job.worker_id is None
    assert await ready_queue.size() == 0


async def test_queue_entry_for_a_deleted_job_is_discarded(
    ready_queue: ReadyQueue, make_runner: Callable[..., JobRunner]
) -> None:
    await ready_queue.enqueue(uuid.uuid4(), 0)

    assert await make_runner().run_once() is False
    assert await ready_queue.size() == 0


async def test_worker_processes_highest_priority_first(
    client: AsyncClient, session: AsyncSession, make_runner: Callable[..., JobRunner]
) -> None:
    low = await _submit(client, priority=1)
    high = await _submit(client, priority=99)
    medium = await _submit(client, priority=50)

    runner = make_runner()
    order: list[str] = []
    seen: set[str] = set()

    for _ in range(3):
        assert await runner.run_once() is True
        completed = {
            str(job_id)
            for job_id in (
                await session.execute(select(Job.id).where(Job.status == JobStatus.COMPLETED))
            ).scalars()
        }
        just_finished = completed - seen
        assert len(just_finished) == 1
        order.append(just_finished.pop())
        seen = completed

    assert order == [high, medium, low]


async def test_worker_processes_equal_priority_in_fifo_order(
    client: AsyncClient, session: AsyncSession, make_runner: Callable[..., JobRunner]
) -> None:
    submitted = [await _submit(client, priority=5) for _ in range(4)]
    runner = make_runner()

    for index, expected in enumerate(submitted):
        assert await runner.run_once() is True
        assert (await _load(session, expected)).status is JobStatus.COMPLETED
        # Everything queued later must still be untouched.
        for later in submitted[index + 1 :]:
            assert (await _load(session, later)).status is JobStatus.PENDING


async def test_two_workers_cannot_execute_the_same_job(
    client: AsyncClient, session: AsyncSession, make_runner: Callable[..., JobRunner]
) -> None:
    """Exactly one worker executes a single queued job."""
    job_id = await _submit(client)

    results = await asyncio.gather(
        make_runner(worker_id="worker-a").run_once(),
        make_runner(worker_id="worker-b").run_once(),
    )

    assert results.count(True) == 1
    job = await _load(session, job_id)
    assert job.status is JobStatus.COMPLETED
    # One claim means one attempt and one execution token, now released.
    assert job.attempt_count == 1
    assert job.execution_token is None


async def test_three_workers_drain_a_burst_exactly_once(
    client: AsyncClient, session: AsyncSession, ready_queue: ReadyQueue, make_runner
) -> None:
    """Mirrors `--scale worker=3`: every job reaches a terminal state once."""
    job_ids = [await _submit(client, priority=index % 10) for index in range(9)]
    workers = [make_runner(worker_id=f"worker-{index}") for index in range(3)]

    # Each pass gives every worker one chance to take a job.
    for _ in range(len(job_ids)):
        await asyncio.gather(*(worker.run_once() for worker in workers))
        if await ready_queue.size() == 0:
            break

    jobs = (await session.execute(select(Job))).scalars().all()
    for job in jobs:
        await session.refresh(job)

    assert len(jobs) == len(job_ids)
    assert all(job.status is JobStatus.COMPLETED for job in jobs)
    assert all(job.attempt_count == 1 for job in jobs)
    assert all(job.execution_token is None for job in jobs)
    assert await ready_queue.size() == 0


async def test_three_full_workers_handle_mixed_work_safely(
    client: AsyncClient,
    session: AsyncSession,
    session_factory,
    ready_queue: ReadyQueue,
    make_runner: Callable[..., JobRunner],
    make_scheduler: Callable[..., JobScheduler],
    make_recovery: Callable[..., LeaseRecovery],
) -> None:
    """`--scale worker=3` with all three loops running in every replica.

    Ready jobs, a due scheduled job and an abandoned PROCESSING job are handled
    at the same time by three of each loop. Every job must be touched exactly
    once: no duplicate execution, no duplicate activation, no double recovery.
    """
    ready = [await _submit(client, priority=index) for index in range(4)]
    due = await _submit(client, scheduled_at=(utcnow() + timedelta(hours=1)).isoformat())
    await make_due(session, uuid.UUID(due))

    abandoned = await _submit(client)
    async with session_factory() as claim_session:
        claimed = await job_service.claim_job(
            claim_session, uuid.UUID(abandoned), "worker-crashed"
        )
        assert claimed is not None
        await expire_lease(claim_session, uuid.UUID(abandoned))

    stop = asyncio.Event()
    loops = []
    for index in range(3):
        loops.append(make_runner(worker_id=f"worker-{index}").run_forever(stop))
        loops.append(make_scheduler(worker_id=f"worker-{index}").run_forever(stop))
        loops.append(make_recovery(worker_id=f"worker-{index}").run_forever(stop))
    running = asyncio.gather(*loops)

    try:
        for _ in range(500):
            finished = [
                (await _load(session, job_id)).status is JobStatus.COMPLETED
                for job_id in [*ready, due]
            ]
            recovered = (await _load(session, abandoned)).status is JobStatus.SCHEDULED
            if all(finished) and recovered:
                break
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(running, timeout=10)

    for job_id in [*ready, due]:
        job = await _load(session, job_id)
        assert job.status is JobStatus.COMPLETED
        assert job.attempt_count == 1

    crashed = await _load(session, abandoned)
    assert crashed.status is JobStatus.SCHEDULED
    assert crashed.error == job_service.LEASE_EXPIRED_ERROR
    # Recovered once: a second recovery would have consumed another attempt.
    assert crashed.attempt_count == 1
    assert crashed.execution_token is None
    assert await ready_queue.size() == 0


async def test_run_forever_returns_immediately_when_already_stopped(
    make_runner: Callable[..., JobRunner],
) -> None:
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(make_runner().run_forever(stop), timeout=5)


async def test_run_forever_drains_work_then_exits_on_signal(
    client: AsyncClient, session: AsyncSession, ready_queue: ReadyQueue, make_runner
) -> None:
    """The polling loop picks work up on its own and stops when signalled."""
    job_id = await _submit(client)
    stop = asyncio.Event()
    loop_task = asyncio.create_task(make_runner().run_forever(stop))

    try:
        for _ in range(500):
            if await ready_queue.size() == 0 and (
                await _load(session, job_id)
            ).status is JobStatus.COMPLETED:
                break
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(loop_task, timeout=5)

    assert (await _load(session, job_id)).status is JobStatus.COMPLETED
