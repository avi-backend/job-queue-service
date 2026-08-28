"""Scheduled-job activation, for both future jobs and delayed retries.

Nothing here waits for a schedule to arrive: scheduled_at is written into the
past with the database clock, which is the same clock the activation query
compares against.
"""

import asyncio
import uuid
from collections.abc import Callable
from datetime import timedelta

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.time import utcnow
from app.db.models import JobStatus
from app.services import job_service
from app.services.attempt import Attempt
from app.services.queue_service import ENTRIES_KEY, ReadyQueue
from tests.factories import insert_job, job_request, make_due, reload
from worker.runner import JobRunner
from worker.scheduler import JobScheduler

WORKER = "worker-a"


async def test_a_due_scheduled_job_becomes_pending_and_is_enqueued(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    job = await insert_job(
        session, status=JobStatus.SCHEDULED, scheduled_at=utcnow() - timedelta(seconds=1)
    )

    assert await make_scheduler().run_once() == 1

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.PENDING
    candidate = await ready_queue.peek()
    assert candidate is not None and candidate.job_id == job.id


async def test_a_future_scheduled_job_is_left_alone(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    job = await insert_job(
        session, status=JobStatus.SCHEDULED, scheduled_at=utcnow() + timedelta(hours=1)
    )

    assert await make_scheduler().run_once() == 0

    assert (await reload(session, job.id)).status is JobStatus.SCHEDULED
    assert await ready_queue.size() == 0


async def test_only_scheduled_jobs_are_activated(
    session: AsyncSession, make_scheduler: Callable[..., JobScheduler]
) -> None:
    """A due scheduled_at on a job in another state must not resurrect it."""
    past = utcnow() - timedelta(seconds=1)
    pending = await insert_job(session, status=JobStatus.PENDING, scheduled_at=past)
    failed = await insert_job(session, status=JobStatus.FAILED, scheduled_at=past)
    processing = await insert_job(session, status=JobStatus.PROCESSING, scheduled_at=past)

    assert await make_scheduler().run_once() == 0

    assert (await reload(session, pending.id)).status is JobStatus.PENDING
    assert (await reload(session, failed.id)).status is JobStatus.FAILED
    assert (await reload(session, processing.id)).status is JobStatus.PROCESSING


async def test_activation_respects_the_batch_size(
    session: AsyncSession, make_scheduler: Callable[..., JobScheduler]
) -> None:
    for _ in range(5):
        await insert_job(
            session, status=JobStatus.SCHEDULED, scheduled_at=utcnow() - timedelta(seconds=1)
        )

    scheduler = make_scheduler(batch_size=2)

    assert await scheduler.run_once() == 2
    assert await scheduler.run_once() == 2
    assert await scheduler.run_once() == 1
    assert await scheduler.run_once() == 0


async def test_earlier_schedules_are_activated_first(
    session: AsyncSession, make_scheduler: Callable[..., JobScheduler]
) -> None:
    now = utcnow()
    later = await insert_job(
        session, status=JobStatus.SCHEDULED, scheduled_at=now - timedelta(seconds=1)
    )
    earlier = await insert_job(
        session, status=JobStatus.SCHEDULED, scheduled_at=now - timedelta(minutes=5)
    )

    assert await make_scheduler(batch_size=1).run_once() == 1

    assert (await reload(session, earlier.id)).status is JobStatus.PENDING
    assert (await reload(session, later.id)).status is JobStatus.SCHEDULED


async def test_concurrent_schedulers_activate_a_job_exactly_once(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    """Mirrors `--scale worker=3`: three scheduler loops, one activation."""
    job = await insert_job(
        session, status=JobStatus.SCHEDULED, scheduled_at=utcnow() - timedelta(seconds=1)
    )
    schedulers = [make_scheduler(worker_id=f"scheduler-{index}") for index in range(3)]

    counts = await asyncio.gather(*(scheduler.run_once() for scheduler in schedulers))

    assert sum(counts) == 1
    assert (await reload(session, job.id)).status is JobStatus.PENDING
    # One activation means one queue entry, so FIFO order is not disturbed.
    assert await ready_queue.size() == 1


async def test_concurrent_schedulers_split_a_batch_without_overlap(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    due = utcnow() - timedelta(seconds=1)
    for _ in range(9):
        await insert_job(session, status=JobStatus.SCHEDULED, scheduled_at=due)
    schedulers = [make_scheduler(worker_id=f"scheduler-{index}") for index in range(3)]

    counts = await asyncio.gather(*(scheduler.run_once() for scheduler in schedulers))

    assert sum(counts) == 9
    assert await ready_queue.size() == 9


async def test_a_due_retry_is_activated_and_re_enqueued(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    """A retry travels the same path as any other scheduled job."""
    job = await insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER)
    assert claimed is not None
    await job_service.fail_attempt(session, Attempt.of(claimed), "smtp timeout")
    # Waiting out the 30 second backoff is what the clock rewrite replaces.
    assert await make_scheduler().run_once() == 0
    await make_due(session, job.id)

    assert await make_scheduler().run_once() == 1

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.PENDING
    assert stored.attempt_count == 1
    assert await ready_queue.size() == 1


async def test_a_retry_gets_a_new_queue_entry_that_stale_removal_cannot_delete(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    """Retry re-enqueue safety: the old candidate must not take the new one down.

    A worker observed entry 1, executed the job, and it failed. By the time the
    worker's queue cleanup arrives, the retry has already been activated under
    entry 2. Removing entry 1 must leave entry 2 queued.
    """
    job = await insert_job(session)
    stale_candidate = await ready_queue.enqueue(job.id, job.priority)

    claimed = await job_service.claim_job(session, job.id, WORKER)
    assert claimed is not None
    await job_service.fail_attempt(session, Attempt.of(claimed), "smtp timeout")
    await make_due(session, job.id)
    assert await make_scheduler().run_once() == 1

    fresh_candidate = await ready_queue.peek()
    assert fresh_candidate is not None
    assert fresh_candidate.member != stale_candidate.member

    # The late cleanup: it removed nothing, because its entry was replaced.
    assert await ready_queue.remove(stale_candidate) is False

    assert await ready_queue.size() == 1
    remaining = await ready_queue.peek()
    assert remaining is not None and remaining.member == fresh_candidate.member
    assert await ready_queue.current_member(job.id) == fresh_candidate.member


async def test_activation_survives_a_redis_failure_and_leaves_the_job_pending(
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    ready_queue: ReadyQueue,
    redis,
) -> None:
    """The documented DB -> Redis window: PostgreSQL stays correct, Redis lags.

    The transition is committed before the enqueue, so a Redis outage cannot
    lose the job. It becomes invisible to workers until something re-queues it,
    and the scheduler says so in the log rather than rolling the database back
    on a write whose outcome it cannot know.
    """
    job = await insert_job(
        session, status=JobStatus.SCHEDULED, scheduled_at=utcnow() - timedelta(seconds=1)
    )

    class BrokenQueue(ReadyQueue):
        async def enqueue(self, job_id: uuid.UUID, priority: int):
            raise ConnectionError("redis is down")

    scheduler = JobScheduler(
        session_factory=session_factory,
        ready_queue=BrokenQueue(redis),
        worker_id="scheduler-1",
        interval=0.01,
        batch_size=10,
    )

    assert await scheduler.run_once() == 1

    assert (await reload(session, job.id)).status is JobStatus.PENDING
    assert await ready_queue.size() == 0
    assert await redis.hlen(ENTRIES_KEY) == 0


async def test_scheduler_loop_activates_without_being_driven(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    job = await insert_job(
        session, status=JobStatus.SCHEDULED, scheduled_at=utcnow() - timedelta(seconds=1)
    )
    stop = asyncio.Event()
    loop_task = asyncio.create_task(make_scheduler().run_forever(stop))

    try:
        for _ in range(500):
            if (await reload(session, job.id)).status is JobStatus.PENDING:
                break
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(loop_task, timeout=5)

    assert (await reload(session, job.id)).status is JobStatus.PENDING
    assert await ready_queue.size() == 1


async def test_an_activated_retry_runs_to_completion_on_the_next_pass(
    client: AsyncClient,
    session: AsyncSession,
    make_runner: Callable[..., JobRunner],
    make_scheduler: Callable[..., JobScheduler],
) -> None:
    """End to end: failure, backoff, activation, successful second attempt."""
    response = await client.post("/jobs", json=job_request("webhook"))
    job_id = uuid.UUID(response.json()["id"])

    # 0.99 always trips the simulated webhook failure, 0.0 never does.
    assert await make_runner(random_value=0.99).run_once() is True
    failed_once = await reload(session, job_id)
    assert failed_once.status is JobStatus.SCHEDULED
    assert failed_once.attempt_count == 1

    await make_due(session, job_id)
    assert await make_scheduler().run_once() == 1
    assert await make_runner(random_value=0.0).run_once() is True

    completed = await reload(session, job_id)
    assert completed.status is JobStatus.COMPLETED
    assert completed.attempt_count == 2
    assert completed.error is None
