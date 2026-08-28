"""Crash recovery.

A crash is simulated by backdating the lease rather than by killing a process, so
these tests reproduce exactly what recovery reacts to (an expired lease on a
PROCESSING row) without waiting out a lease or depending on process control.
The manual crash demonstration in the README covers the real kill.
"""

import asyncio
from collections.abc import Callable

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Job, JobStatus
from app.services import job_service
from app.services.attempt import Attempt
from app.services.job_service import LEASE_EXPIRED_ERROR, AttemptOutcome
from app.services.queue_service import ReadyQueue
from tests.factories import expire_lease, insert_job, make_due, reload
from worker.recovery import LeaseRecovery

WORKER_A = "worker-a"
WORKER_B = "worker-b"


async def _claim_and_expire(session: AsyncSession, **overrides) -> Attempt:
    """A worker that claimed a job and then stopped heartbeating."""
    job = await insert_job(session, **overrides)
    claimed = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed is not None
    await expire_lease(session, job.id)
    return Attempt.of(claimed)


async def test_an_expired_lease_is_recovered_as_a_failed_attempt(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    attempt = await _claim_and_expire(session)

    assert await make_recovery().run_once() == 1

    stored = await reload(session, attempt.job_id)
    assert stored.status is JobStatus.SCHEDULED
    assert stored.error == LEASE_EXPIRED_ERROR
    assert stored.scheduled_at is not None
    # The attempt was consumed at claim time and is not given back.
    assert stored.attempt_count == 1
    assert stored.worker_id is None
    assert stored.execution_token is None
    assert stored.lease_expires_at is None


async def test_recovery_applies_the_normal_retry_backoff(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    attempt = await _claim_and_expire(session)

    await make_recovery().run_once()

    stored = await reload(session, attempt.job_id)
    assert stored.scheduled_at is not None
    now = await session.scalar(select(func.now()))
    delay = (stored.scheduled_at - now).total_seconds()
    assert 28 <= delay <= 30


async def test_a_live_lease_is_never_recovered(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    job = await insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER_A, lease_seconds=60)
    assert claimed is not None

    assert await make_recovery().run_once() == 0

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.PROCESSING
    assert stored.worker_id == WORKER_A
    assert stored.execution_token == claimed.execution_token


async def test_only_processing_jobs_are_recovered(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    """A leftover lease on a finished job is not a reason to run it again."""
    attempt = await _claim_and_expire(session)
    await job_service.complete_job(session, attempt, {"status": "sent"})
    await expire_lease(session, attempt.job_id)

    assert await make_recovery().run_once() == 0
    assert (await reload(session, attempt.job_id)).status is JobStatus.COMPLETED


async def test_an_expired_final_attempt_fails_permanently(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    attempt = await _claim_and_expire(session, attempt_count=2)
    assert attempt.attempt_count == 3

    assert await make_recovery().run_once() == 1

    stored = await reload(session, attempt.job_id)
    assert stored.status is JobStatus.FAILED
    assert stored.error == LEASE_EXPIRED_ERROR
    assert stored.completed_at is not None
    assert stored.attempt_count == 3
    assert stored.worker_id is None
    assert stored.execution_token is None
    assert stored.lease_expires_at is None


async def test_a_recovered_job_runs_again_with_a_new_attempt(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    """The crashed attempt is spent, so the retry is attempt two of three."""
    attempt = await _claim_and_expire(session)
    await make_recovery().run_once()

    await make_due(session, attempt.job_id)
    activated = await job_service.activate_due_scheduled_jobs(session, limit=10)
    assert len(activated) == 1
    reclaimed = await job_service.claim_job(session, attempt.job_id, WORKER_B)

    assert reclaimed is not None
    assert reclaimed.attempt_count == 2
    assert reclaimed.worker_id == WORKER_B
    assert reclaimed.execution_token != attempt.execution_token


async def test_recovery_does_not_enqueue_anything(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_recovery: Callable[..., LeaseRecovery],
) -> None:
    """Recovery hands the job to the scheduler; it never publishes directly."""
    await _claim_and_expire(session)

    await make_recovery().run_once()

    assert await ready_queue.size() == 0


async def test_concurrent_recovery_loops_recover_a_job_exactly_once(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    """Three recovery loops, one expired job, one recovery."""
    attempt = await _claim_and_expire(session)
    loops = [make_recovery(worker_id=f"recovery-{index}") for index in range(3)]

    counts = await asyncio.gather(*(loop.run_once() for loop in loops))

    assert sum(counts) == 1
    stored = await reload(session, attempt.job_id)
    assert stored.status is JobStatus.SCHEDULED
    assert stored.attempt_count == 1


async def test_concurrent_recovery_loops_split_a_batch_without_overlap(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    for _ in range(6):
        await _claim_and_expire(session)
    loops = [make_recovery(worker_id=f"recovery-{index}") for index in range(3)]

    counts = await asyncio.gather(*(loop.run_once() for loop in loops))

    assert sum(counts) == 6
    statuses = (await session.execute(select(Job.status))).scalars().all()
    assert all(status is JobStatus.SCHEDULED for status in statuses)


async def test_a_heartbeat_during_the_sweep_keeps_the_job(session: AsyncSession) -> None:
    """The write-time expiry check, exercised at the exact race it protects.

    Recovery selected the row while the lease was expired; before the write
    landed, the owner's heartbeat pushed the lease forward. The private call is
    used deliberately: it is the only way to interleave the two halves of a
    recovery that is otherwise a single transaction.
    """
    attempt = await _claim_and_expire(session)

    await job_service.extend_lease(session, attempt, lease_seconds=60)
    result = await job_service._apply_failed_attempt(
        session, attempt, LEASE_EXPIRED_ERROR, require_expired_lease=True
    )
    await session.commit()

    assert result is None
    stored = await reload(session, attempt.job_id)
    assert stored.status is JobStatus.PROCESSING
    assert stored.worker_id == WORKER_A
    assert stored.execution_token == attempt.execution_token


async def test_recovery_reports_which_outcome_it_applied(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """One sweep can retry one job and permanently fail another."""
    retryable = await _claim_and_expire(session)
    exhausted = await _claim_and_expire(session, attempt_count=2)

    async with session_factory() as recovery_session:
        results = await job_service.recover_expired_leases(recovery_session, limit=10)

    by_job = {result.job.id: result.outcome for result in results}
    assert by_job[retryable.job_id] is AttemptOutcome.RETRY_SCHEDULED
    assert by_job[exhausted.job_id] is AttemptOutcome.PERMANENTLY_FAILED


async def test_recovery_loop_recovers_without_being_driven(
    session: AsyncSession, make_recovery: Callable[..., LeaseRecovery]
) -> None:
    attempt = await _claim_and_expire(session)
    stop = asyncio.Event()
    loop_task = asyncio.create_task(make_recovery().run_forever(stop))

    try:
        for _ in range(500):
            if (await reload(session, attempt.job_id)).status is JobStatus.SCHEDULED:
                break
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(loop_task, timeout=5)

    assert (await reload(session, attempt.job_id)).status is JobStatus.SCHEDULED
