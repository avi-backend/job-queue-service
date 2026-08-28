"""The atomic claim is the ownership boundary, so it is tested against real PostgreSQL."""

import asyncio
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.errors import OwnershipLostError
from app.core.time import utcnow
from app.db.models import Job, JobStatus
from app.services import job_service
from app.services.attempt import Attempt

WORKER = "worker-a"


async def _insert_job(
    session: AsyncSession, status: JobStatus = JobStatus.PENDING, **overrides
) -> Job:
    job = Job(
        type="email",
        payload={"to": "user@example.com", "subject": "Hello", "body": None},
        status=status,
        **overrides,
    )
    session.add(job)
    await session.commit()
    return job


async def test_claim_transitions_pending_to_processing(session: AsyncSession) -> None:
    job = await _insert_job(session)

    claimed = await job_service.claim_job(session, job.id, WORKER)

    assert claimed is not None
    assert claimed.status is JobStatus.PROCESSING
    assert claimed.worker_id == WORKER
    assert claimed.started_at is not None
    assert claimed.attempt_count == 1


async def test_claim_increments_attempt_count_exactly_once(session: AsyncSession) -> None:
    job = await _insert_job(session)

    await job_service.claim_job(session, job.id, WORKER)

    stored = await session.scalar(select(Job.attempt_count).where(Job.id == job.id))
    assert stored == 1


async def test_second_worker_cannot_claim_a_claimed_job(session: AsyncSession) -> None:
    job = await _insert_job(session)

    first = await job_service.claim_job(session, job.id, WORKER)
    second = await job_service.claim_job(session, job.id, "worker-b")

    assert first is not None
    assert second is None
    stored = (await session.execute(select(Job).where(Job.id == job.id))).scalars().one()
    await session.refresh(stored)
    assert stored.worker_id == WORKER
    assert stored.attempt_count == 1


@pytest.mark.parametrize(
    "status",
    [JobStatus.SCHEDULED, JobStatus.PROCESSING, JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED],
)
async def test_only_pending_jobs_can_be_claimed(
    session: AsyncSession, status: JobStatus
) -> None:
    job = await _insert_job(session, status=status)

    assert await job_service.claim_job(session, job.id, WORKER) is None


async def test_claiming_an_unknown_job_returns_none(session: AsyncSession) -> None:
    assert await job_service.claim_job(session, uuid.uuid4(), WORKER) is None


async def test_claim_refuses_to_exceed_max_attempts(session: AsyncSession) -> None:
    """Guards the attempt_count <= max_attempts check constraint."""
    job = await _insert_job(session, attempt_count=3, max_attempts=3)

    assert await job_service.claim_job(session, job.id, WORKER) is None


async def test_concurrent_claims_yield_exactly_one_winner(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Eight workers race for one job; PostgreSQL must pick a single owner."""
    job = await _insert_job(session)

    async def claim(worker_id: str) -> Job | None:
        async with session_factory() as worker_session:
            return await job_service.claim_job(worker_session, job.id, worker_id)

    results = await asyncio.gather(*(claim(f"worker-{index}") for index in range(8)))

    winners = [claimed for claimed in results if claimed is not None]
    assert len(winners) == 1

    await session.refresh(job)
    assert job.status is JobStatus.PROCESSING
    assert job.attempt_count == 1
    assert job.worker_id == winners[0].worker_id


async def test_complete_job_stores_result_and_completion_time(session: AsyncSession) -> None:
    job = await _insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER)
    assert claimed is not None

    completed = await job_service.complete_job(session, Attempt.of(claimed), {"status": "sent"})

    assert completed.status is JobStatus.COMPLETED
    assert completed.result == {"status": "sent"}
    assert completed.progress == 100
    assert completed.completed_at is not None
    assert completed.error is None
    # A finished job is no longer owned or leased.
    assert completed.worker_id is None
    assert completed.execution_token is None
    assert completed.lease_expires_at is None


async def test_a_failed_attempt_with_attempts_left_is_rescheduled(
    session: AsyncSession,
) -> None:
    """A failed attempt with retries left becomes SCHEDULED, not FAILED."""
    job = await _insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER)
    assert claimed is not None

    result = await job_service.fail_attempt(
        session, Attempt.of(claimed), "delivery failed"
    )

    assert result.outcome is job_service.AttemptOutcome.RETRY_SCHEDULED
    assert result.job.status is JobStatus.SCHEDULED
    assert result.job.error == "delivery failed"
    assert result.job.attempt_count == 1
    assert result.job.completed_at is None


async def test_a_failed_final_attempt_stores_error_and_completion_time(
    session: AsyncSession,
) -> None:
    job = await _insert_job(session, attempt_count=2, max_attempts=3)
    claimed = await job_service.claim_job(session, job.id, WORKER)
    assert claimed is not None

    result = await job_service.fail_attempt(
        session, Attempt.of(claimed), "delivery failed"
    )

    assert result.outcome is job_service.AttemptOutcome.PERMANENTLY_FAILED
    assert result.job.status is JobStatus.FAILED
    assert result.job.error == "delivery failed"
    assert result.job.completed_at is not None
    assert result.job.attempt_count == 3


async def test_progress_updates_only_apply_while_processing(session: AsyncSession) -> None:
    job = await _insert_job(session)
    unowned = Attempt(
        job_id=job.id,
        worker_id=WORKER,
        execution_token=uuid.uuid4(),
        attempt_count=0,
        max_attempts=job.max_attempts,
    )

    with pytest.raises(OwnershipLostError):
        await job_service.update_progress(session, unowned, 50)
    await session.refresh(job)
    assert job.progress == 0

    claimed = await job_service.claim_job(session, job.id, WORKER)
    assert claimed is not None
    await job_service.update_progress(session, Attempt.of(claimed), 50)
    await session.refresh(job)
    assert job.progress == 50


async def test_started_at_marks_the_start_of_the_current_attempt(
    session: AsyncSession,
) -> None:
    """The claim restamps started_at; lease logic never reads an older attempt."""
    original = utcnow() - timedelta(hours=1)
    job = await _insert_job(session, started_at=original)

    claimed = await job_service.claim_job(session, job.id, WORKER)

    assert claimed is not None
    assert claimed.started_at is not None
    assert claimed.started_at > original
