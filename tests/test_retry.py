"""Retry policy and the failure flow.

The delays are asserted by comparing scheduled_at with the database clock, so a
30-second or 120-second backoff is verified without any test waiting for it.
"""

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobStatus
from app.services import job_service
from app.services.attempt import Attempt
from app.services.retry_policy import retry_delay_seconds
from tests.factories import insert_job, make_due, reload

WORKER = "worker-a"

#: Statement round-trips cost a few milliseconds, never a whole second.
TOLERANCE_SECONDS = 2.0


async def _seconds_until_scheduled(session: AsyncSession, job_id) -> float:
    """How far in the future scheduled_at sits, measured by the database clock."""
    job = await reload(session, job_id)
    assert job.scheduled_at is not None
    now = await session.scalar(select(func.now()))
    return (job.scheduled_at - now).total_seconds()


async def _claim_at_attempt(session: AsyncSession, attempt_number: int) -> Attempt:
    """Claim a job so that the live attempt is `attempt_number`."""
    job = await insert_job(session, attempt_count=attempt_number - 1)
    claimed = await job_service.claim_job(session, job.id, WORKER)
    assert claimed is not None and claimed.attempt_count == attempt_number
    return Attempt.of(claimed)


def test_retry_delays_are_thirty_then_one_hundred_and_twenty() -> None:
    assert retry_delay_seconds(1) == 30
    assert retry_delay_seconds(2) == 120


def test_no_delay_is_defined_for_a_fourth_attempt() -> None:
    """Three attempts is the whole policy; asking for a fourth is a bug."""
    with pytest.raises(ValueError):
        retry_delay_seconds(3)


async def test_failure_after_attempt_one_schedules_a_retry_in_thirty_seconds(
    session: AsyncSession,
) -> None:
    attempt = await _claim_at_attempt(session, 1)

    result = await job_service.fail_attempt(session, attempt, "smtp timeout")

    assert result.outcome is job_service.AttemptOutcome.RETRY_SCHEDULED
    delay = await _seconds_until_scheduled(session, attempt.job_id)
    assert 30 - TOLERANCE_SECONDS <= delay <= 30

    job = await reload(session, attempt.job_id)
    assert job.status is JobStatus.SCHEDULED
    assert job.error == "smtp timeout"
    assert job.attempt_count == 1
    assert job.completed_at is None
    # A retry must not look owned or leased while it waits.
    assert job.worker_id is None
    assert job.execution_token is None
    assert job.lease_expires_at is None


async def test_failure_after_attempt_two_schedules_a_retry_in_two_minutes(
    session: AsyncSession,
) -> None:
    attempt = await _claim_at_attempt(session, 2)

    result = await job_service.fail_attempt(session, attempt, "smtp timeout again")

    assert result.outcome is job_service.AttemptOutcome.RETRY_SCHEDULED
    delay = await _seconds_until_scheduled(session, attempt.job_id)
    assert 120 - TOLERANCE_SECONDS <= delay <= 120

    job = await reload(session, attempt.job_id)
    assert job.status is JobStatus.SCHEDULED
    assert job.attempt_count == 2


async def test_failure_after_the_third_attempt_fails_permanently(
    session: AsyncSession,
) -> None:
    attempt = await _claim_at_attempt(session, 3)

    result = await job_service.fail_attempt(session, attempt, "gave up")

    assert result.outcome is job_service.AttemptOutcome.PERMANENTLY_FAILED
    job = await reload(session, attempt.job_id)
    assert job.status is JobStatus.FAILED
    assert job.error == "gave up"
    assert job.completed_at is not None
    assert job.attempt_count == 3
    assert job.worker_id is None
    assert job.execution_token is None
    assert job.lease_expires_at is None


async def test_a_permanently_failed_job_is_never_activated_or_claimed(
    session: AsyncSession,
) -> None:
    """No fourth attempt exists: nothing can bring a FAILED job back."""
    attempt = await _claim_at_attempt(session, 3)
    await job_service.fail_attempt(session, attempt, "gave up")

    assert await job_service.activate_due_scheduled_jobs(session, limit=10) == []
    assert await job_service.claim_job(session, attempt.job_id, WORKER) is None
    assert (await reload(session, attempt.job_id)).status is JobStatus.FAILED


async def test_the_stored_error_describes_the_most_recent_attempt(
    session: AsyncSession,
) -> None:
    job = await insert_job(session)
    first = await job_service.claim_job(session, job.id, WORKER)
    assert first is not None
    await job_service.fail_attempt(session, Attempt.of(first), "first failure")
    assert (await reload(session, job.id)).error == "first failure"

    await make_due(session, job.id)
    await job_service.activate_due_scheduled_jobs(session, limit=10)
    second = await job_service.claim_job(session, job.id, WORKER)
    assert second is not None and second.attempt_count == 2

    await job_service.fail_attempt(session, Attempt.of(second), "second failure")

    stored = await reload(session, job.id)
    assert stored.error == "second failure"
    assert stored.status is JobStatus.SCHEDULED


async def test_a_successful_retry_clears_the_earlier_error(session: AsyncSession) -> None:
    job = await insert_job(session, attempt_count=1, error="first failure")
    claimed = await job_service.claim_job(session, job.id, WORKER)
    assert claimed is not None

    completed = await job_service.complete_job(session, Attempt.of(claimed), {"status": "sent"})

    assert completed.status is JobStatus.COMPLETED
    assert completed.error is None
    assert completed.attempt_count == 2
