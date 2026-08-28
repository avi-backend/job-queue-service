"""Execution-token fencing.

The scenario these tests exist for: a worker stalls, its lease expires, recovery
hands the job to someone else, and the original worker then wakes up still
holding its old token. Every write it makes must match zero rows.

Real PostgreSQL throughout, because the guarantee is the conditional UPDATE
itself, not any Python-side check.
"""

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import OwnershipLostError
from app.db.models import JobStatus
from app.services import job_service
from app.services.attempt import Attempt
from tests.factories import expire_lease, insert_job, make_due, reload

WORKER_A = "worker-a"
WORKER_B = "worker-b"


def _forged(attempt: Attempt, **changes) -> Attempt:
    """Same attempt with one field replaced, standing in for a stale worker."""
    fields = {
        "job_id": attempt.job_id,
        "worker_id": attempt.worker_id,
        "execution_token": attempt.execution_token,
        "attempt_count": attempt.attempt_count,
        "max_attempts": attempt.max_attempts,
    }
    return Attempt(**{**fields, **changes})


async def test_claim_assigns_an_execution_token(session: AsyncSession) -> None:
    job = await insert_job(session)

    claimed = await job_service.claim_job(session, job.id, WORKER_A)

    assert claimed is not None
    assert claimed.execution_token is not None
    assert isinstance(claimed.execution_token, uuid.UUID)


async def test_each_claim_mints_a_different_token(session: AsyncSession) -> None:
    """A second attempt of the same job is a different ownership generation."""
    job = await insert_job(session)

    first = await job_service.claim_job(session, job.id, WORKER_A)
    assert first is not None
    first_token = first.execution_token

    # Release the job the way a retry does, let the backoff elapse, claim again.
    await job_service.fail_attempt(session, Attempt.of(first), "boom")
    await make_due(session, job.id)
    await job_service.activate_due_scheduled_jobs(session, limit=10)

    second = await job_service.claim_job(session, job.id, WORKER_A)

    assert second is not None
    assert second.execution_token != first_token
    assert second.attempt_count == 2


async def test_claim_sets_a_lease(session: AsyncSession) -> None:
    job = await insert_job(session)

    claimed = await job_service.claim_job(session, job.id, WORKER_A, lease_seconds=30)

    assert claimed is not None
    assert claimed.lease_expires_at is not None
    assert claimed.started_at is not None
    # The lease is in the future relative to the row's own start time.
    assert claimed.lease_expires_at > claimed.started_at


async def test_started_at_records_the_current_attempt(session: AsyncSession) -> None:
    """Each claim restamps started_at, so it describes the attempt in flight."""
    job = await insert_job(session)
    first = await job_service.claim_job(session, job.id, WORKER_A)
    assert first is not None and first.started_at is not None
    first_started_at = first.started_at

    await job_service.fail_attempt(session, Attempt.of(first), "boom")
    await make_due(session, job.id)
    await job_service.activate_due_scheduled_jobs(session, limit=10)

    second = await job_service.claim_job(session, job.id, WORKER_A)

    assert second is not None and second.started_at is not None
    assert second.started_at > first_started_at


async def test_heartbeat_with_a_wrong_token_changes_nothing(session: AsyncSession) -> None:
    job = await insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed is not None
    attempt = Attempt.of(claimed)
    lease_before = claimed.lease_expires_at

    with pytest.raises(OwnershipLostError):
        await job_service.extend_lease(session, _forged(attempt, execution_token=uuid.uuid4()))

    stored = await reload(session, job.id)
    assert stored.lease_expires_at == lease_before
    assert stored.execution_token == attempt.execution_token


async def test_heartbeat_from_another_worker_changes_nothing(session: AsyncSession) -> None:
    """The token matches, but the process does not: still not the owner."""
    job = await insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed is not None
    attempt = Attempt.of(claimed)

    with pytest.raises(OwnershipLostError):
        await job_service.extend_lease(session, _forged(attempt, worker_id=WORKER_B))

    stored = await reload(session, job.id)
    assert stored.worker_id == WORKER_A


async def test_completion_with_a_wrong_token_changes_nothing(session: AsyncSession) -> None:
    job = await insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed is not None
    attempt = Attempt.of(claimed)

    with pytest.raises(OwnershipLostError):
        await job_service.complete_job(
            session, _forged(attempt, execution_token=uuid.uuid4()), {"status": "sent"}
        )

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.PROCESSING
    assert stored.result is None
    assert stored.completed_at is None


async def test_failure_with_a_wrong_token_changes_nothing(session: AsyncSession) -> None:
    job = await insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed is not None
    attempt = Attempt.of(claimed)

    with pytest.raises(OwnershipLostError):
        await job_service.fail_attempt(
            session, _forged(attempt, execution_token=uuid.uuid4()), "stale failure"
        )

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.PROCESSING
    assert stored.error is None
    assert stored.scheduled_at is None


async def test_progress_with_a_wrong_token_changes_nothing(session: AsyncSession) -> None:
    job = await insert_job(session, job_type="batch")
    claimed = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed is not None
    attempt = Attempt.of(claimed)
    await job_service.update_progress(session, attempt, 40)

    with pytest.raises(OwnershipLostError):
        await job_service.update_progress(
            session, _forged(attempt, execution_token=uuid.uuid4()), 90
        )

    stored = await reload(session, job.id)
    assert stored.progress == 40


async def test_fenced_writes_need_the_job_to_be_processing(session: AsyncSession) -> None:
    """A completed job cannot be written again, even with the right token."""
    job = await insert_job(session)
    claimed = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed is not None
    attempt = Attempt.of(claimed)
    await job_service.complete_job(session, attempt, {"status": "sent"})

    with pytest.raises(OwnershipLostError):
        await job_service.extend_lease(session, attempt)
    with pytest.raises(OwnershipLostError):
        await job_service.complete_job(session, attempt, {"status": "sent again"})
    with pytest.raises(OwnershipLostError):
        await job_service.fail_attempt(session, attempt, "too late")

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.COMPLETED
    assert stored.result == {"status": "sent"}
    assert stored.execution_token is None


async def test_stale_worker_cannot_complete_after_recovery_and_reclaim(
    session: AsyncSession,
) -> None:
    """The scenario the fencing token exists for, start to finish.

    Worker A claims, stalls past its lease, recovery releases the job, worker B
    claims it, and only then does A try to finish. A's write must be rejected
    and B's ownership must be untouched.
    """
    job = await insert_job(session)

    claimed_by_a = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed_by_a is not None
    attempt_a = Attempt.of(claimed_by_a)

    # A stalls: its lease lapses instead of being heartbeated.
    await expire_lease(session, job.id)
    recovered = await job_service.recover_expired_leases(session, limit=10)
    assert len(recovered) == 1
    assert recovered[0].job.status is JobStatus.SCHEDULED

    # The retry becomes due and a second worker takes it.
    await make_due(session, job.id)
    activated = await job_service.activate_due_scheduled_jobs(session, limit=10)
    assert len(activated) == 1
    claimed_by_b = await job_service.claim_job(session, job.id, WORKER_B)
    assert claimed_by_b is not None
    attempt_b = Attempt.of(claimed_by_b)
    assert attempt_b.execution_token != attempt_a.execution_token
    assert attempt_b.attempt_count == 2

    # A finally wakes up and tries to finish the attempt it lost.
    with pytest.raises(OwnershipLostError):
        await job_service.complete_job(session, attempt_a, {"status": "sent by a"})
    with pytest.raises(OwnershipLostError):
        await job_service.update_progress(session, attempt_a, 100)
    with pytest.raises(OwnershipLostError):
        await job_service.extend_lease(session, attempt_a)
    with pytest.raises(OwnershipLostError):
        await job_service.fail_attempt(session, attempt_a, "failed in a")

    stored = await reload(session, job.id)
    assert stored.status is JobStatus.PROCESSING
    assert stored.worker_id == WORKER_B
    assert stored.execution_token == attempt_b.execution_token
    assert stored.result is None
    assert stored.progress == 0

    # B can still finish normally.
    completed = await job_service.complete_job(session, attempt_b, {"status": "sent by b"})
    assert completed.result == {"status": "sent by b"}
    assert completed.status is JobStatus.COMPLETED


async def test_stale_worker_cannot_report_progress_after_recovery(
    session: AsyncSession,
) -> None:
    """Progress is fenced too: recovery alone is enough to reject it."""
    job = await insert_job(session, job_type="batch")
    claimed = await job_service.claim_job(session, job.id, WORKER_A)
    assert claimed is not None
    attempt = Attempt.of(claimed)
    await job_service.update_progress(session, attempt, 25)

    await expire_lease(session, job.id)
    await job_service.recover_expired_leases(session, limit=10)

    with pytest.raises(OwnershipLostError):
        await job_service.update_progress(session, attempt, 99)

    stored = await reload(session, job.id)
    assert stored.progress == 25
    assert stored.status is JobStatus.SCHEDULED
