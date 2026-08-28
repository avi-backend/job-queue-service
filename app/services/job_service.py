"""Job persistence logic.

Submission, idempotency, retrieval and listing, plus every state transition that
decides who owns a job: the atomic claim, the fenced writes an owner performs,
scheduled-job activation and crash recovery.

All of these are conditional UPDATE statements. The WHERE clause, not
application logic, is what makes them safe under concurrency, so each one states
the full set of conditions it needs even where a caller has already checked them.
"""

import enum
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Select, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.errors import OwnershipLostError
from app.core.logging import get_logger
from app.core.time import to_utc, utcnow
from app.db.models import Job, JobStatus
from app.schemas.jobs import JobCreateRequest, JobType
from app.services.attempt import Attempt
from app.services.queue_service import ReadyQueue
from app.services.retry_policy import retry_delay_seconds

logger = get_logger(__name__)

#: Submitted idempotency keys stay valid for this long (assignment requires >= 24h).
IDEMPOTENCY_TTL = timedelta(hours=24)

#: max_attempts is server-controlled for this assignment.
DEFAULT_MAX_ATTEMPTS = 3

#: Partial unique index from the initial migration; the final concurrency guard.
IDEMPOTENCY_INDEX_NAME = "uq_jobs_idempotency_key_active"


def _is_idempotency_conflict(error: IntegrityError) -> bool:
    """True only when the unique idempotency index rejected the insert."""
    candidates = (error.orig, getattr(error.orig, "__cause__", None))
    for candidate in candidates:
        constraint = getattr(candidate, "constraint_name", None)
        if constraint:
            return constraint == IDEMPOTENCY_INDEX_NAME
    return IDEMPOTENCY_INDEX_NAME in str(error.orig)


async def _find_active_by_key(session: AsyncSession, key: str) -> Job | None:
    """Find the job currently owning an idempotency key, if it has not expired."""
    now = utcnow()
    statement = select(Job).where(
        Job.idempotency_key == key,
        (Job.idempotency_expires_at.is_(None)) | (Job.idempotency_expires_at > now),
    )
    return (await session.execute(statement)).scalars().first()


async def _release_expired_key(session: AsyncSession, key: str) -> None:
    """Detach an expired key from its old job so the key can be reused.

    The WHERE clause re-checks expiry, so this can never steal a live key even
    if two requests run it concurrently.
    """
    now = utcnow()
    statement = (
        update(Job)
        .where(
            Job.idempotency_key == key,
            Job.idempotency_expires_at.is_not(None),
            Job.idempotency_expires_at <= now,
        )
        .values(idempotency_key=None, idempotency_expires_at=None)
    )
    result = await session.execute(statement)
    if result.rowcount:
        logger.info("released expired idempotency key", extra={"released_rows": result.rowcount})


def _build_job(request: JobCreateRequest, idempotency_key: str | None) -> Job:
    now = utcnow()
    scheduled_at = to_utc(request.scheduled_at) if request.scheduled_at else None
    is_future = scheduled_at is not None and scheduled_at > now

    return Job(
        type=request.type.value,
        payload=request.payload,
        status=JobStatus.SCHEDULED if is_future else JobStatus.PENDING,
        priority=request.priority,
        max_attempts=DEFAULT_MAX_ATTEMPTS,
        scheduled_at=scheduled_at,
        idempotency_key=idempotency_key,
        idempotency_expires_at=now + IDEMPOTENCY_TTL if idempotency_key else None,
    )


async def create_job(
    session: AsyncSession,
    request: JobCreateRequest,
    idempotency_key: str | None = None,
) -> tuple[Job, bool]:
    """Create a job, or return the existing one for a live idempotency key.

    Returns the job and whether it was newly created. Correctness under
    concurrency comes from the unique partial index rather than from the
    read-then-insert check: the check is only a fast path, and a lost race is
    resolved by reloading the winning row.
    """
    if idempotency_key:
        existing = await _find_active_by_key(session, idempotency_key)
        if existing is not None:
            return existing, False
        await _release_expired_key(session, idempotency_key)

    job = _build_job(request, idempotency_key)
    session.add(job)
    try:
        await session.flush()
        # Load server-generated values (created_at) while still in the session's
        # greenlet context, so the response never triggers a lazy load.
        await session.refresh(job)
        await session.commit()
    except IntegrityError as error:
        # Leaving the session in a failed transaction would break every later
        # statement, so roll back before doing anything else.
        await session.rollback()
        if not (idempotency_key and _is_idempotency_conflict(error)):
            raise
        winner = await _find_active_by_key(session, idempotency_key)
        if winner is None:
            # The key was released between the conflict and this lookup.
            raise
        logger.info("idempotency race lost; returning existing job", extra={"job_id": str(winner.id)})
        return winner, False

    return job, True


async def submit_job(
    session: AsyncSession,
    ready_queue: ReadyQueue,
    request: JobCreateRequest,
    idempotency_key: str | None = None,
) -> tuple[Job, bool]:
    """Persist a job, then publish it to the ready queue when it is runnable.

    PostgreSQL is committed first so the job is durable before any worker can
    see it. Only newly created PENDING jobs are enqueued: SCHEDULED jobs wait
    for the activation phase, and an idempotency replay must not enqueue a job
    that is already queued or already finished.

    If the enqueue fails after the commit, the job stays durably PENDING but
    invisible to workers until a reconciliation sweep (a later phase) re-queues
    it. That window is logged loudly rather than hidden, and the response still
    reports the truth: the job was persisted.
    """
    job, created = await create_job(session, request, idempotency_key)

    if created and job.status is JobStatus.PENDING:
        try:
            candidate = await ready_queue.enqueue(job.id, job.priority)
        except Exception:
            logger.exception(
                "job_enqueue_failed",
                extra={
                    "job_id": str(job.id),
                    "job_type": job.type,
                    "detail": "job is committed in postgres but not visible to workers",
                },
            )
        else:
            logger.info(
                "job_enqueued",
                extra={
                    "job_id": str(job.id),
                    "job_type": job.type,
                    "priority": job.priority,
                    "queue_entry": candidate.member,
                },
            )

    return job, created


def _lease_deadline(lease_seconds: float | None) -> Any:
    """Lease expiry expressed against the database clock.

    Deliberately not computed from the worker's clock: leases are compared with
    now() by recovery running in another process, so a skewed worker clock must
    not be able to grant itself a longer or shorter lease than it appears to
    have.
    """
    seconds = settings.job_lease_seconds if lease_seconds is None else lease_seconds
    return func.now() + timedelta(seconds=seconds)


#: Fields that describe an in-flight attempt. Cleared whenever a job stops being
#: PROCESSING, so a released job never looks leased or owned.
_RELEASED_OWNERSHIP: dict[str, None] = {
    "worker_id": None,
    "execution_token": None,
    "lease_expires_at": None,
}


class AttemptOutcome(str, enum.Enum):
    """What happened to a processing attempt that did not succeed."""

    RETRY_SCHEDULED = "retry_scheduled"
    PERMANENTLY_FAILED = "permanently_failed"


@dataclass(frozen=True, slots=True)
class AttemptResult:
    outcome: AttemptOutcome
    job: Job


async def claim_job(
    session: AsyncSession,
    job_id: uuid.UUID,
    worker_id: str,
    lease_seconds: float | None = None,
) -> Job | None:
    """Atomically take ownership of a PENDING job for one attempt.

    This single conditional UPDATE is the concurrency boundary. Several workers
    can see the same Redis candidate, but only the transaction whose WHERE
    clause still matches PENDING gets a row back, and only that worker may
    execute the job. A returned row is the sole proof of ownership.

    Every claim mints a fresh execution_token, so an attempt is identifiable
    even when the same worker claims the same job again after a recovery. The
    token is never reused across claims or retries.

    started_at records the start of the current attempt rather than the first
    one ever made. Lease and heartbeat logic reads live ownership fields, so it
    does not depend on started_at, and a retry's runtime is more useful to an
    operator than the timestamp of an attempt that already failed.

    Returns the claimed job, or None when someone else already owns it, the job
    is no longer pending, or it does not exist.
    """
    statement = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == JobStatus.PENDING,
            # Never let the claim push attempt_count past the check constraint.
            Job.attempt_count < Job.max_attempts,
        )
        .values(
            status=JobStatus.PROCESSING,
            worker_id=worker_id,
            execution_token=uuid.uuid4(),
            lease_expires_at=_lease_deadline(lease_seconds),
            started_at=func.now(),
            attempt_count=Job.attempt_count + 1,
        )
        .returning(Job)
        # populate_existing refreshes an instance already in the session's
        # identity map; without it RETURNING hands back stale attributes.
        .execution_options(synchronize_session=False, populate_existing=True)
    )
    job = (await session.execute(statement)).scalars().first()
    await session.commit()
    return job


def _owned_by(attempt: Attempt) -> tuple[Any, ...]:
    """The fence. Only the live owner of this exact attempt matches."""
    return (
        Job.id == attempt.job_id,
        Job.status == JobStatus.PROCESSING,
        Job.worker_id == attempt.worker_id,
        Job.execution_token == attempt.execution_token,
    )


async def extend_lease(
    session: AsyncSession, attempt: Attempt, lease_seconds: float | None = None
) -> datetime:
    """Push the lease forward. Raises OwnershipLostError if the attempt is gone.

    This is the heartbeat's only write. Because it is fenced like every other
    ownership operation, a worker that was already recovered cannot resurrect
    its lease and start competing with the job's new owner.
    """
    statement = (
        update(Job)
        .where(*_owned_by(attempt))
        .values(lease_expires_at=_lease_deadline(lease_seconds))
        .returning(Job.lease_expires_at)
        .execution_options(synchronize_session=False)
    )
    extended = (await session.execute(statement)).scalars().first()
    await session.commit()
    if extended is None:
        raise OwnershipLostError(attempt.job_id, attempt.worker_id, attempt.execution_token)
    return extended


async def update_progress(session: AsyncSession, attempt: Attempt, progress: int) -> None:
    """Persist a progress percentage for the attempt that owns the job."""
    statement = (
        update(Job)
        .where(*_owned_by(attempt))
        .values(progress=max(0, min(100, progress)))
        .execution_options(synchronize_session=False)
    )
    result = await session.execute(statement)
    await session.commit()
    if not result.rowcount:
        raise OwnershipLostError(attempt.job_id, attempt.worker_id, attempt.execution_token)


async def complete_job(session: AsyncSession, attempt: Attempt, result: dict[str, Any]) -> Job:
    """Mark the owned attempt COMPLETED and store its result."""
    statement = (
        update(Job)
        .where(*_owned_by(attempt))
        .values(
            status=JobStatus.COMPLETED,
            result=result,
            progress=100,
            completed_at=func.now(),
            error=None,
            **_RELEASED_OWNERSHIP,
        )
        .returning(Job)
        .execution_options(synchronize_session=False, populate_existing=True)
    )
    job = (await session.execute(statement)).scalars().first()
    await session.commit()
    if job is None:
        raise OwnershipLostError(attempt.job_id, attempt.worker_id, attempt.execution_token)
    return job


async def _apply_failed_attempt(
    session: AsyncSession,
    attempt: Attempt,
    error: str,
    require_expired_lease: bool = False,
) -> AttemptResult | None:
    """Record a failed attempt as a delayed retry or a permanent failure.

    Does not commit, so recovery can settle a whole batch in one transaction and
    keep its row locks until the end.

    The branch is chosen from the attempt's own counters, but the SQL repeats the
    condition so the write is still correct on its own terms. Returns None when
    the fence no longer matches, which the callers translate into either an
    ownership-lost error or a skipped recovery.
    """
    if attempt.has_attempts_left:
        delay = retry_delay_seconds(attempt.attempt_count)
        statement = (
            update(Job)
            .where(*_owned_by(attempt), Job.attempt_count < Job.max_attempts)
            .values(
                status=JobStatus.SCHEDULED,
                scheduled_at=func.now() + timedelta(seconds=delay),
                error=error,
                **_RELEASED_OWNERSHIP,
            )
        )
        outcome = AttemptOutcome.RETRY_SCHEDULED
    else:
        statement = (
            update(Job)
            .where(*_owned_by(attempt), Job.attempt_count >= Job.max_attempts)
            .values(
                status=JobStatus.FAILED,
                error=error,
                completed_at=func.now(),
                **_RELEASED_OWNERSHIP,
            )
        )
        outcome = AttemptOutcome.PERMANENTLY_FAILED

    if require_expired_lease:
        # Recovery only: prove at write time that the lease is still expired,
        # so a worker that heartbeated between the read and the write keeps its
        # job.
        statement = statement.where(Job.lease_expires_at < func.now())

    statement = statement.returning(Job).execution_options(
        synchronize_session=False, populate_existing=True
    )
    job = (await session.execute(statement)).scalars().first()
    return None if job is None else AttemptResult(outcome=outcome, job=job)


async def fail_attempt(session: AsyncSession, attempt: Attempt, error: str) -> AttemptResult:
    """Handle a handler failure: schedule a retry, or fail the job for good.

    The retry is left SCHEDULED rather than enqueued; the scheduler loop makes
    it visible to workers once the backoff has elapsed. That keeps one code path
    for user-scheduled jobs and retries, and keeps PostgreSQL the only thing
    that decides when a job is runnable.
    """
    result = await _apply_failed_attempt(session, attempt, error)
    await session.commit()
    if result is None:
        raise OwnershipLostError(attempt.job_id, attempt.worker_id, attempt.execution_token)

    if result.outcome is AttemptOutcome.RETRY_SCHEDULED:
        logger.warning(
            "job_retry_scheduled",
            extra={
                **attempt.log_context,
                "scheduled_at": result.job.scheduled_at.isoformat(),
                "error": error,
            },
        )
    else:
        logger.warning(
            "job_retry_exhausted",
            extra={**attempt.log_context, "max_attempts": attempt.max_attempts, "error": error},
        )
    return result


async def activate_due_scheduled_jobs(session: AsyncSession, limit: int) -> Sequence[Job]:
    """Move due SCHEDULED jobs to PENDING and return the rows this call moved.

    Concurrency safety comes from two places. FOR UPDATE SKIP LOCKED means
    parallel schedulers pick disjoint rows instead of blocking on each other,
    and the UPDATE repeats the status and due-time conditions so a row can only
    be activated once even if it were somehow selected twice.

    Returning only the rows this call transitioned is what makes the caller's
    enqueue safe: nobody else will enqueue them.

    The due set is a CTE rather than an inline subquery on purpose. PostgreSQL
    cannot hash a subquery that carries FOR UPDATE, so an inline version is
    re-planned per candidate row: each activation changes which rows are still
    due, the next evaluation returns a different pair, and the batch limit stops
    holding. As a CTE the locking select runs exactly once.
    """
    due = (
        select(Job.id)
        .where(Job.status == JobStatus.SCHEDULED, Job.scheduled_at <= func.now())
        .order_by(Job.scheduled_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
        .cte("due_jobs")
    )
    statement = (
        update(Job)
        .where(
            Job.id.in_(select(due.c.id)),
            Job.status == JobStatus.SCHEDULED,
            Job.scheduled_at <= func.now(),
        )
        .values(status=JobStatus.PENDING)
        .returning(Job)
        .execution_options(synchronize_session=False, populate_existing=True)
    )
    activated = (await session.execute(statement)).scalars().all()
    await session.commit()
    return activated


#: Error recorded on a job whose owner stopped heartbeating.
LEASE_EXPIRED_ERROR = "worker lease expired"


async def recover_expired_leases(session: AsyncSession, limit: int) -> Sequence[AttemptResult]:
    """Take jobs away from owners whose lease ran out.

    An expired lease is treated as a failed attempt, because attempt_count was
    already incremented at claim time and the work may well have happened. So
    the same retry policy applies: back off and retry while attempts remain,
    otherwise fail permanently.

    Rows are locked with SKIP LOCKED and each write re-proves both the fence and
    the expiry, so several recovery loops never recover the same ownership twice
    and a worker that beats its heartbeat in during the sweep keeps its job.
    """
    expired = (
        (
            await session.execute(
                select(Job)
                .where(
                    Job.status == JobStatus.PROCESSING,
                    Job.lease_expires_at < func.now(),
                )
                .order_by(Job.lease_expires_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )

    recovered: list[AttemptResult] = []
    for job in expired:
        attempt = Attempt.of(job)
        result = await _apply_failed_attempt(
            session, attempt, LEASE_EXPIRED_ERROR, require_expired_lease=True
        )
        if result is None:
            logger.info("job_recovery_skipped", extra=attempt.log_context)
            continue
        recovered.append(result)
        logger.warning(
            "job_recovered",
            extra={
                **attempt.log_context,
                "outcome": result.outcome.value,
                "scheduled_at": (
                    result.job.scheduled_at.isoformat() if result.job.scheduled_at else None
                ),
            },
        )

    # One commit for the batch: the row locks taken above are held until here.
    await session.commit()
    return recovered


async def get_job(session: AsyncSession, job_id: uuid.UUID) -> Job | None:
    return await session.get(Job, job_id)


def _apply_filters(
    statement: Select, status: JobStatus | None, job_type: JobType | None
) -> Select:
    if status is not None:
        statement = statement.where(Job.status == status)
    if job_type is not None:
        statement = statement.where(Job.type == job_type.value)
    return statement


async def list_jobs(
    session: AsyncSession,
    *,
    status: JobStatus | None = None,
    job_type: JobType | None = None,
    limit: int,
    offset: int,
) -> tuple[Sequence[Job], int]:
    """Return a page of jobs, newest first, plus the total matching count."""
    total = await session.scalar(
        _apply_filters(select(func.count()).select_from(Job), status, job_type)
    )
    page = _apply_filters(select(Job), status, job_type)
    # id breaks ties so pagination stays stable when created_at collides.
    page = page.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit).offset(offset)
    rows = (await session.execute(page)).scalars().all()
    return rows, total or 0
