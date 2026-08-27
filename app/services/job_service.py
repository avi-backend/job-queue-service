"""Job persistence logic: submission, idempotency, retrieval and listing."""

import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from sqlalchemy import Select, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.time import to_utc, utcnow
from app.db.models import Job, JobStatus
from app.schemas.jobs import JobCreateRequest, JobType
from app.services.queue_service import ReadyQueue

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


async def claim_job(session: AsyncSession, job_id: uuid.UUID, worker_id: str) -> Job | None:
    """Atomically take ownership of a PENDING job.

    This single conditional UPDATE is the concurrency boundary. Several workers
    can see the same Redis candidate, but only the transaction whose WHERE
    clause still matches PENDING gets a row back, and only that worker may
    execute the job. A returned row is the sole proof of ownership.

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
            started_at=func.coalesce(Job.started_at, func.now()),
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


async def update_progress(session: AsyncSession, job_id: uuid.UUID, progress: int) -> None:
    """Persist a progress percentage for a job that is being processed."""
    statement = (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.PROCESSING)
        .values(progress=max(0, min(100, progress)))
        .execution_options(synchronize_session=False)
    )
    await session.execute(statement)
    await session.commit()


async def complete_job(
    session: AsyncSession, job_id: uuid.UUID, result: dict[str, Any]
) -> Job | None:
    """Mark a processing job COMPLETED and store its result."""
    statement = (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.PROCESSING)
        .values(
            status=JobStatus.COMPLETED,
            result=result,
            progress=100,
            completed_at=func.now(),
            error=None,
            # No heartbeat exists yet, but a finished job must never look leased.
            lease_expires_at=None,
        )
        .returning(Job)
        .execution_options(synchronize_session=False, populate_existing=True)
    )
    job = (await session.execute(statement)).scalars().first()
    await session.commit()
    return job


async def fail_job(session: AsyncSession, job_id: uuid.UUID, error: str) -> Job | None:
    """Mark a processing job FAILED. Phase 3 does not retry."""
    statement = (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.PROCESSING)
        .values(
            status=JobStatus.FAILED,
            error=error,
            completed_at=func.now(),
            lease_expires_at=None,
        )
        .returning(Job)
        .execution_options(synchronize_session=False, populate_existing=True)
    )
    job = (await session.execute(statement)).scalars().first()
    await session.commit()
    return job


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
