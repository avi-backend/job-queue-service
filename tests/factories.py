"""Shared builders and database helpers for the tests.

Lease and backoff deadlines are moved by rewriting the timestamp, never by
waiting: `expire_lease` makes a lease look expired instantly, so a test can
exercise a 60-second lease or a 120-second backoff in microseconds. The
timestamps are written with the database clock, the same clock the recovery and
scheduler queries compare against.
"""

import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobStatus
from app.services import job_service

VALID_PAYLOADS: dict[str, dict[str, Any]] = {
    "email": {"to": "user@example.com", "subject": "Hello", "body": "optional body"},
    "webhook": {"url": "https://example.com/webhook", "event": "order.created"},
    "report": {"report_type": "sales", "format": "pdf"},
    "batch": {"items": [{"index": 1}, {"index": 2}]},
}


def job_request(job_type: str = "email", **overrides: Any) -> dict[str, Any]:
    """Build a valid POST /jobs body, overriding individual fields as needed."""
    body: dict[str, Any] = {"type": job_type, "payload": VALID_PAYLOADS[job_type]}
    body.update(overrides)
    return body


async def insert_job(
    session: AsyncSession,
    status: JobStatus = JobStatus.PENDING,
    job_type: str = "email",
    **overrides: Any,
) -> Job:
    """Insert a job directly, bypassing the API."""
    job = Job(
        type=job_type,
        payload=VALID_PAYLOADS[job_type],
        status=status,
        **overrides,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def expire_lease(
    session: AsyncSession, job_id: uuid.UUID, seconds_ago: float = 1.0
) -> None:
    """Backdate a lease so it is already expired, without waiting for it."""
    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(lease_expires_at=func.now() - timedelta(seconds=seconds_ago))
    )
    await session.commit()


async def expire_and_recover(session: AsyncSession, job_id: uuid.UUID) -> Sequence[Any]:
    """Expire a lease and recover it inside a single transaction.

    Needed whenever the owner's heartbeat is actually running: expiring the
    lease in one transaction and recovering in another leaves a gap in which a
    healthy beat re-extends the lease, and recovery then correctly finds nothing
    to do. Doing both under one transaction holds the row lock across the pair,
    so the beat either lands before the expiry (and is overwritten) or blocks
    until the job has already left PROCESSING.
    """
    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(lease_expires_at=func.now() - timedelta(seconds=1))
    )
    return await job_service.recover_expired_leases(session, limit=10)


async def make_due(session: AsyncSession, job_id: uuid.UUID, seconds_ago: float = 1.0) -> None:
    """Bring a scheduled_at into the past so the job is due to be activated."""
    await session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(scheduled_at=func.now() - timedelta(seconds=seconds_ago))
    )
    await session.commit()


async def reload(session: AsyncSession, job_id: uuid.UUID) -> Job:
    """Read a job's current committed state."""
    job = (await session.execute(select(Job).where(Job.id == job_id))).scalars().one()
    await session.refresh(job)
    return job
