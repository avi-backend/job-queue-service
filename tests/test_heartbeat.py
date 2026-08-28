"""Lease heartbeat.

The heartbeat interval here is milliseconds, never the production 20 seconds:
the property under test is "a beat extends the lease and a lost attempt stops
the loop", which is independent of the interval's size.
"""

import asyncio
import uuid
from collections.abc import Callable

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import JobStatus
from app.services import job_service
from app.services.attempt import Attempt
from tests.factories import expire_and_recover, insert_job, job_request, reload
from worker.heartbeat import LeaseHeartbeat
from worker.runner import JobRunner

WORKER = "worker-a"
FAST_INTERVAL = 0.02


async def _claim(session: AsyncSession, lease_seconds: float = 30.0, **overrides) -> Attempt:
    job = await insert_job(session, **overrides)
    claimed = await job_service.claim_job(session, job.id, WORKER, lease_seconds=lease_seconds)
    assert claimed is not None
    return Attempt.of(claimed)


async def test_a_beat_extends_the_lease(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    attempt = await _claim(session, lease_seconds=5)
    before = (await reload(session, attempt.job_id)).lease_expires_at
    assert before is not None

    heartbeat = LeaseHeartbeat(
        session_factory=session_factory,
        attempt=attempt,
        interval=FAST_INTERVAL,
        lease_seconds=60,
    )
    assert await heartbeat.beat() is True

    after = (await reload(session, attempt.job_id)).lease_expires_at
    assert after is not None and after > before


async def test_a_beat_for_a_lost_attempt_stops_the_heartbeat(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    attempt = await _claim(session)
    stale = Attempt(
        job_id=attempt.job_id,
        worker_id=attempt.worker_id,
        execution_token=uuid.uuid4(),
        attempt_count=attempt.attempt_count,
        max_attempts=attempt.max_attempts,
    )

    heartbeat = LeaseHeartbeat(
        session_factory=session_factory, attempt=stale, interval=FAST_INTERVAL
    )

    assert await heartbeat.beat() is False
    assert heartbeat.ownership_lost.is_set()


async def test_the_running_loop_keeps_extending_the_lease(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    attempt = await _claim(session, lease_seconds=5)
    before = (await reload(session, attempt.job_id)).lease_expires_at
    assert before is not None

    async with LeaseHeartbeat(
        session_factory=session_factory,
        attempt=attempt,
        interval=FAST_INTERVAL,
        lease_seconds=60,
    ):
        for _ in range(200):
            current = (await reload(session, attempt.job_id)).lease_expires_at
            if current is not None and current > before:
                break
            await asyncio.sleep(0.01)

    after = (await reload(session, attempt.job_id)).lease_expires_at
    assert after is not None and after > before


async def test_the_loop_notices_ownership_loss_on_its_own(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Recovery takes the job away mid-flight; the heartbeat reports it."""
    attempt = await _claim(session)

    async with LeaseHeartbeat(
        session_factory=session_factory, attempt=attempt, interval=FAST_INTERVAL
    ) as heartbeat:
        recovered = await expire_and_recover(session, attempt.job_id)
        assert len(recovered) == 1
        await asyncio.wait_for(heartbeat.ownership_lost.wait(), timeout=5)

    assert heartbeat.ownership_lost.is_set()


async def test_leaving_the_context_leaves_no_background_task(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """A leaked heartbeat would keep a dead attempt's lease alive."""
    attempt = await _claim(session)
    before = len(asyncio.all_tasks())

    async with LeaseHeartbeat(
        session_factory=session_factory, attempt=attempt, interval=FAST_INTERVAL
    ):
        assert len(asyncio.all_tasks()) == before + 1

    assert len(asyncio.all_tasks()) == before


async def test_a_transient_heartbeat_error_does_not_end_the_attempt(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession], monkeypatch
) -> None:
    """A database blip is not ownership loss: keep beating, keep executing."""
    attempt = await _claim(session)

    async def broken_extend(*args, **kwargs):
        raise ConnectionError("database went away")

    monkeypatch.setattr(job_service, "extend_lease", broken_extend)
    heartbeat = LeaseHeartbeat(
        session_factory=session_factory, attempt=attempt, interval=FAST_INTERVAL
    )

    assert await heartbeat.beat() is True
    assert not heartbeat.ownership_lost.is_set()


async def test_the_worker_abandons_a_handler_whose_attempt_was_recovered(
    client: AsyncClient,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
    make_runner: Callable[..., JobRunner],
    monkeypatch,
) -> None:
    """The stall scenario, driven through the real worker.

    The handler hangs, recovery takes the job away, and the worker must stop
    executing and write nothing. Recovery's state has to survive untouched.
    """
    response = await client.post("/jobs", json=job_request())
    job_id = uuid.UUID(response.json()["id"])
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def hanging_handler(context):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return {"status": "sent late"}

    monkeypatch.setattr("worker.runner.get_handler", lambda job_type: hanging_handler)
    runner = make_runner(heartbeat_interval=FAST_INTERVAL)
    execution = asyncio.create_task(runner.run_once())

    await asyncio.wait_for(started.wait(), timeout=5)
    async with session_factory() as recovery_session:
        recovered = await expire_and_recover(recovery_session, job_id)
    assert len(recovered) == 1

    assert await asyncio.wait_for(execution, timeout=10) is True
    assert cancelled.is_set()

    stored = await reload(session, job_id)
    assert stored.status is JobStatus.SCHEDULED
    assert stored.error == job_service.LEASE_EXPIRED_ERROR
    assert stored.worker_id is None
    assert stored.execution_token is None
    assert stored.result is None
