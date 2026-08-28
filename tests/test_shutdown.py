"""Graceful worker shutdown.

SIGTERM must drain the owned attempt (heartbeat included) and must not claim
another job. Tests use a short real sleep so drain is observable without
waiting on production handler durations.
"""

import asyncio
import uuid
from collections.abc import Callable

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JobStatus
from app.services import job_service
from app.services.queue_service import ReadyQueue
from tests.factories import expire_lease, reload
from tests.test_worker import _load, _submit
from worker.recovery import LeaseRecovery
from worker.runner import JobRunner


async def test_graceful_shutdown_while_idle_exits_cleanly(
    make_runner: Callable[..., JobRunner],
) -> None:
    stop = asyncio.Event()
    stop.set()
    await asyncio.wait_for(make_runner().run_forever(stop), timeout=5)


async def test_graceful_shutdown_finishes_the_active_job(
    client: AsyncClient,
    session: AsyncSession,
    make_runner: Callable[..., JobRunner],
    monkeypatch,
) -> None:
    job_id = await _submit(client)
    started = asyncio.Event()

    async def slow_handler(context):
        started.set()
        await asyncio.sleep(0.15)
        return {"status": "sent after drain"}

    monkeypatch.setattr("worker.runner.get_handler", lambda job_type: slow_handler)
    stop = asyncio.Event()
    runner = make_runner()
    loop_task = asyncio.create_task(runner.run_forever(stop))

    await asyncio.wait_for(started.wait(), timeout=5)
    stop.set()
    await asyncio.wait_for(loop_task, timeout=5)

    job = await _load(session, job_id)
    assert job.status is JobStatus.COMPLETED
    assert job.result == {"status": "sent after drain"}


async def test_heartbeat_stays_alive_during_graceful_drain(
    client: AsyncClient,
    session: AsyncSession,
    make_runner: Callable[..., JobRunner],
    monkeypatch,
) -> None:
    job_id = await _submit(client)
    started = asyncio.Event()

    async def slow_handler(context):
        started.set()
        await asyncio.sleep(0.25)
        return {"status": "sent"}

    monkeypatch.setattr("worker.runner.get_handler", lambda job_type: slow_handler)
    stop = asyncio.Event()
    runner = make_runner(heartbeat_interval=0.05, lease_seconds=5)
    loop_task = asyncio.create_task(runner.run_forever(stop))

    await asyncio.wait_for(started.wait(), timeout=5)
    lease_at_stop = (await _load(session, job_id)).lease_expires_at
    assert lease_at_stop is not None
    stop.set()

    for _ in range(50):
        current = (await _load(session, job_id)).lease_expires_at
        if current is not None and current > lease_at_stop:
            break
        await asyncio.sleep(0.02)
    else:
        raise AssertionError("heartbeat did not extend the lease after shutdown was requested")

    await asyncio.wait_for(loop_task, timeout=5)
    assert (await _load(session, job_id)).status is JobStatus.COMPLETED


async def test_worker_claims_no_second_job_after_shutdown(
    client: AsyncClient,
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_runner: Callable[..., JobRunner],
    monkeypatch,
) -> None:
    first = await _submit(client, priority=10)
    second = await _submit(client, priority=1)
    started = asyncio.Event()

    async def slow_handler(context):
        started.set()
        await asyncio.sleep(0.15)
        return {"status": "sent"}

    monkeypatch.setattr("worker.runner.get_handler", lambda job_type: slow_handler)
    stop = asyncio.Event()
    loop_task = asyncio.create_task(make_runner().run_forever(stop))

    await asyncio.wait_for(started.wait(), timeout=5)
    stop.set()
    await asyncio.wait_for(loop_task, timeout=5)

    assert (await _load(session, first)).status is JobStatus.COMPLETED
    assert (await _load(session, second)).status is JobStatus.PENDING
    assert await ready_queue.size() == 1


async def test_shutdown_after_peek_does_not_claim(
    client: AsyncClient,
    session: AsyncSession,
    ready_queue: ReadyQueue,
    make_runner: Callable[..., JobRunner],
) -> None:
    job_id = await _submit(client)
    stop = asyncio.Event()
    stop.set()

    assert await make_runner().run_once(stop) is False
    assert (await _load(session, job_id)).status is JobStatus.PENDING
    assert await ready_queue.size() == 1


async def test_crash_recovery_still_works_after_hard_kill(
    client: AsyncClient,
    session: AsyncSession,
    session_factory,
    make_recovery: Callable[..., LeaseRecovery],
) -> None:
    """SIGKILL is unchanged: an expired lease is recovered as a failed attempt."""
    job_uuid = uuid.UUID(await _submit(client))
    async with session_factory() as claim_session:
        claimed = await job_service.claim_job(claim_session, job_uuid, "worker-killed")
        assert claimed is not None
        await expire_lease(claim_session, job_uuid)

    assert await make_recovery().run_once() == 1
    stored = await reload(session, job_uuid)
    assert stored.status is JobStatus.SCHEDULED
    assert stored.error == job_service.LEASE_EXPIRED_ERROR
    assert stored.attempt_count == 1
