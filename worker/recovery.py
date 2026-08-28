"""Crash recovery.

A worker that dies, hangs, or loses its database connection stops heartbeating
but leaves the row PROCESSING. This loop is what stops that job from being stuck
forever: once the lease has expired, the attempt is taken away from its owner and
treated as failed, so the retry policy can give it another run.

Nothing here trusts the read. The rows are selected with FOR UPDATE SKIP LOCKED
and each write re-proves both the fence and the expiry, so recovery loops in
three workers pick disjoint rows and a worker whose heartbeat arrives during the
sweep keeps its job.

Recovery does not enqueue anything. A recovered job becomes SCHEDULED with its
backoff, and the scheduler loop publishes it when it is due, which keeps a single
path from "runnable" to "queued".
"""

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger
from app.services import job_service
from worker.loops import run_until_stopped

logger = get_logger("worker")


class LeaseRecovery:
    """Reclaims jobs whose owner stopped heartbeating."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        worker_id: str,
        interval: float,
        batch_size: int,
    ) -> None:
        self._session_factory = session_factory
        self._worker_id = worker_id
        self._interval = interval
        self._batch_size = batch_size

    async def run_forever(self, stop: asyncio.Event) -> None:
        logger.info("recovery_started", extra={"worker_id": self._worker_id})
        await run_until_stopped(
            cycle=self.run_once,
            interval=self._interval,
            stop=stop,
            failure_event="job_recovery_failed",
            context={"worker_id": self._worker_id},
        )
        logger.info("recovery_stopped", extra={"worker_id": self._worker_id})

    async def run_once(self) -> int:
        """Recover one batch of expired leases. Returns how many were recovered."""
        async with self._session_factory() as session:
            recovered = await job_service.recover_expired_leases(session, self._batch_size)

        if recovered:
            logger.info(
                "lease_recovery_sweep",
                extra={"worker_id": self._worker_id, "recovered": len(recovered)},
            )
        return len(recovered)
