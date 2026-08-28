"""Lease heartbeat.

A claim owns a job only until its lease expires; the heartbeat is what keeps a
healthy worker's ownership alive. It runs as a background task next to the
handler and pushes lease_expires_at forward on an interval shorter than the
lease, so a worker that is merely slow is never mistaken for a crashed one.

The heartbeat write is fenced exactly like every other ownership operation. If it
matches zero rows the attempt has already been taken away, and the only correct
behaviour is to stop: the local handler is cancelled and nothing further is
written for that attempt. Cancelling the coroutine stops local execution only.
Side effects it already sent to the outside world cannot be recalled, which is
why this service promises at-least-once execution and not exactly-once.
"""

import asyncio
from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.errors import OwnershipLostError
from app.core.logging import get_logger
from app.services import job_service
from app.services.attempt import Attempt
from worker.loops import sleep_until_stopped

logger = get_logger("worker")

#: How long shutdown waits for an in-flight heartbeat write to finish.
STOP_TIMEOUT_SECONDS = 5.0


class LeaseHeartbeat:
    """Extends one attempt's lease until the attempt ends or is lost.

    Used as an async context manager so the background task cannot outlive the
    handler it belongs to, whatever way the handler exits.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        attempt: Attempt,
        interval: float,
        lease_seconds: float | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._attempt = attempt
        self._interval = interval
        self._lease_seconds = lease_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        #: Set once a beat proves the attempt is no longer owned.
        self.ownership_lost = asyncio.Event()

    async def __aenter__(self) -> "LeaseHeartbeat":
        self._task = asyncio.create_task(self._run(), name=f"heartbeat-{self._attempt.job_id}")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stop.set()
        task, self._task = self._task, None
        if task is None:
            return
        try:
            # Awaited rather than only cancelled, so an in-flight heartbeat
            # write can never land after the caller settles the job. The bound
            # keeps a wedged connection from blocking shutdown; wait_for
            # cancels the task itself when it fires.
            await asyncio.wait_for(task, timeout=STOP_TIMEOUT_SECONDS)
        except (TimeoutError, asyncio.CancelledError):
            pass

    async def _run(self) -> None:
        while not self._stop.is_set():
            await sleep_until_stopped(self._stop, self._interval)
            if self._stop.is_set():
                return
            if not await self.beat():
                return

    async def beat(self) -> bool:
        """Send one heartbeat. False once ownership is lost for good.

        A transient database error is not ownership loss, so it is logged and
        the next beat tries again. If the failures continue the lease simply
        lapses and recovery takes over, which is the behaviour we want.
        """
        try:
            async with self._session_factory() as session:
                lease_expires_at = await job_service.extend_lease(
                    session, self._attempt, self._lease_seconds
                )
        except OwnershipLostError:
            self.ownership_lost.set()
            logger.warning(
                "job_heartbeat_ownership_lost",
                extra={
                    **self._attempt.log_context,
                    "detail": "attempt was recovered or reclaimed; abandoning local execution",
                },
            )
            return False
        except Exception:
            logger.exception("job_heartbeat_failed", extra=self._attempt.log_context)
            return True

        # Logged at INFO: one line per lease extension is low volume at a
        # 20-second interval, and it is the only external evidence that a
        # long-running job is still owned by a healthy worker.
        logger.info(
            "job_heartbeat",
            extra={
                **self._attempt.log_context,
                "lease_expires_at": lease_expires_at.isoformat(),
            },
        )
        return True
