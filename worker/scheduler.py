"""Scheduled-job activation.

One loop serves both kinds of delayed work, because they are the same thing in
the database: a user-submitted future job and a retry waiting out its backoff are
both SCHEDULED rows with a scheduled_at. When that time passes, the row becomes
PENDING and is published to the ready queue.

Ordering of the two steps is fixed and matters. PostgreSQL is committed first,
so a job is durably runnable before any worker can see it; Redis only tells
workers where to look.

Failure window
--------------
If the Redis enqueue fails after the commit, the job stays PENDING but invisible
to workers until something re-queues it. That window is real and is logged
loudly rather than papered over. It is not closed with a distributed
pseudo-transaction: rolling the row back to SCHEDULED after a Redis timeout
whose outcome is unknown can just as easily produce a double enqueue, and
neither store can promise the other's write happened. A reconciliation sweep
that re-queues PENDING jobs missing from the queue index is the honest fix, and
it belongs with the queue-statistics work of a later phase, where it can be
built to check the entry mapping instead of blindly re-enqueueing and destroying
FIFO order.
"""

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger
from app.services import job_service
from app.services.queue_service import ReadyQueue
from worker.loops import run_until_stopped

logger = get_logger("worker")


class JobScheduler:
    """Promotes due SCHEDULED jobs to PENDING and enqueues them."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        ready_queue: ReadyQueue,
        worker_id: str,
        interval: float,
        batch_size: int,
    ) -> None:
        self._session_factory = session_factory
        self._queue = ready_queue
        self._worker_id = worker_id
        self._interval = interval
        self._batch_size = batch_size

    async def run_forever(self, stop: asyncio.Event) -> None:
        logger.info("scheduler_started", extra={"worker_id": self._worker_id})
        await run_until_stopped(
            cycle=self.run_once,
            interval=self._interval,
            stop=stop,
            failure_event="scheduler_cycle_failed",
            context={"worker_id": self._worker_id},
        )
        logger.info("scheduler_stopped", extra={"worker_id": self._worker_id})

    async def run_once(self) -> int:
        """Activate one batch of due jobs. Returns how many this call activated.

        Every returned row was transitioned by this call alone, so enqueueing
        them cannot duplicate the work of a scheduler running in another worker.
        """
        async with self._session_factory() as session:
            activated = await job_service.activate_due_scheduled_jobs(session, self._batch_size)

        for job in activated:
            context = {
                "job_id": str(job.id),
                "job_type": job.type,
                "worker_id": self._worker_id,
                "attempt": job.attempt_count,
                "scheduled_at": job.scheduled_at.isoformat() if job.scheduled_at else None,
            }
            logger.info("scheduled_job_activated", extra=context)
            try:
                candidate = await self._queue.enqueue(job.id, job.priority)
            except Exception:
                logger.exception(
                    "scheduled_job_enqueue_failed",
                    extra={
                        **context,
                        "detail": "job is PENDING in postgres but not visible to workers",
                    },
                )
            else:
                logger.info(
                    "job_enqueued",
                    extra={**context, "priority": job.priority, "queue_entry": candidate.member},
                )

        return len(activated)
