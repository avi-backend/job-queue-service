"""Worker execution loop.

Pickup is deliberately peek-then-claim rather than pop-then-claim. Reading the
candidate non-destructively means a worker that dies before the PostgreSQL claim
takes nothing with it: the entry is still queued for someone else. Popping first
would open a window where the only record of readiness is gone while no worker
durably owns the job.

Ownership comes solely from the atomic PENDING -> PROCESSING update, so several
workers may look at the same candidate and exactly one can execute it.

Queue cleanup always targets the exact entry token this worker observed, so a
late removal cannot discard a newer entry for the same job.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger
from app.db.models import Job, JobStatus
from app.jobs.base import JobContext, JobExecutionError
from app.jobs.registry import get_handler
from app.services import job_service
from app.services.queue_service import QueueCandidate, ReadyQueue

logger = get_logger("worker")

#: Stale candidates are skipped within one cycle instead of sleeping between each.
MAX_CANDIDATES_PER_CYCLE = 10


class JobRunner:
    """Claims and executes ready jobs, one at a time."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        ready_queue: ReadyQueue,
        worker_id: str,
        poll_interval: float,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        self._session_factory = session_factory
        self._queue = ready_queue
        self._worker_id = worker_id
        self._poll_interval = poll_interval
        self._sleep = sleep
        self._random = random_source

    async def run_forever(self, stop: asyncio.Event) -> None:
        """Poll until asked to stop, backing off when there is nothing to do."""
        logger.info("worker_started", extra={"worker_id": self._worker_id})

        while not stop.is_set():
            try:
                did_work = await self.run_once()
            except Exception:
                logger.exception("worker_cycle_failed", extra={"worker_id": self._worker_id})
                did_work = False

            if not did_work:
                # Waiting on the stop event instead of a plain sleep keeps
                # shutdown responsive without busy-spinning.
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass

        logger.info("worker_stopped", extra={"worker_id": self._worker_id})

    async def run_once(self) -> bool:
        """Execute at most one job. Returns whether a job was executed."""
        for _ in range(MAX_CANDIDATES_PER_CYCLE):
            candidate = await self._queue.peek()
            if candidate is None:
                return False

            logger.debug(
                "job_candidate_seen",
                extra={
                    "job_id": str(candidate.job_id),
                    "worker_id": self._worker_id,
                    "queue_entry": candidate.member,
                },
            )

            async with self._session_factory() as session:
                job = await job_service.claim_job(session, candidate.job_id, self._worker_id)

            if job is None:
                await self._discard_unclaimable(candidate)
                continue

            # Ownership is durable now, so dropping the observed entry is safe.
            await self._queue.remove(candidate)
            logger.info(
                "job_claimed",
                extra={
                    "job_id": str(job.id),
                    "job_type": job.type,
                    "worker_id": self._worker_id,
                    "attempt": job.attempt_count,
                },
            )
            await self._execute(job)
            return True

        return False

    async def _discard_unclaimable(self, candidate: QueueCandidate) -> None:
        """Drop a candidate this worker could not claim.

        The entry is removed unless the job is genuinely still claimable, which
        keeps a job that someone else owns from being polled forever without
        discarding work that a future attempt could pick up. Removal targets the
        observed token, so a newer entry for the same job is never affected.
        """
        async with self._session_factory() as session:
            job = await job_service.get_job(session, candidate.job_id)

        still_claimable = (
            job is not None
            and job.status is JobStatus.PENDING
            and job.attempt_count < job.max_attempts
        )
        removed = False if still_claimable else await self._queue.remove(candidate)

        logger.info(
            "job_claim_failed",
            extra={
                "job_id": str(candidate.job_id),
                "job_type": job.type if job else None,
                "worker_id": self._worker_id,
                "job_status": job.status.value if job else "missing",
                "queue_entry": candidate.member,
                "queue_entry_removed": removed,
            },
        )

    async def _execute(self, job: Job) -> None:
        context_log = {
            "job_id": str(job.id),
            "job_type": job.type,
            "worker_id": self._worker_id,
            "attempt": job.attempt_count,
        }
        logger.info("job_started", extra=context_log)

        handler = get_handler(job.type)
        if handler is None:
            await self._fail(job, f"no handler registered for job type '{job.type}'", context_log)
            return

        async with self._session_factory() as session:
            context = JobContext(
                job_id=job.id,
                job_type=job.type,
                payload=job.payload,
                attempt=job.attempt_count,
                worker_id=self._worker_id,
                sleep=self._sleep,
                random=self._random,
                report_progress=self._progress_reporter(session, job),
            )

            try:
                result = await handler(context)
            except JobExecutionError as error:
                await job_service.fail_job(session, job.id, str(error))
                logger.warning("job_failed", extra={**context_log, "error": str(error)})
            except Exception as error:
                logger.exception("job_failed", extra={**context_log, "error": repr(error)})
                await job_service.fail_job(session, job.id, f"unexpected error: {error!r}")
            else:
                await job_service.complete_job(session, job.id, result)
                logger.info("job_completed", extra=context_log)

    async def _fail(self, job: Job, error: str, context_log: dict) -> None:
        async with self._session_factory() as session:
            await job_service.fail_job(session, job.id, error)
        logger.warning("job_failed", extra={**context_log, "error": error})

    def _progress_reporter(
        self, session: AsyncSession, job: Job
    ) -> Callable[[int, int], Awaitable[None]]:
        async def report(processed: int, total: int) -> None:
            percentage = int(processed / total * 100) if total else 100
            await job_service.update_progress(session, job.id, percentage)

        return report


def build_runner(
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    worker_id: str,
    poll_interval: float,
) -> JobRunner:
    return JobRunner(
        session_factory=session_factory,
        ready_queue=ReadyQueue(redis),
        worker_id=worker_id,
        poll_interval=poll_interval,
    )
