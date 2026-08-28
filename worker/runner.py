"""Worker execution loop.

Pickup is deliberately peek-then-claim rather than pop-then-claim. Reading the
candidate non-destructively means a worker that dies before the PostgreSQL claim
takes nothing with it: the entry is still queued for someone else. Popping first
would open a window where the only record of readiness is gone while no worker
durably owns the job.

Ownership comes solely from the atomic PENDING -> PROCESSING claim, which mints a
fresh execution token. That token, together with worker_id, fences every write
this worker makes afterwards: heartbeat, progress, completion and failure all
match zero rows once the attempt has been recovered or reclaimed. A stale worker
can therefore waste work, but it cannot corrupt the job's state.

Queue cleanup always targets the exact entry token this worker observed, so a
late removal cannot discard a newer entry for the same job.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.errors import OwnershipLostError
from app.core.logging import get_logger
from app.db.models import Job, JobStatus
from app.jobs.base import JobContext, JobExecutionError, JobHandler
from app.jobs.registry import get_handler
from app.services import job_service
from app.services.attempt import Attempt
from app.services.job_service import AttemptOutcome
from app.services.queue_service import QueueCandidate, ReadyQueue
from worker.heartbeat import LeaseHeartbeat
from worker.loops import sleep_until_stopped

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
        lease_seconds: float,
        heartbeat_interval: float,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        self._session_factory = session_factory
        self._queue = ready_queue
        self._worker_id = worker_id
        self._poll_interval = poll_interval
        self._lease_seconds = lease_seconds
        self._heartbeat_interval = heartbeat_interval
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
                await sleep_until_stopped(stop, self._poll_interval)

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
                job = await job_service.claim_job(
                    session, candidate.job_id, self._worker_id, self._lease_seconds
                )

            if job is None:
                await self._discard_unclaimable(candidate)
                continue

            # Ownership is durable now, so dropping the observed entry is safe.
            await self._queue.remove(candidate)
            attempt = Attempt.of(job)
            logger.info(
                "job_claimed",
                extra={
                    **attempt.log_context,
                    "job_type": job.type,
                    "lease_expires_at": (
                        job.lease_expires_at.isoformat() if job.lease_expires_at else None
                    ),
                },
            )
            await self._execute(job, attempt)
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

    async def _execute(self, job: Job, attempt: Attempt) -> None:
        context_log = {**attempt.log_context, "job_type": job.type}
        logger.info("job_started", extra=context_log)

        handler = get_handler(job.type)
        if handler is None:
            async with self._session_factory() as session:
                await self._settle_failure(
                    session,
                    attempt,
                    f"no handler registered for job type '{job.type}'",
                    context_log,
                )
            return

        async with self._session_factory() as session:
            context = JobContext(
                job_id=job.id,
                job_type=job.type,
                payload=job.payload,
                attempt=attempt.attempt_count,
                worker_id=self._worker_id,
                execution_token=attempt.execution_token,
                sleep=self._sleep,
                random=self._random,
                report_progress=self._progress_reporter(session, attempt),
            )

            try:
                # The heartbeat stops before the job is settled, so a beat can
                # never race the completion or failure write.
                async with LeaseHeartbeat(
                    session_factory=self._session_factory,
                    attempt=attempt,
                    interval=self._heartbeat_interval,
                    lease_seconds=self._lease_seconds,
                ) as heartbeat:
                    result = await self._run_handler(handler, context, attempt, heartbeat)
            except OwnershipLostError:
                # Someone else owns this job now. Writing anything would
                # overwrite their state, so this attempt ends silently.
                logger.warning(
                    "job_ownership_lost",
                    extra={**context_log, "detail": "attempt abandoned without writing"},
                )
            except JobExecutionError as error:
                await self._settle_failure(session, attempt, str(error), context_log)
            except Exception as error:
                logger.exception("job_handler_crashed", extra=context_log)
                await self._settle_failure(
                    session, attempt, f"unexpected error: {error!r}", context_log
                )
            else:
                await self._settle_success(session, attempt, result, context_log)

    async def _run_handler(
        self,
        handler: JobHandler,
        context: JobContext,
        attempt: Attempt,
        heartbeat: LeaseHeartbeat,
    ) -> dict[str, Any]:
        """Run the handler, abandoning it if the heartbeat loses ownership.

        Racing the two means a stalled worker stops executing as soon as it
        learns the job was taken from it, instead of running to completion and
        discovering the truth only when its write is rejected. Cancellation
        reaches the handler coroutine only: work it already pushed to an
        external system stays done.
        """
        handler_task = asyncio.create_task(handler(context))
        lost_task = asyncio.create_task(heartbeat.ownership_lost.wait())
        try:
            done, _ = await asyncio.wait(
                {handler_task, lost_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if handler_task in done:
                return handler_task.result()

            handler_task.cancel()
            with suppress(asyncio.CancelledError):
                await handler_task
            raise OwnershipLostError(
                attempt.job_id, attempt.worker_id, attempt.execution_token
            )
        finally:
            lost_task.cancel()
            with suppress(asyncio.CancelledError):
                await lost_task

    async def _settle_success(
        self, session: AsyncSession, attempt: Attempt, result: dict[str, Any], context_log: dict
    ) -> None:
        try:
            await job_service.complete_job(session, attempt, result)
        except OwnershipLostError:
            logger.warning(
                "job_ownership_lost",
                extra={**context_log, "detail": "completion rejected; job has a newer owner"},
            )
            return
        logger.info("job_completed", extra=context_log)

    async def _settle_failure(
        self, session: AsyncSession, attempt: Attempt, error: str, context_log: dict
    ) -> None:
        """Record a failed attempt as a delayed retry or a permanent failure."""
        try:
            outcome = await job_service.fail_attempt(session, attempt, error)
        except OwnershipLostError:
            logger.warning(
                "job_ownership_lost",
                extra={**context_log, "detail": "failure rejected; job has a newer owner"},
            )
            return

        logger.warning(
            "job_failed",
            extra={
                **context_log,
                "error": error,
                "retrying": outcome.outcome is AttemptOutcome.RETRY_SCHEDULED,
            },
        )

    def _progress_reporter(
        self, session: AsyncSession, attempt: Attempt
    ) -> Callable[[int, int], Awaitable[None]]:
        """Progress writes are fenced too, so a stale worker cannot report."""

        async def report(processed: int, total: int) -> None:
            percentage = int(processed / total * 100) if total else 100
            await job_service.update_progress(session, attempt, percentage)

        return report


def build_runner(
    session_factory: async_sessionmaker[AsyncSession],
    redis: Redis,
    worker_id: str,
    poll_interval: float,
    lease_seconds: float,
    heartbeat_interval: float,
) -> JobRunner:
    return JobRunner(
        session_factory=session_factory,
        ready_queue=ReadyQueue(redis),
        worker_id=worker_id,
        poll_interval=poll_interval,
        lease_seconds=lease_seconds,
        heartbeat_interval=heartbeat_interval,
    )
