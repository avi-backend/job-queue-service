"""Worker entry point.

Each worker process runs three independent loops concurrently:

    runner    claims and executes ready jobs
    scheduler promotes due SCHEDULED jobs (future jobs and retries) to PENDING
    recovery  takes jobs away from owners whose lease expired

They are all safe to run in every replica of the worker, so `--scale worker=3`
gives three of each. Nothing coordinates them at the process level; the atomic
database transitions inside each loop are the only thing preventing duplicate
activation or duplicate recovery.

A signal stops all three. Finishing an in-flight job before exit is Phase 4B; a
job interrupted by shutdown today keeps its lease until it expires, and crash
recovery retries it.
"""

import asyncio
import signal

from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.core.redis import close_redis, redis_client
from app.db.session import SessionFactory, dispose_engine
from app.services.queue_service import ReadyQueue
from worker.identity import build_worker_id
from worker.recovery import LeaseRecovery
from worker.runner import JobRunner
from worker.scheduler import JobScheduler

logger = get_logger("worker")


async def run() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    worker_id = build_worker_id()
    ready_queue = ReadyQueue(redis_client)

    runner = JobRunner(
        session_factory=SessionFactory,
        ready_queue=ready_queue,
        worker_id=worker_id,
        poll_interval=settings.worker_poll_interval_seconds,
        lease_seconds=settings.job_lease_seconds,
        heartbeat_interval=settings.job_heartbeat_seconds,
    )
    scheduler = JobScheduler(
        session_factory=SessionFactory,
        ready_queue=ready_queue,
        worker_id=worker_id,
        interval=settings.scheduler_interval_seconds,
        batch_size=settings.scheduler_batch_size,
    )
    recovery = LeaseRecovery(
        session_factory=SessionFactory,
        worker_id=worker_id,
        interval=settings.recovery_interval_seconds,
        batch_size=settings.recovery_batch_size,
    )

    logger.info(
        "worker_configuration",
        extra={
            "worker_id": worker_id,
            "lease_seconds": settings.job_lease_seconds,
            "heartbeat_seconds": settings.job_heartbeat_seconds,
            "scheduler_interval_seconds": settings.scheduler_interval_seconds,
            "recovery_interval_seconds": settings.recovery_interval_seconds,
        },
    )

    try:
        await asyncio.gather(
            runner.run_forever(stop),
            scheduler.run_forever(stop),
            recovery.run_forever(stop),
        )
    finally:
        await close_redis()
        await dispose_engine()
        logger.info("worker_shutdown_complete", extra={"worker_id": worker_id})


def main() -> None:
    configure_logging()
    asyncio.run(run())


if __name__ == "__main__":
    main()
