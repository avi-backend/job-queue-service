"""Worker entry point.

Runs the polling loop until SIGTERM/SIGINT stops it. Finishing an in-flight job
before exit, along with heartbeat leases and crash recovery, belongs to a later
phase; today a signal stops the loop and the process exits.
"""

import asyncio
import signal

from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.core.redis import close_redis, redis_client
from app.db.session import SessionFactory, dispose_engine
from worker.identity import build_worker_id
from worker.runner import build_runner

logger = get_logger("worker")


async def run() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    worker_id = build_worker_id()
    runner = build_runner(
        session_factory=SessionFactory,
        redis=redis_client,
        worker_id=worker_id,
        poll_interval=settings.worker_poll_interval_seconds,
    )

    try:
        await runner.run_forever(stop)
    finally:
        await close_redis()
        await dispose_engine()
        logger.info("worker_shutdown_complete", extra={"worker_id": worker_id})


def main() -> None:
    configure_logging()
    asyncio.run(run())


if __name__ == "__main__":
    main()
