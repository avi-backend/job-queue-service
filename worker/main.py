"""Placeholder worker entry point.

Phase 1 only proves the container boots and stays healthy. Job claiming, the
execution loop, retries, and lease recovery are implemented in a later phase.
"""

import asyncio
import signal

from app.core.logging import configure_logging, get_logger

logger = get_logger("worker")

HEARTBEAT_SECONDS = 30


async def run() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    logger.warning(
        "worker placeholder started: job execution is not implemented in this phase"
    )

    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_SECONDS)
        except TimeoutError:
            logger.info("worker idle: execution loop pending implementation")

    logger.info("worker placeholder stopped")


def main() -> None:
    configure_logging()
    asyncio.run(run())


if __name__ == "__main__":
    main()
