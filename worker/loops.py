"""Shared shape for the worker's background loops.

Every loop waits on the stop event instead of sleeping, so SIGTERM is noticed
immediately rather than after a full interval, and no loop is allowed to die on
an exception: a failed cycle is logged and the next one runs.
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

from app.core.logging import get_logger

logger = get_logger("worker")


async def sleep_until_stopped(stop: asyncio.Event, timeout: float) -> None:
    """Wait out an interval, returning early when asked to stop."""
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=timeout)


async def run_until_stopped(
    cycle: Callable[[], Awaitable[object]],
    interval: float,
    stop: asyncio.Event,
    failure_event: str,
    context: dict[str, object],
) -> None:
    """Run `cycle` every `interval` seconds until `stop` is set."""
    while not stop.is_set():
        try:
            await cycle()
        except Exception:
            logger.exception(failure_event, extra=context)
        await sleep_until_stopped(stop, interval)
