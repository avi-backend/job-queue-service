"""Handler interface shared by all job types.

Handlers perform business execution only. Claiming, status transitions, result
persistence and logging belong to the worker, so handlers never touch a database
session. Sleeping and randomness arrive through the context, which is what lets
tests run instantly and deterministically.
"""

import asyncio
import random
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol


class JobExecutionError(Exception):
    """Raised by a handler when the job itself failed.

    Distinct from unexpected exceptions: this represents a business failure the
    worker is expected to record on the job.
    """


async def _no_progress(processed: int, total: int) -> None:
    """Default progress sink for handlers executed outside a worker."""


@dataclass(slots=True)
class JobContext:
    """Everything a handler may use, with side effects injected."""

    job_id: uuid.UUID
    job_type: str
    payload: dict[str, Any]
    attempt: int
    worker_id: str
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    random: Callable[[], float] = random.random
    report_progress: Callable[[int, int], Awaitable[None]] = field(default=_no_progress)

    def duration(self, minimum: float, maximum: float) -> float:
        """Pick a simulated duration inside a range using the injected source."""
        return minimum + self.random() * (maximum - minimum)


class JobHandler(Protocol):
    """Executes one job and returns the result stored on it."""

    async def __call__(self, context: JobContext) -> dict[str, Any]: ...
