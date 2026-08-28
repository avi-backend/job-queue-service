"""Liveness and cheap queue statistics.

PostgreSQL counts and Redis ready size are gathered independently so a failure
in one store is visible without hiding the other. A difference between
`pending` and `ready` is expected and is not treated as unhealthiness.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models import JobStatus
from app.schemas.health import HealthResponse, QueueStats
from app.services import job_service
from app.services.queue_service import ReadyQueue

logger = get_logger(__name__)

HEALTHY = "healthy"
UNHEALTHY = "unhealthy"


async def collect_health(session: AsyncSession, ready_queue: ReadyQueue) -> HealthResponse:
    """Probe both stores and return a report. Never raises for a down dependency."""
    database = UNHEALTHY
    redis = UNHEALTHY
    counts = {status.value: None for status in JobStatus}
    ready: int | None = None

    try:
        await session.execute(text("SELECT 1"))
        counts = await job_service.count_jobs_by_status(session)
        database = HEALTHY
    except Exception:
        logger.exception("health_database_failed")

    try:
        ready = await ready_queue.size()
        redis = HEALTHY
    except Exception:
        logger.exception("health_redis_failed")

    overall = HEALTHY if database == HEALTHY and redis == HEALTHY else UNHEALTHY
    return HealthResponse(
        status=overall,
        database=database,
        redis=redis,
        queue=QueueStats(ready=ready, **counts),
    )
