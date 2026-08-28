"""Health and queue-statistics response."""

from pydantic import BaseModel, Field


class QueueStats(BaseModel):
    """Live counts. `ready` is Redis; the rest are PostgreSQL GROUP BY counts.

    `ready` and `pending` can differ. Redis is only an index, so a job can be
    PENDING in PostgreSQL and missing from the ready queue (or the reverse, a
    stale entry). That mismatch is a visibility window, not an outage, and does
    not by itself make the service unhealthy.
    """

    ready: int | None = Field(
        default=None,
        description="Redis ready-queue size. Null when Redis cannot be reached.",
    )
    pending: int | None = None
    scheduled: int | None = None
    processing: int | None = None
    completed: int | None = Field(
        default=None,
        description="Included so operators can see throughput, not just backlog.",
    )
    failed: int | None = None
    cancelled: int | None = None


class HealthResponse(BaseModel):
    status: str
    database: str
    redis: str
    queue: QueueStats
