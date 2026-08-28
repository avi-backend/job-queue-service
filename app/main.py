"""FastAPI application entry point."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Response, status
from pydantic import BaseModel
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app import __version__
from app.api.jobs import router as jobs_router
from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.core.redis import close_redis, get_redis
from app.db.session import dispose_engine, get_session
from app.schemas.health import HealthResponse
from app.services.health import HEALTHY, collect_health
from app.services.queue_service import ReadyQueue

logger = get_logger(__name__)


class ServiceInfo(BaseModel):
    service: str
    version: str
    status: str
    docs: str


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    logger.info("api starting", extra={"environment": settings.environment})
    yield
    await close_redis()
    await dispose_engine()
    logger.info("api stopped")


app = FastAPI(
    title="Job Queue Service",
    description="Distributed job queue backed by PostgreSQL, with Redis as the ready-job queue.",
    version=__version__,
    lifespan=lifespan,
)

app.include_router(jobs_router)


@app.get("/", response_model=ServiceInfo, tags=["service"])
async def root() -> ServiceInfo:
    return ServiceInfo(
        service=settings.app_name,
        version=__version__,
        status="ok",
        docs="/docs",
    )


@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["service"],
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": HealthResponse,
            "description": "PostgreSQL or Redis is unreachable.",
        }
    },
)
async def health(
    response: Response,
    session: Annotated[AsyncSession, Depends(get_session)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> HealthResponse:
    """Dependency health plus queue and job-status counts.

    A mismatch between Redis `ready` and PostgreSQL `pending` is reported as-is
    and does not make the service unhealthy: Redis is an index, not the source
    of truth.
    """
    report = await collect_health(session, ReadyQueue(redis))
    if report.status != HEALTHY:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return report
