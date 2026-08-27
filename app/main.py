"""FastAPI application entry point."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from app import __version__
from app.api.jobs import router as jobs_router
from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.core.redis import close_redis
from app.db.session import dispose_engine

logger = get_logger(__name__)


class ServiceInfo(BaseModel):
    service: str
    version: str
    status: str
    docs: str


class HealthResponse(BaseModel):
    status: str


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


@app.get("/health", response_model=HealthResponse, tags=["service"])
async def health() -> HealthResponse:
    """Liveness only. Dependency and queue statistics land in a later phase."""
    return HealthResponse(status="ok")
