"""Job API endpoints."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis import get_redis
from app.db.models import JobStatus
from app.db.session import get_session
from app.schemas.jobs import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    JobCreateRequest,
    JobListResponse,
    JobResponse,
    JobType,
)
from app.services import job_service
from app.services.queue_service import ReadyQueue

router = APIRouter(prefix="/jobs", tags=["jobs"])


async def get_ready_queue(redis: Annotated[Redis, Depends(get_redis)]) -> ReadyQueue:
    return ReadyQueue(redis)

IdempotencyKey = Annotated[
    str | None,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Reusing a key within 24 hours returns the original job instead of creating another.",
    ),
]


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a job",
    responses={
        status.HTTP_200_OK: {
            "model": JobResponse,
            "description": "Existing job returned for a still-valid idempotency key.",
        }
    },
)
async def create_job(
    request: JobCreateRequest,
    response: Response,
    session: Annotated[AsyncSession, Depends(get_session)],
    ready_queue: Annotated[ReadyQueue, Depends(get_ready_queue)],
    idempotency_key: IdempotencyKey = None,
) -> JobResponse:
    job, created = await job_service.submit_job(session, ready_queue, request, idempotency_key)
    if not created:
        response.status_code = status.HTTP_200_OK
    return JobResponse.model_validate(job)


@router.get("", response_model=JobListResponse, summary="List jobs")
async def list_jobs(
    session: Annotated[AsyncSession, Depends(get_session)],
    job_status: Annotated[JobStatus | None, Query(alias="status")] = None,
    job_type: Annotated[JobType | None, Query(alias="type")] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JobListResponse:
    jobs, total = await job_service.list_jobs(
        session, status=job_status, job_type=job_type, limit=limit, offset=offset
    )
    return JobListResponse(
        items=[JobResponse.model_validate(job) for job in jobs],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get a job",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Job not found."}},
)
async def get_job(
    job_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> JobResponse:
    job = await job_service.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return JobResponse.model_validate(job)
