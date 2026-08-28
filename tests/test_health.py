"""GET /health: dependency probes and queue statistics."""

from datetime import timedelta

from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis import get_redis
from app.core.time import utcnow
from app.db.models import JobStatus
from app.db.session import get_session
from app.main import app
from app.services.queue_service import ReadyQueue
from tests.factories import insert_job, job_request


async def test_health_reports_postgres_and_redis(
    client: AsyncClient, ready_queue: ReadyQueue
) -> None:
    response = await client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["database"] == "healthy"
    assert body["redis"] == "healthy"
    assert body["queue"]["ready"] == 0


async def test_health_reports_ready_queue_size_and_status_counts(
    client: AsyncClient, session: AsyncSession, ready_queue: ReadyQueue
) -> None:
    await client.post("/jobs", json=job_request())
    await client.post("/jobs", json=job_request())
    await client.post(
        "/jobs",
        json=job_request(scheduled_at=(utcnow() + timedelta(hours=1)).isoformat()),
    )
    await insert_job(session, status=JobStatus.PROCESSING, attempt_count=1)
    await insert_job(session, status=JobStatus.COMPLETED, attempt_count=1)
    await insert_job(session, status=JobStatus.FAILED, attempt_count=3, error="gave up")
    await insert_job(session, status=JobStatus.CANCELLED)

    response = await client.get("/health")

    assert response.status_code == 200
    queue = response.json()["queue"]
    assert queue["ready"] == await ready_queue.size() == 2
    assert queue["pending"] == 2
    assert queue["scheduled"] == 1
    assert queue["processing"] == 1
    assert queue["completed"] == 1
    assert queue["failed"] == 1
    assert queue["cancelled"] == 1


async def test_health_does_not_treat_pending_ready_mismatch_as_unhealthy(
    client: AsyncClient, session: AsyncSession, ready_queue: ReadyQueue
) -> None:
    """A PENDING row with no Redis entry is the documented enqueue window."""
    await insert_job(session, status=JobStatus.PENDING)

    response = await client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["queue"]["pending"] == 1
    assert body["queue"]["ready"] == 0


async def test_health_returns_503_when_redis_is_down(session_factory) -> None:
    dead = Redis.from_url(
        "redis://127.0.0.1:1/0",
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=0.05,
        socket_timeout=0.05,
    )

    async def override_session():
        async with session_factory() as request_session:
            yield request_session

    async def override_redis():
        return dead

    app.dependency_overrides[get_session] = override_session
    app.dependency_overrides[get_redis] = override_redis
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health")
    finally:
        app.dependency_overrides.clear()
        await dead.aclose()

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unhealthy"
    assert body["redis"] == "unhealthy"
    assert body["database"] == "healthy"
    assert body["queue"]["ready"] is None
    assert body["queue"]["pending"] == 0


async def test_health_returns_503_when_postgres_is_down(redis: Redis) -> None:
    class DeadSession:
        async def execute(self, *args, **kwargs):
            raise OSError("connection refused")

    async def override_session():
        yield DeadSession()

    async def override_redis():
        return redis

    app.dependency_overrides[get_session] = override_session
    app.dependency_overrides[get_redis] = override_redis
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unhealthy"
    assert body["database"] == "unhealthy"
    assert body["redis"] == "healthy"
    assert body["queue"]["ready"] == 0
    assert body["queue"]["pending"] is None
