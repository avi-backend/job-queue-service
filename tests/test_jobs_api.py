"""Submission, retrieval and listing behaviour of the job API."""

import uuid
from datetime import timedelta

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.time import utcnow
from app.db.models import Job
from tests.factories import job_request

RESPONSE_FIELDS = {
    "id",
    "type",
    "payload",
    "status",
    "priority",
    "attempt_count",
    "max_attempts",
    "progress",
    "result",
    "error",
    "scheduled_at",
    "created_at",
    "started_at",
    "completed_at",
}


async def test_submit_then_retrieve_job(client: AsyncClient) -> None:
    created = await client.post("/jobs", json=job_request(priority=7))
    assert created.status_code == 201, created.text
    body = created.json()

    fetched = await client.get(f"/jobs/{body['id']}")

    assert fetched.status_code == 200
    assert fetched.json() == body
    assert body["type"] == "email"
    assert body["status"] == "pending"
    assert body["priority"] == 7
    assert body["attempt_count"] == 0
    assert body["max_attempts"] == 3
    assert body["progress"] == 0
    assert body["result"] is None
    assert body["error"] is None


async def test_response_exposes_expected_fields_only(client: AsyncClient) -> None:
    response = await client.post("/jobs", json=job_request())

    assert set(response.json()) == RESPONSE_FIELDS


async def test_future_scheduled_job_is_scheduled(
    client: AsyncClient, session: AsyncSession
) -> None:
    scheduled_at = utcnow() + timedelta(hours=1)

    response = await client.post(
        "/jobs", json=job_request(scheduled_at=scheduled_at.isoformat())
    )

    assert response.status_code == 201, response.text
    assert response.json()["status"] == "scheduled"
    job = (await session.execute(select(Job))).scalars().one()
    assert job.scheduled_at == scheduled_at


async def test_job_without_schedule_is_pending(client: AsyncClient) -> None:
    response = await client.post("/jobs", json=job_request())

    assert response.json()["status"] == "pending"
    assert response.json()["scheduled_at"] is None


async def test_job_scheduled_in_the_past_is_pending(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs", json=job_request(scheduled_at=(utcnow() - timedelta(minutes=5)).isoformat())
    )

    assert response.status_code == 201, response.text
    assert response.json()["status"] == "pending"


async def test_non_utc_offset_is_normalised_to_utc(
    client: AsyncClient, session: AsyncSession
) -> None:
    """A +02:00 instant an hour ahead is still in the future, and stored as UTC."""
    response = await client.post(
        "/jobs", json=job_request(scheduled_at="2030-06-01T12:00:00+02:00")
    )

    assert response.status_code == 201
    assert response.json()["status"] == "scheduled"
    job = (await session.execute(select(Job))).scalars().one()
    assert job.scheduled_at is not None
    assert job.scheduled_at.utcoffset() == timedelta(0)
    assert job.scheduled_at.hour == 10


async def test_unknown_job_returns_404(client: AsyncClient) -> None:
    response = await client.get(f"/jobs/{uuid.uuid4()}")

    assert response.status_code == 404
    assert response.json()["detail"] == "job not found"


async def test_malformed_uuid_returns_422(client: AsyncClient) -> None:
    response = await client.get("/jobs/not-a-uuid")

    assert response.status_code == 422


async def test_list_filters_by_status(client: AsyncClient) -> None:
    await client.post("/jobs", json=job_request())
    scheduled = await client.post(
        "/jobs",
        json=job_request(scheduled_at=(utcnow() + timedelta(hours=1)).isoformat()),
    )

    response = await client.get("/jobs", params={"status": "scheduled"})

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert [item["id"] for item in body["items"]] == [scheduled.json()["id"]]


async def test_list_filters_by_type(client: AsyncClient) -> None:
    await client.post("/jobs", json=job_request("email"))
    webhook = await client.post("/jobs", json=job_request("webhook"))
    await client.post("/jobs", json=job_request("report"))

    response = await client.get("/jobs", params={"type": "webhook"})

    body = response.json()
    assert body["total"] == 1
    assert [item["id"] for item in body["items"]] == [webhook.json()["id"]]


async def test_list_combines_status_and_type_filters(client: AsyncClient) -> None:
    await client.post("/jobs", json=job_request("email"))
    await client.post(
        "/jobs",
        json=job_request("webhook", scheduled_at=(utcnow() + timedelta(hours=1)).isoformat()),
    )

    response = await client.get("/jobs", params={"status": "pending", "type": "webhook"})

    assert response.json()["total"] == 0


async def test_list_is_ordered_newest_first(client: AsyncClient) -> None:
    first = await client.post("/jobs", json=job_request())
    second = await client.post("/jobs", json=job_request())
    third = await client.post("/jobs", json=job_request())

    response = await client.get("/jobs")

    returned = [item["id"] for item in response.json()["items"]]
    assert returned == [third.json()["id"], second.json()["id"], first.json()["id"]]


async def test_list_pagination_reports_total_and_window(client: AsyncClient) -> None:
    for _ in range(3):
        await client.post("/jobs", json=job_request())

    response = await client.get("/jobs", params={"limit": 2, "offset": 1})

    body = response.json()
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 1
    assert len(body["items"]) == 2


async def test_list_defaults(client: AsyncClient) -> None:
    response = await client.get("/jobs")

    body = response.json()
    assert body == {"items": [], "total": 0, "limit": 50, "offset": 0}


async def test_list_rejects_limit_above_maximum(client: AsyncClient) -> None:
    assert (await client.get("/jobs", params={"limit": 101})).status_code == 422
    assert (await client.get("/jobs", params={"limit": 0})).status_code == 422
    assert (await client.get("/jobs", params={"offset": -1})).status_code == 422


async def test_list_rejects_unknown_filter_values(client: AsyncClient) -> None:
    assert (await client.get("/jobs", params={"status": "sleeping"})).status_code == 422
    assert (await client.get("/jobs", params={"type": "teleport"})).status_code == 422


async def test_created_job_is_not_claimed_by_any_worker(
    client: AsyncClient, session: AsyncSession
) -> None:
    """Phase 2 only persists jobs; nothing claims or executes them yet."""
    await client.post("/jobs", json=job_request())

    job = (await session.execute(select(Job))).scalars().one()
    assert job.worker_id is None
    assert job.lease_expires_at is None
    assert job.started_at is None
    assert job.completed_at is None
    assert await session.scalar(select(func.count()).select_from(Job)) == 1
