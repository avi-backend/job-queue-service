"""Payload validation must reject bad input before anything is persisted."""

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job
from tests.factories import VALID_PAYLOADS, job_request


@pytest.mark.parametrize("job_type", sorted(VALID_PAYLOADS))
async def test_valid_payload_accepted_for_every_job_type(
    client: AsyncClient, job_type: str
) -> None:
    response = await client.post("/jobs", json=job_request(job_type))

    assert response.status_code == 201, response.text
    assert response.json()["type"] == job_type


@pytest.mark.parametrize(
    ("job_type", "payload", "case"),
    [
        ("email", {"subject": "Hello"}, "missing recipient"),
        ("email", {"to": "not-an-email", "subject": "Hello"}, "malformed recipient"),
        ("email", {"to": "user@example.com", "subject": ""}, "empty subject"),
        (
            "email",
            {"to": "user@example.com", "subject": "Hello", "unexpected": 1},
            "unknown field",
        ),
        ("webhook", {"url": "not-a-url"}, "malformed url"),
        ("webhook", {}, "missing url"),
        ("report", {"report_type": "sales", "format": "xlsx"}, "unsupported format"),
        ("report", {"format": "pdf"}, "missing report type"),
        ("batch", {"items": []}, "empty items"),
        ("batch", {}, "missing items"),
    ],
)
async def test_invalid_payload_is_rejected_and_not_persisted(
    client: AsyncClient,
    session: AsyncSession,
    job_type: str,
    payload: dict,
    case: str,
) -> None:
    response = await client.post("/jobs", json={"type": job_type, "payload": payload})

    assert response.status_code == 422, f"{case}: {response.text}"
    assert await session.scalar(select(func.count()).select_from(Job)) == 0, case


async def test_unknown_job_type_is_rejected(client: AsyncClient) -> None:
    response = await client.post(
        "/jobs", json={"type": "teleport", "payload": {"items": [1]}}
    )

    assert response.status_code == 422


async def test_payload_is_stored_in_canonical_form(
    client: AsyncClient, session: AsyncSession
) -> None:
    """Optional fields are materialised so workers see a predictable shape."""
    response = await client.post(
        "/jobs", json={"type": "email", "payload": {"to": "user@example.com", "subject": "Hi"}}
    )

    assert response.status_code == 201
    job = (await session.execute(select(Job))).scalars().one()
    assert job.payload == {"to": "user@example.com", "subject": "Hi", "body": None}


@pytest.mark.parametrize("priority", [-1, 101])
async def test_priority_outside_allowed_range_is_rejected(
    client: AsyncClient, priority: int
) -> None:
    response = await client.post("/jobs", json=job_request(priority=priority))

    assert response.status_code == 422


async def test_naive_scheduled_at_is_rejected(client: AsyncClient) -> None:
    response = await client.post("/jobs", json=job_request(scheduled_at="2030-01-01T00:00:00"))

    assert response.status_code == 422


@pytest.mark.parametrize(
    "field",
    [
        "status",
        "attempt_count",
        "result",
        "error",
        "worker_id",
        "lease_expires_at",
        "completed_at",
        "started_at",
        "max_attempts",
    ],
)
async def test_server_owned_fields_are_rejected(client: AsyncClient, field: str) -> None:
    response = await client.post("/jobs", json=job_request(**{field: "whatever"}))

    assert response.status_code == 422, field
