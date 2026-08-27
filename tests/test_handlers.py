"""Handlers in isolation. Sleeps and randomness are injected, so nothing waits."""

import uuid

import pytest

from app.jobs.base import JobContext, JobExecutionError
from app.jobs.handlers import (
    WEBHOOK_SUCCESS_RATE,
    handle_batch,
    handle_email,
    handle_report,
    handle_webhook,
)
from app.jobs.registry import get_handler
from app.schemas.jobs import JobType
from tests.factories import VALID_PAYLOADS


async def _no_progress(processed: int, total: int) -> None:
    pass


def build_context(
    job_type: str, random_value: float = 0.0, **overrides
) -> tuple[JobContext, list[float]]:
    """Return a context plus the list that records requested sleep durations.

    The durations are recorded rather than awaited, so tests can assert the
    simulated timing without spending it.
    """
    slept: list[float] = []

    async def record_sleep(seconds: float) -> None:
        slept.append(seconds)

    context = JobContext(
        job_id=overrides.get("job_id", uuid.uuid4()),
        job_type=job_type,
        payload=overrides.get("payload", VALID_PAYLOADS[job_type]),
        attempt=1,
        worker_id="worker-test",
        sleep=record_sleep,
        random=lambda: random_value,
        report_progress=overrides.get("report_progress", _no_progress),
    )
    return context, slept


async def test_email_handler_returns_message_id() -> None:
    context, slept = build_context("email")

    result = await handle_email(context)

    assert result["status"] == "sent"
    assert result["message_id"].startswith("msg_")
    assert slept == [1.0]


async def test_email_duration_stays_within_the_simulated_range() -> None:
    fastest, fastest_sleeps = build_context("email", random_value=0.0)
    slowest, slowest_sleeps = build_context("email", random_value=1.0)

    await handle_email(fastest)
    await handle_email(slowest)

    assert fastest_sleeps == [1.0]
    assert slowest_sleeps == [3.0]


async def test_report_handler_returns_file_url() -> None:
    job_id = uuid.uuid4()
    context, slept = build_context("report", job_id=job_id)

    result = await handle_report(context)

    assert result["file_url"] == f"https://example.local/reports/{job_id}.pdf"
    assert slept == [3.0]


async def test_report_handler_uses_requested_format() -> None:
    context, _ = build_context("report", payload={"report_type": "sales", "format": "csv"})

    result = await handle_report(context)

    assert result["file_url"].endswith(".csv")


async def test_webhook_handler_succeeds_deterministically() -> None:
    context, slept = build_context("webhook", random_value=0.0)

    result = await handle_webhook(context)

    assert result == {"status_code": 200, "delivered": True}
    assert slept == [1.0]


async def test_webhook_handler_fails_deterministically() -> None:
    context, _ = build_context("webhook", random_value=0.99)

    with pytest.raises(JobExecutionError) as failure:
        await handle_webhook(context)

    assert "https://example.com/webhook" in str(failure.value)
    assert "503" in str(failure.value)


@pytest.mark.parametrize(
    ("random_value", "should_succeed"),
    [(0.0, True), (WEBHOOK_SUCCESS_RATE - 0.01, True), (WEBHOOK_SUCCESS_RATE, False), (1.0, False)],
)
async def test_webhook_success_boundary(random_value: float, should_succeed: bool) -> None:
    context, _ = build_context("webhook", random_value=random_value)

    if should_succeed:
        assert await handle_webhook(context) == {"status_code": 200, "delivered": True}
    else:
        with pytest.raises(JobExecutionError):
            await handle_webhook(context)


async def test_batch_handler_reports_progress_for_each_item() -> None:
    reported: list[tuple[int, int]] = []

    async def report(processed: int, total: int) -> None:
        reported.append((processed, total))

    context, slept = build_context(
        "batch",
        payload={"items": [{"i": 1}, {"i": 2}, {"i": 3}, {"i": 4}]},
        report_progress=report,
    )

    result = await handle_batch(context)

    assert reported == [(1, 4), (2, 4), (3, 4), (4, 4)]
    assert result == {"processed": 4, "failed": 0, "total": 4}
    assert len(slept) == 4


async def test_registry_covers_every_job_type() -> None:
    for job_type in JobType:
        assert get_handler(job_type.value) is not None


async def test_registry_returns_none_for_unknown_type() -> None:
    assert get_handler("teleport") is None
