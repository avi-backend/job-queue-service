"""Mock job handlers.

These simulate work with injected sleeps instead of doing real I/O, which is
what the assignment asks for. Durations and the webhook outcome come from the
context, so tests neither wait nor depend on luck.
"""

import uuid
from typing import Any

from app.jobs.base import JobContext, JobExecutionError

EMAIL_DURATION_SECONDS = (1.0, 3.0)
WEBHOOK_DURATION_SECONDS = (1.0, 2.0)
REPORT_DURATION_SECONDS = (3.0, 5.0)
BATCH_ITEM_DURATION_SECONDS = 0.2

#: Simulated webhook delivery succeeds 80% of the time.
WEBHOOK_SUCCESS_RATE = 0.8

REPORT_BASE_URL = "https://example.local/reports"


async def handle_email(context: JobContext) -> dict[str, Any]:
    await context.sleep(context.duration(*EMAIL_DURATION_SECONDS))
    return {"message_id": f"msg_{uuid.uuid4().hex}", "status": "sent"}


async def handle_webhook(context: JobContext) -> dict[str, Any]:
    await context.sleep(context.duration(*WEBHOOK_DURATION_SECONDS))

    if context.random() >= WEBHOOK_SUCCESS_RATE:
        raise JobExecutionError(
            f"webhook delivery to {context.payload['url']} failed with status 503"
        )

    return {"status_code": 200, "delivered": True}


async def handle_report(context: JobContext) -> dict[str, Any]:
    await context.sleep(context.duration(*REPORT_DURATION_SECONDS))
    file_format = context.payload["format"]
    return {"file_url": f"{REPORT_BASE_URL}/{context.job_id}.{file_format}"}


async def handle_batch(context: JobContext) -> dict[str, Any]:
    """Process each item, reporting progress as a percentage of the total."""
    items = context.payload["items"]
    total = len(items)
    processed = 0

    for _ in items:
        await context.sleep(BATCH_ITEM_DURATION_SECONDS)
        processed += 1
        await context.report_progress(processed, total)

    return {"processed": processed, "failed": 0, "total": total}
