"""Maps job types to handlers."""

from app.jobs.base import JobHandler
from app.jobs.handlers import handle_batch, handle_email, handle_report, handle_webhook
from app.schemas.jobs import JobType

HANDLERS: dict[JobType, JobHandler] = {
    JobType.EMAIL: handle_email,
    JobType.WEBHOOK: handle_webhook,
    JobType.REPORT: handle_report,
    JobType.BATCH: handle_batch,
}


def get_handler(job_type: str) -> JobHandler | None:
    """Look up a handler by the job's stored type string."""
    try:
        return HANDLERS[JobType(job_type)]
    except ValueError:
        return None
