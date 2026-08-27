"""Request and response schemas for the job API."""

import enum
import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    HttpUrl,
    ValidationError,
    model_validator,
)

from app.db.models import JobStatus

MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 50
MIN_PRIORITY = 0
MAX_PRIORITY = 100


class JobType(str, enum.Enum):
    """Job types the service accepts. Persisted as a plain string column."""

    EMAIL = "email"
    WEBHOOK = "webhook"
    REPORT = "report"
    BATCH = "batch"


class ReportFormat(str, enum.Enum):
    PDF = "pdf"
    CSV = "csv"


class PayloadBase(BaseModel):
    """Unknown payload keys are rejected so typos surface as validation errors."""

    model_config = ConfigDict(extra="forbid")


class EmailPayload(PayloadBase):
    to: EmailStr
    subject: str = Field(min_length=1, max_length=255)
    body: str | None = None


class WebhookPayload(PayloadBase):
    url: HttpUrl
    event: str | None = Field(default=None, min_length=1, max_length=255)


class ReportPayload(PayloadBase):
    report_type: str = Field(min_length=1, max_length=255)
    format: ReportFormat


class BatchPayload(PayloadBase):
    items: list[Any] = Field(min_length=1)


PAYLOAD_MODELS: dict[JobType, type[PayloadBase]] = {
    JobType.EMAIL: EmailPayload,
    JobType.WEBHOOK: WebhookPayload,
    JobType.REPORT: ReportPayload,
    JobType.BATCH: BatchPayload,
}


def _format_payload_errors(error: ValidationError) -> str:
    parts = []
    for item in error.errors():
        location = ".".join(str(piece) for piece in item["loc"]) or "payload"
        parts.append(f"{location}: {item['msg']}")
    return "; ".join(parts)


class JobCreateRequest(BaseModel):
    """Client-supplied job submission. Server-owned fields are not accepted."""

    model_config = ConfigDict(extra="forbid")

    type: JobType
    payload: dict[str, Any]
    priority: int = Field(default=0, ge=MIN_PRIORITY, le=MAX_PRIORITY)
    scheduled_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def validate_payload_for_type(self) -> "JobCreateRequest":
        """Validate the payload against its job type before anything is persisted.

        The nested error is re-raised as a ValueError so FastAPI reports it as a
        normal 422 body validation error instead of a server error.
        """
        model = PAYLOAD_MODELS[self.type]
        try:
            validated = model.model_validate(self.payload)
        except ValidationError as error:
            raise ValueError(
                f"invalid payload for job type '{self.type.value}': "
                f"{_format_payload_errors(error)}"
            ) from error

        # Store the canonical, JSON-serialisable form of the payload.
        self.payload = validated.model_dump(mode="json")
        return self


class JobResponse(BaseModel):
    """Public job representation. Lease and worker ownership stay internal."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    attempt_count: int
    max_attempts: int
    progress: int
    result: dict[str, Any] | None
    error: str | None
    scheduled_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class JobListResponse(BaseModel):
    items: list[JobResponse]
    total: int
    limit: int
    offset: int


PageLimit = Annotated[int, Field(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)]
PageOffset = Annotated[int, Field(default=0, ge=0)]
