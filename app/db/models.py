"""PostgreSQL models. PostgreSQL is the durable source of truth for job state."""

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class JobStatus(str, enum.Enum):
    """Job lifecycle states. Values are stored lowercase in PostgreSQL."""

    SCHEDULED = "scheduled"
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


#: Terminal states are useful for both queries and later application logic.
TERMINAL_STATUSES: frozenset[JobStatus] = frozenset(
    {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
)

job_status_enum = Enum(
    JobStatus,
    name="job_status",
    # Persist the lowercase member values, not the uppercase member names.
    values_callable=lambda enum_cls: [member.value for member in enum_cls],
    native_enum=True,
)


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    type: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[JobStatus] = mapped_column(job_status_enum, nullable=False)

    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default=text("3")
    )
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))

    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Ownership of an in-flight job. Set only by the atomic PENDING -> PROCESSING
    # transition that a worker performs in a later phase.
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    idempotency_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    logs: Mapped[list["JobLog"]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        CheckConstraint("attempt_count <= max_attempts", name="attempt_count_within_max"),
        CheckConstraint("progress BETWEEN 0 AND 100", name="progress_percentage_range"),
        # The database is the concurrency guarantee against duplicate keys; the
        # 24h window itself is enforced in the application layer, which releases
        # a key by nulling it once it expires.
        Index(
            "uq_jobs_idempotency_key_active",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        # Sweep that releases expired idempotency keys.
        Index(
            "ix_jobs_idempotency_expires_at",
            "idempotency_expires_at",
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        # Scheduler: status = 'scheduled' AND scheduled_at <= now()
        Index(
            "ix_jobs_scheduled_due",
            "scheduled_at",
            postgresql_where=text("status = 'scheduled'"),
        ),
        # Crash recovery: status = 'processing' AND lease_expires_at < now()
        Index(
            "ix_jobs_processing_lease",
            "lease_expires_at",
            postgresql_where=text("status = 'processing'"),
        ),
        # Job listing, filtered by status or type and ordered by recency. Plain
        # btree indexes are enough: PostgreSQL scans them backwards for DESC.
        Index("ix_jobs_status_created_at", "status", "created_at"),
        Index("ix_jobs_type_created_at", "type", "created_at"),
        Index("ix_jobs_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<Job id={self.id} type={self.type!r} status={self.status.value}>"


class JobLog(Base):
    __tablename__ = "job_logs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    level: Mapped[str] = mapped_column(String(16), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    # `metadata` is reserved on declarative classes, so the attribute is renamed
    # while the column keeps the specified name.
    log_metadata: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    job: Mapped[Job] = relationship(back_populates="logs")

    __table_args__ = (Index("ix_job_logs_job_id_created_at", "job_id", "created_at"),)

    def __repr__(self) -> str:
        return f"<JobLog id={self.id} job_id={self.job_id} level={self.level!r}>"
