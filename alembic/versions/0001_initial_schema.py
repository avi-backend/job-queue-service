"""initial schema: jobs and job_logs

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-08-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

JOB_STATUS_VALUES = (
    "scheduled",
    "pending",
    "processing",
    "completed",
    "failed",
    "cancelled",
)


def upgrade() -> None:
    job_status = postgresql.ENUM(*JOB_STATUS_VALUES, name="job_status", create_type=False)
    job_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("type", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", job_status, nullable=False),
        sa.Column("priority", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default=sa.text("3"), nullable=False),
        sa.Column("progress", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_id", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("idempotency_expires_at", sa.DateTime(timezone=True), nullable=True),
        # Names are bare: the metadata naming convention adds the ck_jobs_ prefix.
        sa.CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        sa.CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        sa.CheckConstraint("attempt_count <= max_attempts", name="attempt_count_within_max"),
        sa.CheckConstraint("progress BETWEEN 0 AND 100", name="progress_percentage_range"),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
    )
    op.create_index(
        "uq_jobs_idempotency_key_active",
        "jobs",
        ["idempotency_key"],
        unique=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.create_index(
        "ix_jobs_idempotency_expires_at",
        "jobs",
        ["idempotency_expires_at"],
        unique=False,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.create_index(
        "ix_jobs_scheduled_due",
        "jobs",
        ["scheduled_at"],
        unique=False,
        postgresql_where=sa.text("status = 'scheduled'"),
    )
    op.create_index(
        "ix_jobs_processing_lease",
        "jobs",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("status = 'processing'"),
    )
    op.create_index("ix_jobs_status_created_at", "jobs", ["status", "created_at"], unique=False)
    op.create_index("ix_jobs_type_created_at", "jobs", ["type", "created_at"], unique=False)
    op.create_index("ix_jobs_created_at", "jobs", ["created_at"], unique=False)

    op.create_table(
        "job_logs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name="fk_job_logs_job_id_jobs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_job_logs"),
    )
    op.create_index(
        "ix_job_logs_job_id_created_at", "job_logs", ["job_id", "created_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_job_logs_job_id_created_at", table_name="job_logs")
    op.drop_table("job_logs")

    op.drop_index("ix_jobs_created_at", table_name="jobs")
    op.drop_index("ix_jobs_type_created_at", table_name="jobs")
    op.drop_index("ix_jobs_status_created_at", table_name="jobs")
    op.drop_index("ix_jobs_processing_lease", table_name="jobs")
    op.drop_index("ix_jobs_scheduled_due", table_name="jobs")
    op.drop_index("ix_jobs_idempotency_expires_at", table_name="jobs")
    op.drop_index("uq_jobs_idempotency_key_active", table_name="jobs")
    op.drop_table("jobs")

    sa.Enum(name="job_status").drop(op.get_bind(), checkfirst=True)
