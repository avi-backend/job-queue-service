"""add jobs.execution_token fencing token

Revision ID: 0002_execution_token
Revises: 0001_initial_schema
Create Date: 2026-08-28

Adds the fencing token and releases any Phase 3 PROCESSING row that cannot be
recovered under the new lease model.

Phase 3 claims set worker_id but left lease_expires_at NULL and had no token
column. Crash recovery only matches `lease_expires_at < now()`, and that
comparison is unknown when the lease is NULL, so those rows would otherwise
stay PROCESSING forever after upgrade.

The data step therefore moves pre-token PROCESSING rows to SCHEDULED with
scheduled_at = now(), clears the ownership fields, and leaves attempt_count
alone: the old claim already consumed that attempt. The existing scheduler
then activates them under the fenced claim path.

Downgrade only drops the column. Released rows stay SCHEDULED; putting them
back to PROCESSING would re-strand them without a lease.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0002_execution_token"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("execution_token", postgresql.UUID(as_uuid=True), nullable=True),
    )
    # After add_column every existing row has a NULL token, which is exactly
    # the Phase 3 PROCESSING set. A row already claimed under 4A would have a
    # token and must not be touched.
    op.execute(
        sa.text(
            """
            UPDATE jobs
            SET
                status = 'scheduled',
                scheduled_at = now(),
                worker_id = NULL,
                execution_token = NULL,
                lease_expires_at = NULL
            WHERE status = 'processing'
              AND execution_token IS NULL
            """
        )
    )


def downgrade() -> None:
    op.drop_column("jobs", "execution_token")
