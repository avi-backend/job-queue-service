"""Identity of a single processing attempt.

Passing this object instead of a bare job id is deliberate: every write against a
PROCESSING job has to be fenced by worker_id and execution_token, and a signature
that demands the whole attempt makes it impossible to forget one of them.
"""

import uuid
from dataclasses import dataclass

from app.db.models import Job


@dataclass(frozen=True, slots=True)
class Attempt:
    """Proof of ownership handed out by a successful claim.

    worker_id says which process owns the job; execution_token says which
    ownership generation. attempt_count and max_attempts are captured here
    because they cannot change while the attempt is owned (only a claim
    increments attempt_count), so the retry decision can be made without
    re-reading the row.
    """

    job_id: uuid.UUID
    worker_id: str
    execution_token: uuid.UUID
    attempt_count: int
    max_attempts: int

    @classmethod
    def of(cls, job: Job) -> "Attempt":
        """Read the attempt identity off a row that is currently PROCESSING."""
        if job.worker_id is None or job.execution_token is None:
            raise ValueError(f"job {job.id} carries no owned attempt")
        return cls(
            job_id=job.id,
            worker_id=job.worker_id,
            execution_token=job.execution_token,
            attempt_count=job.attempt_count,
            max_attempts=job.max_attempts,
        )

    @property
    def has_attempts_left(self) -> bool:
        return self.attempt_count < self.max_attempts

    @property
    def log_context(self) -> dict[str, object]:
        return {
            "job_id": str(self.job_id),
            "worker_id": self.worker_id,
            "execution_token": str(self.execution_token),
            "attempt": self.attempt_count,
        }
