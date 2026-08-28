"""Errors that cross layer boundaries."""

import uuid


class OwnershipLostError(RuntimeError):
    """Raised when a worker writes to a job it no longer owns.

    Every write against a PROCESSING job is conditional on job id, status,
    worker_id and execution_token. Zero updated rows means the attempt was taken
    away (lease expired, recovery ran, another worker reclaimed it), so the
    correct response is to stop and surface it, never to retry the write or
    assume it landed.
    """

    def __init__(self, job_id: uuid.UUID, worker_id: str, execution_token: uuid.UUID) -> None:
        super().__init__(
            f"worker {worker_id} no longer owns attempt {execution_token} of job {job_id}"
        )
        self.job_id = job_id
        self.worker_id = worker_id
        self.execution_token = execution_token
