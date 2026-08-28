"""Errors that cross layer boundaries."""

import uuid


class JobNotFoundError(LookupError):
    """Raised when an API mutation names a job that does not exist."""

    def __init__(self, job_id: uuid.UUID) -> None:
        super().__init__(f"job {job_id} not found")
        self.job_id = job_id


class JobConflictError(RuntimeError):
    """Raised when a job exists but is not in a state that allows the mutation."""

    def __init__(self, job_id: uuid.UUID, action: str, status: str) -> None:
        super().__init__(f"cannot {action} job {job_id} while it is {status}")
        self.job_id = job_id
        self.action = action
        self.status = status


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
