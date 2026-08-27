"""Worker identity.

Stable for the lifetime of the process and unique across workers, so
`jobs.worker_id` records exactly which process claimed a job. The random suffix
keeps two containers distinct even if a scheduler reuses a hostname and pid.
"""

import os
import socket
import uuid


def build_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
