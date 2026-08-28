"""Configuration invariants.

The heartbeat/lease relationship is validated rather than documented, because a
heartbeat slower than the lease would let a perfectly healthy worker be recovered
while it is still executing, which is the exact outcome the lease exists to
prevent.
"""

import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_defaults_beat_well_inside_the_lease() -> None:
    settings = Settings()

    assert settings.job_heartbeat_seconds < settings.job_lease_seconds
    # Enough headroom that one slow or failed beat is survivable.
    assert settings.job_heartbeat_seconds * 2 <= settings.job_lease_seconds


@pytest.mark.parametrize(
    ("lease", "heartbeat"),
    [(60, 60), (60, 90), (5, 5.0001)],
)
def test_a_heartbeat_at_or_past_the_lease_is_rejected(lease: float, heartbeat: float) -> None:
    with pytest.raises(ValidationError, match="JOB_HEARTBEAT_SECONDS"):
        Settings(job_lease_seconds=lease, job_heartbeat_seconds=heartbeat)


@pytest.mark.parametrize("value", [0, -1])
def test_timings_must_be_positive(value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(job_lease_seconds=value)
    with pytest.raises(ValidationError):
        Settings(scheduler_interval_seconds=value)
    with pytest.raises(ValidationError):
        Settings(recovery_interval_seconds=value)


@pytest.mark.parametrize("value", [0, -5])
def test_batch_sizes_must_be_at_least_one(value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(scheduler_batch_size=value)
    with pytest.raises(ValidationError):
        Settings(recovery_batch_size=value)
