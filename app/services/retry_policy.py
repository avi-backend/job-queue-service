"""Retry timing. One function so every caller agrees on the schedule.

The assignment asks for three attempts with a fixed exponential-style backoff:

    attempt 1 fails -> wait 30s  -> attempt 2
    attempt 2 fails -> wait 120s -> attempt 3
    attempt 3 fails -> FAILED

The delay is chosen from the attempt that just failed, not from the attempt
about to run, because attempt_count is already incremented at claim time and is
therefore the only value a failing worker (or crash recovery) can trust.
"""

#: Delay before the attempt that follows the attempt used as the key.
RETRY_DELAYS_SECONDS: dict[int, int] = {1: 30, 2: 120}


def retry_delay_seconds(attempt_count: int) -> int:
    """Seconds to wait before retrying after `attempt_count` failed attempts."""
    if attempt_count not in RETRY_DELAYS_SECONDS:
        raise ValueError(
            f"no retry delay defined for attempt {attempt_count}; "
            f"known attempts: {sorted(RETRY_DELAYS_SECONDS)}"
        )
    return RETRY_DELAYS_SECONDS[attempt_count]
