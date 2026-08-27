"""Time helpers. Every datetime in this service is timezone-aware and in UTC."""

from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(UTC)


def to_utc(value: datetime) -> datetime:
    """Normalise an aware datetime to UTC. Naive input is a programming error."""
    if value.tzinfo is None:
        raise ValueError("naive datetime is not allowed; expected a timezone-aware value")
    return value.astimezone(UTC)
