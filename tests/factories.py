"""Shared request builders for the API tests."""

from typing import Any

VALID_PAYLOADS: dict[str, dict[str, Any]] = {
    "email": {"to": "user@example.com", "subject": "Hello", "body": "optional body"},
    "webhook": {"url": "https://example.com/webhook", "event": "order.created"},
    "report": {"report_type": "sales", "format": "pdf"},
    "batch": {"items": [{"index": 1}, {"index": 2}]},
}


def job_request(job_type: str = "email", **overrides: Any) -> dict[str, Any]:
    """Build a valid POST /jobs body, overriding individual fields as needed."""
    body: dict[str, Any] = {"type": job_type, "payload": VALID_PAYLOADS[job_type]}
    body.update(overrides)
    return body
