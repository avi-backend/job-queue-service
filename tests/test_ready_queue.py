"""Ready queue behaviour: membership rules, absolute priority, FIFO and entry tokens."""

import uuid
from datetime import timedelta

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis

from app.core.time import utcnow
from app.services.queue_service import (
    ENTRIES_KEY,
    MAX_PRIORITY,
    READY_QUEUE_KEY,
    SEQUENCE_KEY,
    SEQUENCE_WIDTH,
    QueueCandidate,
    ReadyQueue,
    entry_score,
)
from tests.factories import job_request


async def test_pending_submission_is_enqueued(
    client: AsyncClient, ready_queue: ReadyQueue
) -> None:
    response = await client.post("/jobs", json=job_request())

    assert response.status_code == 201
    assert await ready_queue.size() == 1
    candidate = await ready_queue.peek()
    assert candidate is not None
    assert candidate.job_id == uuid.UUID(response.json()["id"])


async def test_scheduled_submission_is_not_enqueued(
    client: AsyncClient, ready_queue: ReadyQueue
) -> None:
    response = await client.post(
        "/jobs",
        json=job_request(scheduled_at=(utcnow() + timedelta(hours=1)).isoformat()),
    )

    assert response.status_code == 201
    assert response.json()["status"] == "scheduled"
    assert await ready_queue.size() == 0


async def test_idempotent_replay_is_not_enqueued_twice(
    client: AsyncClient, ready_queue: ReadyQueue
) -> None:
    headers = {"Idempotency-Key": "queue-key"}

    first = await client.post("/jobs", json=job_request(), headers=headers)
    second = await client.post("/jobs", json=job_request(), headers=headers)

    assert first.status_code == 201
    assert second.status_code == 200
    assert await ready_queue.size() == 1


async def test_higher_priority_is_served_first(
    client: AsyncClient, ready_queue: ReadyQueue
) -> None:
    low = await client.post("/jobs", json=job_request(priority=1))
    high = await client.post("/jobs", json=job_request(priority=90))
    medium = await client.post("/jobs", json=job_request(priority=50))

    order = await ready_queue.peek_many(3)

    assert [str(candidate.job_id) for candidate in order] == [
        high.json()["id"],
        medium.json()["id"],
        low.json()["id"],
    ]


async def test_fifo_within_the_same_priority(
    client: AsyncClient, ready_queue: ReadyQueue
) -> None:
    submitted = [
        (await client.post("/jobs", json=job_request(priority=5))).json()["id"]
        for _ in range(5)
    ]

    order = await ready_queue.peek_many(5)

    assert [str(candidate.job_id) for candidate in order] == submitted
    # Ties are broken by the padded sequence in the member, not by chance.
    assert [candidate.sequence for candidate in order] == sorted(
        candidate.sequence for candidate in order
    )


async def test_priority_beats_arrival_order(
    client: AsyncClient, ready_queue: ReadyQueue
) -> None:
    """A late high-priority job jumps ahead of an earlier low-priority job."""
    early = await client.post("/jobs", json=job_request(priority=0))
    late = await client.post("/jobs", json=job_request(priority=100))

    order = await ready_queue.peek_many(2)

    assert [str(candidate.job_id) for candidate in order] == [
        late.json()["id"],
        early.json()["id"],
    ]


async def test_priority_ordering_survives_any_sequence_gap(
    ready_queue: ReadyQueue, redis: Redis
) -> None:
    """The invariant that the previous score formula could not hold.

    A priority 99 job is queued, then the sequence is advanced past any band
    width a combined score could have used, and a priority 100 job is queued.
    Because the score carries priority alone, the newer high-priority job still
    sorts first no matter how large the sequence gap is.
    """
    lower_priority = await ready_queue.enqueue(uuid.uuid4(), 99)

    await redis.set(SEQUENCE_KEY, 10**18)
    higher_priority = await ready_queue.enqueue(uuid.uuid4(), 100)

    assert higher_priority.sequence > lower_priority.sequence + 10**12
    order = await ready_queue.peek_many(2)
    assert [candidate.job_id for candidate in order] == [
        higher_priority.job_id,
        lower_priority.job_id,
    ]


async def test_priority_ordering_is_absolute_across_every_pair(
    ready_queue: ReadyQueue, redis: Redis
) -> None:
    """Enqueue descending by priority with a huge sequence jump between each."""
    expected = []
    for priority in (100, 99, 75, 50, 25, 1, 0):
        await redis.incrby(SEQUENCE_KEY, 10**15)
        expected.append((await ready_queue.enqueue(uuid.uuid4(), priority)).job_id)

    order = await ready_queue.peek_many(len(expected))

    assert [candidate.job_id for candidate in order] == expected


async def test_score_depends_only_on_priority(ready_queue: ReadyQueue) -> None:
    first = await ready_queue.enqueue(uuid.uuid4(), 40)
    second = await ready_queue.enqueue(uuid.uuid4(), 40)

    assert await ready_queue.score(first) == await ready_queue.score(second)
    assert await ready_queue.score(first) == entry_score(40)


async def test_entry_score_inverts_priority() -> None:
    assert entry_score(100) == 0
    assert entry_score(0) == MAX_PRIORITY
    assert entry_score(100) < entry_score(50) < entry_score(0)
    # Out-of-range values are clamped rather than corrupting the ordering.
    assert entry_score(500) == 0
    assert entry_score(-5) == MAX_PRIORITY


async def test_member_encodes_padded_sequence_and_job_id(ready_queue: ReadyQueue) -> None:
    job_id = uuid.uuid4()

    candidate = await ready_queue.enqueue(job_id, 10)

    sequence_text, _, job_id_text = candidate.member.partition(":")
    assert len(sequence_text) == SEQUENCE_WIDTH
    assert sequence_text == str(candidate.sequence).zfill(SEQUENCE_WIDTH)
    assert job_id_text == str(job_id)
    assert QueueCandidate.from_member(candidate.member) == candidate


async def test_padded_members_sort_lexicographically_in_numeric_order(
    ready_queue: ReadyQueue, redis: Redis
) -> None:
    """Padding is what makes lexicographic member order match numeric order."""
    first = await ready_queue.enqueue(uuid.uuid4(), 7)
    await redis.set(SEQUENCE_KEY, 10**15)
    second = await ready_queue.enqueue(uuid.uuid4(), 7)

    assert first.member < second.member
    order = await ready_queue.peek_many(2)
    assert [candidate.job_id for candidate in order] == [first.job_id, second.job_id]


async def test_remove_targets_the_observed_entry(ready_queue: ReadyQueue) -> None:
    candidate = await ready_queue.enqueue(uuid.uuid4(), 10)

    assert await ready_queue.size() == 1
    assert await ready_queue.remove(candidate) is True
    assert await ready_queue.remove(candidate) is False
    assert await ready_queue.size() == 0
    assert await ready_queue.peek() is None
    assert await ready_queue.current_member(candidate.job_id) is None


async def test_re_enqueue_replaces_the_previous_entry(ready_queue: ReadyQueue) -> None:
    job_id = uuid.uuid4()

    first = await ready_queue.enqueue(job_id, 10)
    second = await ready_queue.enqueue(job_id, 10)

    assert second.member != first.member
    assert await ready_queue.size() == 1
    assert await ready_queue.current_member(job_id) == second.member
    remaining = await ready_queue.peek()
    assert remaining is not None and remaining.member == second.member


async def test_re_enqueue_can_change_priority(ready_queue: ReadyQueue) -> None:
    job_id = uuid.uuid4()
    other = await ready_queue.enqueue(uuid.uuid4(), 50)

    await ready_queue.enqueue(job_id, 1)
    promoted = await ready_queue.enqueue(job_id, 100)

    assert await ready_queue.size() == 2
    order = await ready_queue.peek_many(2)
    assert [candidate.job_id for candidate in order] == [promoted.job_id, other.job_id]


async def test_removing_a_stale_candidate_leaves_the_new_entry_intact(
    ready_queue: ReadyQueue, redis: Redis
) -> None:
    """A worker's late cleanup must not discard a re-enqueued entry.

    The old member is put back into the sorted set to simulate the worst case,
    where a stale entry is still present when the delayed removal arrives. The
    removal must drop only the old token and must leave the job's current
    mapping pointing at the new one.
    """
    job_id = uuid.uuid4()
    stale = await ready_queue.enqueue(job_id, 10)
    current = await ready_queue.enqueue(job_id, 10)
    await redis.zadd(READY_QUEUE_KEY, {stale.member: entry_score(10)})
    assert await ready_queue.size() == 2

    assert await ready_queue.remove(stale) is True

    assert await ready_queue.size() == 1
    remaining = await ready_queue.peek()
    assert remaining is not None and remaining.member == current.member
    assert await ready_queue.current_member(job_id) == current.member


async def test_removing_a_stale_candidate_does_not_clear_a_newer_mapping(
    ready_queue: ReadyQueue, redis: Redis
) -> None:
    """Compare-and-delete: the mapping survives when it points elsewhere."""
    job_id = uuid.uuid4()
    stale = await ready_queue.enqueue(job_id, 10)
    current = await ready_queue.enqueue(job_id, 10)

    # The stale member is already gone, so this is purely the mapping check.
    assert await ready_queue.remove(stale) is False
    assert await redis.hget(ENTRIES_KEY, str(job_id)) == current.member
    assert await ready_queue.size() == 1


async def test_entry_mapping_is_cleaned_up_after_removal(
    ready_queue: ReadyQueue, redis: Redis
) -> None:
    """The companion hash must not grow once entries are consumed."""
    candidates = [await ready_queue.enqueue(uuid.uuid4(), 10) for _ in range(3)]

    for candidate in candidates:
        await ready_queue.remove(candidate)

    assert await ready_queue.size() == 0
    assert await redis.hlen(ENTRIES_KEY) == 0


async def test_distinct_jobs_at_equal_priority_keep_separate_entries(
    ready_queue: ReadyQueue,
) -> None:
    first = await ready_queue.enqueue(uuid.uuid4(), 10)
    second = await ready_queue.enqueue(uuid.uuid4(), 10)

    assert await ready_queue.size() == 2
    assert await ready_queue.current_member(first.job_id) == first.member
    assert await ready_queue.current_member(second.job_id) == second.member


async def test_malformed_member_is_rejected() -> None:
    with pytest.raises(ValueError, match="malformed queue member"):
        QueueCandidate.from_member("no-separator")
