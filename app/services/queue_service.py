"""Redis ready queue.

Only PENDING jobs belong here, and membership grants no ownership: a worker owns
a job only after the atomic PENDING -> PROCESSING transition in PostgreSQL. The
queue is an index that tells workers where to look, so a lost or stale entry
costs a delay, never correctness.

Ordering
--------
The sorted set is ordered ascending and the score encodes priority alone:

    score = MAX_PRIORITY - priority

Priority 100 therefore always scores 0 and priority 0 always scores 100, so
priority ordering is absolute for the lifetime of the system. Nothing else is
mixed into the score, which is what keeps the invariant from decaying: an
unbounded sequence added to the score would eventually exceed any fixed priority
band and let an old low-priority job sort ahead of a new high-priority one.

FIFO within a priority comes from the member instead. Redis breaks score ties by
comparing members lexicographically, so each member carries a zero-padded
monotonic Redis sequence ahead of the job id:

    0000000000000000123:<job-uuid>

19 digits covers the whole range of a 64-bit INCR counter, so the padding never
overflows into a shorter string and lexicographic order always matches numeric
order.

Entry tokens
------------
The member is the entry token, and a worker removes the exact token it observed.
A companion hash maps job_id -> current token so a re-enqueue can replace a
job's previous entry, and removal is compare-and-delete: the old member is
removed by exact value, and the mapping is cleared only if it still points at
that same token. A delayed cleanup of an old candidate therefore cannot remove
or invalidate a newer entry for the same job.

The multi-key operations run as Lua scripts, which Redis executes without
interleaving other clients' commands. That is atomicity with respect to
interleaving; it is not transactional rollback, and a runtime error partway
through a script leaves earlier writes in place.
"""

import uuid
from dataclasses import dataclass

from redis.asyncio import Redis

READY_QUEUE_KEY = "job_queue:ready"
SEQUENCE_KEY = "job_queue:sequence"
ENTRIES_KEY = "job_queue:entries"

MAX_PRIORITY = 100

#: Width of the zero-padded sequence in a member, sized for a 64-bit counter.
SEQUENCE_WIDTH = 19


@dataclass(frozen=True, slots=True)
class QueueCandidate:
    """A specific queue entry, as observed by a worker."""

    job_id: uuid.UUID
    member: str
    sequence: int

    @classmethod
    def from_member(cls, member: str) -> "QueueCandidate":
        sequence_text, separator, job_id_text = member.partition(":")
        if not separator:
            raise ValueError(f"malformed queue member: {member!r}")
        return cls(
            job_id=uuid.UUID(job_id_text),
            member=member,
            sequence=int(sequence_text),
        )


def entry_score(priority: int) -> int:
    """Score for a priority. Higher priority yields a lower score."""
    clamped = max(0, min(MAX_PRIORITY, priority))
    return MAX_PRIORITY - clamped


# Replaces any existing entry for the job, so a job is queued at most once.
_ENQUEUE_SCRIPT = """
local ready_key = KEYS[1]
local sequence_key = KEYS[2]
local entries_key = KEYS[3]
local score = tonumber(ARGV[1])
local job_id = ARGV[2]
local width = tonumber(ARGV[3])

local sequence = redis.call('INCR', sequence_key)
local member = string.format('%0' .. width .. 'd', sequence) .. ':' .. job_id

local previous = redis.call('HGET', entries_key, job_id)
if previous then
    redis.call('ZREM', ready_key, previous)
end

redis.call('ZADD', ready_key, score, member)
redis.call('HSET', entries_key, job_id, member)
return member
"""

# Compare-and-delete: drop the exact member, and only clear the job's mapping
# when it still points at that member.
_REMOVE_SCRIPT = """
local ready_key = KEYS[1]
local entries_key = KEYS[2]
local member = ARGV[1]
local job_id = ARGV[2]

local removed = redis.call('ZREM', ready_key, member)
if redis.call('HGET', entries_key, job_id) == member then
    redis.call('HDEL', entries_key, job_id)
end
return removed
"""


class ReadyQueue:
    """Thin wrapper so no other module issues raw Redis commands."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._enqueue_script = redis.register_script(_ENQUEUE_SCRIPT)
        self._remove_script = redis.register_script(_REMOVE_SCRIPT)

    async def enqueue(self, job_id: uuid.UUID, priority: int) -> QueueCandidate:
        """Add a ready job, replacing any entry it already had."""
        member = await self._enqueue_script(
            keys=[READY_QUEUE_KEY, SEQUENCE_KEY, ENTRIES_KEY],
            args=[entry_score(priority), str(job_id), SEQUENCE_WIDTH],
        )
        return QueueCandidate.from_member(member)

    async def peek(self) -> QueueCandidate | None:
        """Read the next candidate without removing it.

        Non-destructive on purpose: a worker that dies between reading and
        claiming must not take the job with it.
        """
        candidates = await self.peek_many(1)
        return candidates[0] if candidates else None

    async def peek_many(self, count: int) -> list[QueueCandidate]:
        members = await self._redis.zrange(READY_QUEUE_KEY, 0, count - 1)
        return [QueueCandidate.from_member(member) for member in members]

    async def remove(self, candidate: QueueCandidate) -> bool:
        """Remove exactly the observed entry. True when this call removed it."""
        removed = await self._remove_script(
            keys=[READY_QUEUE_KEY, ENTRIES_KEY],
            args=[candidate.member, str(candidate.job_id)],
        )
        return bool(removed)

    async def remove_current(self, job_id: uuid.UUID) -> bool:
        """Remove the job's current queue entry, if it has one.

        Looks up the token in the entry mapping and then compare-and-deletes
        that exact member. A concurrent re-enqueue replaces the mapping first,
        so this cannot delete a newer entry for the same job.
        """
        member = await self.current_member(job_id)
        if member is None:
            return False
        return await self.remove(QueueCandidate.from_member(member))

    async def size(self) -> int:
        return int(await self._redis.zcard(READY_QUEUE_KEY))

    async def current_member(self, job_id: uuid.UUID) -> str | None:
        """The token a job is currently queued under, if any."""
        return await self._redis.hget(ENTRIES_KEY, str(job_id))

    async def score(self, candidate: QueueCandidate) -> float | None:
        return await self._redis.zscore(READY_QUEUE_KEY, candidate.member)
