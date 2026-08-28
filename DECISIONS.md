# Design Decisions

This document is the interview-facing rationale. Implementation details live in
the code; this file records *why*.

## PostgreSQL is the source of truth

Every job state that matters is a row in PostgreSQL. Redis never grants
ownership. A worker owns a job only after a conditional `UPDATE` returns a
row. If Redis is empty, wrong, or down, the worst case is delay, not a
corrupted lifecycle.

## Redis is not authoritative

Redis holds a sorted set of ready candidates and a small `job_id → entry token`
map. Membership means "worth looking at", not "yours". Claim still requires
`status = pending`. Stale entries are discarded when the row is no longer
claimable.

**Trade-off A.** Operators must not assume `ZCARD == COUNT(*) FILTER (pending)`.
`/health` reports both numbers.

**Trade-off B.** Submit, scheduler, and manual retry all commit PostgreSQL
*then* enqueue. If Redis fails after the commit, the job is durably `pending`
and invisible to workers. Rolling the row back after a Redis timeout of
unknown outcome can double-enqueue just as easily as it can "fix" a miss.
There is no outbox and no reconciliation sweep. The window is logged and
visible on `/health`. It is not solved.

## Job pickup strategy

Peek, then claim, then delete the **observed** queue token.

1. `ZRANGE` the best candidate (non-destructive).
2. `UPDATE ... WHERE id = :id AND status = 'pending' AND attempt_count < max_attempts`.
3. On success, `ZREM` that exact member and compare-and-delete the mapping.
4. On failure, remove the member only if the job is no longer claimable.

Pop-then-claim was rejected: a worker that dies after `ZPOP` and before the
`UPDATE` takes the only record of readiness with it, and nothing durably owns
the job.

Several workers may see the same candidate. Exactly one `UPDATE` wins.

## Priority queue implementation

Score is `100 - priority` and **only** that. FIFO within a priority is the
zero-padded `INCR` sequence inside the member. Redis breaks score ties
lexicographically, so padding makes string order match numeric order.

An earlier formula mixed sequence into the score
(`(100 - priority) * 10^12 + sequence`). An unbounded sequence eventually
exceeds any fixed priority band and inverts priority. Separating the
dimensions keeps the invariant for the life of the system.

**Trade-off E.** FIFO is enqueue-order FIFO, not `created_at` order. Two
commits can be published to Redis in the opposite sequence.

Removal is always by exact member token. A delayed cleanup of an old
candidate cannot delete a retry's new entry.

Lua is used so the multi-key enqueue/remove is not interleaved with other
clients. That is interleaving atomicity, not transactional rollback.

## Retry backoff strategy

Three attempts, delays chosen from the attempt that just failed (already
incremented at claim):

| Failed attempt | Wait |
| --- | --- |
| 1 | 30 seconds |
| 2 | 120 seconds |
| 3 | none — `failed` |

Retries are written as `scheduled` with `scheduled_at = now() + delay`, not
pushed into a Redis delayed set. User-scheduled jobs and retries then share
one activation path, and PostgreSQL remains the only clock that decides
when work is runnable.

**Trade-off F.** `POST /jobs/{id}/retry` resets `attempt_count` to 0 and
starts a new cycle. Keeping the exhausted count would violate
`attempt_count <= max_attempts` on the next claim, or force raising
`max_attempts`. Earlier attempts remain in structured logs.

## Worker crash recovery

A claim sets `lease_expires_at = now() + JOB_LEASE_SECONDS` using the
**database** clock. A heartbeat extends that lease while the handler runs.
Recovery selects `processing AND lease_expires_at < now()` with
`FOR UPDATE SKIP LOCKED`, then the `UPDATE` re-proves both the fence and the
expiry so a late heartbeat keeps the job.

An expired lease is a failed attempt: `attempt_count` was already consumed.
Recovery applies the same 30s / 120s policy, or fails with
`worker lease expired`.

A `NULL` lease is not expired (`NULL < now()` is unknown). Upgrade `0002`
therefore moves pre-token `processing` rows to `scheduled` instead of waiting
for recovery that would never match.

## Execution fencing token

**Trade-off D.** After recovery exists, `worker_id` is not enough: the same
process can own two different attempts of one job at different times. Each
claim mints a new `execution_token`. Heartbeat, progress, complete, and fail
all require `worker_id` and `execution_token`. A stale worker updates zero
rows.

## Scheduled jobs

One state, two sources: a user-supplied future `scheduled_at`, and a retry
backoff. The scheduler locks due rows (`FOR UPDATE SKIP LOCKED` via a CTE so
the batch limit is evaluated once), moves them to `pending`, then enqueues.
Several scheduler loops cannot activate the same row twice.

## Cancellation

One conditional `UPDATE`: `pending` or `scheduled` → `cancelled`. That is the
race against claim. `processing` is not cancelled; the owner finishes or the
lease expires. Already-cancelled is idempotent (`200`) so a retried client
request is not told that a successful cancel failed.

Redis cleanup after cancel uses the current entry token. If it fails, the
row is still `cancelled` and cannot be claimed.

## Graceful shutdown

`SIGTERM`/`SIGINT` stop new claims and new scheduler/recovery sweeps. The
owned attempt is drained with its heartbeat still running. Cancelling the
handler on SIGTERM would abandon work the process can still finish and would
conflate process shutdown with ownership loss.

**Trade-off G.** Compose `stop_grace_period` is two minutes. That is a Docker
wait, not an application timeout. When it expires, Docker sends `SIGKILL` and
lease recovery takes over, same as any other crash.

## Idempotency

`Idempotency-Key` is unique while not null (partial index) and remembered for
24 hours in the application. The read-then-insert is a fast path; a lost race
rolls back and reloads the winner. Expired keys are released by a conditional
`UPDATE` that re-checks expiry, so a concurrent request cannot steal a live
key.

## At-least-once semantics

**Trade-off C.** Nothing prevents a handler from having already sent a webhook
or written a file before its lease expires. Recovery will run the job again.
The fencing token only protects **job state** in PostgreSQL. Exactly-once
external side effects are not claimed and are not implemented.

## One thing I would do differently with more time

Close the PostgreSQL-committed / Redis-not-enqueued window with a small
reconciliation sweep: `pending` rows whose `job_id` is absent from the Redis
entry map get a **new** tokenized enqueue. That must check the mapping, not
blindly `ZADD`, or FIFO is destroyed and already-queued jobs are duplicated.

I would not add a second message broker, a DLQ product, or a distributed
transaction across the two stores. The missing piece is a careful, idempotent
re-publish — not a new architecture.
