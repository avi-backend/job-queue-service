# Design Decisions

_Placeholder. Detailed rationale is added as the phases progress._

## Source of truth

_To be completed._

## Job claiming and concurrency

_To be completed._

## Retries and backoff

_To be completed._

## Worker crash recovery

_To be completed._

## Idempotency

_To be completed._

## Trade-offs and known limitations

_To be completed._

## Phase 1 notes

- Job status values are persisted as a native PostgreSQL enum (`job_status`)
  using lowercase values.
- The unique idempotency index is partial (`idempotency_key IS NOT NULL`); the
  24-hour window is enforced in the application layer by releasing an expired
  key, while the database guarantees uniqueness under concurrency.
- Migrations are applied by a one-shot `migrate` compose service that must exit
  successfully before the API and worker start. This keeps a single schema
  writer and lets the worker start independently of API health.

## Phase 2 notes

- Job type is validated by a `JobType` enum in the application layer while the
  column stays `varchar`, so adding a job type needs no migration.
- Each job type has its own Pydantic payload model with `extra="forbid"`, and
  the validated payload is stored in canonical form so workers can rely on a
  predictable shape.
- Idempotent submission checks for a live key first, but correctness does not
  depend on that check: the partial unique index is the guarantee, and a lost
  race is resolved by rolling back and reloading the winning row.
- An expired key is released with a conditional `UPDATE` that re-checks expiry,
  so a concurrent request can never steal a key that is still live.
- Listing orders by `created_at DESC, id DESC`. The `id` tiebreaker keeps
  pagination stable when several jobs share a `created_at` value.

## Phase 3 notes

- Redis membership is an index, never ownership. The atomic
  `UPDATE ... WHERE status = 'pending' RETURNING` is the only concurrency
  boundary, so several workers may see one candidate and exactly one executes it.
- Pickup is peek-then-claim, not pop-then-claim. Popping first would lose the job
  if the worker died before durable ownership existed.
- Priority and FIFO are kept in separate dimensions. The score is
  `MAX_PRIORITY - priority` alone, which makes priority ordering absolute, and
  FIFO comes from a zero-padded `INCR` sequence inside the member, which Redis
  compares lexicographically when scores tie. An earlier design added the
  sequence into the score; that was rejected because an unbounded sequence
  eventually exceeds any fixed priority band and inverts priority.
- The member doubles as the entry token. Workers remove the exact token they
  observed, and a `job_queue:entries` hash maps job_id to its current token so a
  re-enqueue replaces the old entry. Removal is compare-and-delete, so a delayed
  cleanup of a stale candidate cannot remove or invalidate a newer entry for the
  same job. This is what makes Phase 4 re-enqueueing safe.
- Multi-key queue operations run as Lua scripts. Redis runs a script without
  interleaving other clients, which is the property needed here; it is not
  transactional rollback, and a runtime error mid-script leaves earlier writes
  applied.
- FIFO is by enqueue order. Two submissions committed in one order could in
  principle be enqueued in the other, so same-priority ordering is by arrival at
  the queue rather than by `created_at`.
- The claim also requires `attempt_count < max_attempts`, so it can never push a
  job past the Phase 1 check constraint.
- A worker that fails to claim removes the stale entry only when the job is no
  longer claimable. Once retries re-enqueue jobs (Phase 4) this removal will need
  a per-entry token, otherwise a late `ZREM` could delete a freshly re-queued
  entry.
- ORM `UPDATE ... RETURNING` needs `populate_existing=True`, or it returns the
  stale instance already held in the session's identity map.
- Handlers receive sleep, randomness and the progress callback through the
  execution context, which keeps database access out of handlers and lets tests
  run instantly and deterministically.
