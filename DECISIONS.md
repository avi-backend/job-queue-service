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

## Phase 4A notes

- Execution is at-least-once. Nothing in this design prevents a stalled worker
  from having already had an external effect before losing its attempt, so the
  guarantee is narrower and honest: at most one attempt can write job state.
- Ownership is `(worker_id, execution_token)`, not `worker_id` alone. Once crash
  recovery exists, the same process can legitimately own two different attempts
  of the same job at different times, so the process identity cannot fence a
  stale write. The token is a fresh UUID per claim and is never reused.
- Fenced writes return "zero rows" rather than raising in SQL, so the service
  layer converts that into an explicit `OwnershipLostError`. Silently treating a
  no-op update as success is the failure mode this phase exists to remove.
- Ownership is passed around as an `Attempt` object instead of a job id. A
  signature that demands the whole attempt makes it impossible to write an
  unfenced update by accident, which is what section 14 of the brief warns about
  for progress updates.
- `started_at` is restamped on each claim. Keeping the first attempt's timestamp
  was considered, but lease and heartbeat reasoning must never depend on a value
  belonging to an attempt that no longer exists, and the current attempt's
  runtime is the more useful operational number.
- Lease deadlines are computed as `now() + interval` by PostgreSQL, never from a
  worker's clock. Recovery in another process compares against the database
  clock, so a skewed worker must not be able to grant itself a different lease
  than it appears to hold.
- All ownership fields are cleared whenever a job leaves `processing`, including
  on success. A uniform rule ("these fields describe a live attempt only") is
  easier to reason about than keeping `worker_id` as an audit trail on some
  terminal states but not others; who ran an attempt is in the structured logs.
- A recovered attempt is treated as a failed attempt because `attempt_count` was
  already consumed at claim time. Giving the attempt back would let a job that
  reliably kills its worker retry forever.
- Retries are left `scheduled`, not enqueued with a delay. PostgreSQL stays the
  only thing that decides when a job is runnable, and user-scheduled jobs and
  retries then travel one code path. A Redis delayed-set design was rejected:
  it would put schedule state in the index and need its own reconciliation.
- The scheduler commits `scheduled -> pending` before enqueueing, and a failed
  enqueue is logged rather than rolled back. Reverting after a Redis timeout of
  unknown outcome risks a double enqueue as much as a lost one; the honest fix
  is reconciliation that checks the queue's entry mapping, which is deferred.
- Recovery selects with `FOR UPDATE SKIP LOCKED` *and* re-proves both the fence
  and the expiry in the `UPDATE`. The lock alone would be enough for concurrent
  recovery loops, but not for a heartbeat that lands between read and write.
- Migration 0002 cannot leave Phase 3 PROCESSING rows for crash recovery.
  Those rows have `lease_expires_at IS NULL`, and `NULL < now()` is unknown, so
  the recovery predicate never matches. The upgrade therefore moves pre-token
  PROCESSING rows to SCHEDULED with `scheduled_at = now()`, clears ownership,
  and preserves `attempt_count`. The scheduler activates them under a fenced
  claim. Downgrade drops the column only; putting the rows back to PROCESSING
  would re-strand them.

### AI suggestions that failed under analysis

- Suggested: activate due jobs with
  `UPDATE ... WHERE id IN (SELECT id ... LIMIT :batch FOR UPDATE SKIP LOCKED)`.
  It reads correctly and passed a single-row test, but PostgreSQL cannot hash a
  subquery carrying `FOR UPDATE`, so the sub-plan is re-evaluated per candidate
  row. Each activation changes which rows are still due, the next evaluation
  returns a different set, and the batch limit silently stops holding: a
  `batch_size=2` sweep activated all five due jobs. Rewritten as a CTE, which
  PostgreSQL evaluates once. Caught by `test_activation_respects_the_batch_size`.
- Suggested: have the heartbeat cancel the handler task directly. Rejected
  because the heartbeat would then have to own the handler's lifecycle; the
  runner races the handler against an `ownership_lost` event instead, and the
  heartbeat's only job stays "extend or report".
- Suggested: keep the heartbeat running while completion is written, since both
  are fenced anyway. Rejected: a beat arriving after completion updates zero
  rows and would log a spurious ownership loss. The heartbeat is stopped and
  awaited before the job is settled, so the two can never interleave.
- Suggested: treat a failed heartbeat write as lost ownership. Rejected: a
  transient database error is not a statement about ownership. Only a fenced
  update that matched zero rows proves ownership is gone; a blip is logged and
  retried, and if the failures persist the lease lapses and recovery decides.
- Suggested (test): simulate a crash by expiring the lease in one transaction
  and recovering in the next. Wrong whenever the owner's heartbeat is live: the
  beat re-extends the lease in the gap and recovery correctly finds nothing.
  The tests do both under one transaction so the row lock covers the pair.
- Suggested: adding `execution_token` is enough because "existing PROCESSING
  jobs will be recovered once the lease expires". False. Phase 3 never wrote a
  lease, `NULL < now()` is unknown, and recovery would leave those rows stuck.
  The upgrade has to release them itself. Caught before commit by reviewing
  the 0002 data step against the recovery predicate.
