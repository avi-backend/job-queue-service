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
