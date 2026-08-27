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
