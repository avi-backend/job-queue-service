# Job Queue Service

Distributed job queue service. PostgreSQL is the durable source of truth for job
state; Redis is used only as the ready-job delivery queue.

Author: Avi

> Status: Phase 3. Job submission, the Redis ready queue, atomic claiming and
> worker execution are implemented, and multiple workers can run concurrently.
> Retries, retry backoff, scheduled-job activation, crash recovery, heartbeat
> leases, job timeouts and cancellation are **not** implemented yet. A failed
> job stops at `failed` and is not retried.

## Requirements

- Docker and Docker Compose

## Running

```bash
docker compose up --build
```

The API is available on <http://localhost:8000>, with Swagger UI at
<http://localhost:8000/docs>.

A one-shot `migrate` service runs `alembic upgrade head` and must exit
successfully before the API and worker start. To run migrations manually:

```bash
docker compose run --rm migrate
```

## Configuration

Copy `.env.example` to `.env` to override defaults. Compose has working defaults
for local development, so a `.env` file is optional.

## Project layout

_To be completed._

## Architecture

PostgreSQL is the durable source of truth for job state. Redis holds a sorted set
(`job_queue:ready`) of job IDs that are ready to run, and is only an index:
appearing in it grants no ownership.

A worker owns a job only after a single conditional `UPDATE` moves it from
`pending` to `processing` in PostgreSQL. The pickup sequence is deliberately
non-destructive:

1. Read the best candidate with `ZRANGE` (no removal).
2. Attempt the atomic claim.
3. On success, remove the Redis entry and execute the handler.
4. On failure, discard the stale entry and move on without executing.

Popping from Redis before claiming would lose the job if the worker died in
between, since nothing would durably own it and no entry would remain.

Ready-queue ordering keeps priority and FIFO in separate places. The score is
`MAX_PRIORITY - priority` and nothing else, so priority ordering is absolute for
the lifetime of the system. FIFO within a priority comes from the member, which
Redis compares lexicographically when scores tie:

```text
0000000000000000123:<job-uuid>
```

The zero-padded 19-digit Redis `INCR` sequence makes lexicographic order match
numeric order. Mixing the sequence into the score instead would break the
priority invariant once the sequence grew past the priority band width.

That member is the entry token. Workers remove the exact token they observed,
and a small `job_queue:entries` hash maps each job to its current token so a
re-enqueue replaces the previous entry. Removal is compare-and-delete: the old
member is dropped by exact value and the mapping is cleared only if it still
points at that token, so a late cleanup cannot invalidate a newer entry.

Run several workers with:

```bash
docker compose up -d --scale worker=3
```

Correctness does not depend on the number of workers.

## API

Full request and response schemas are browsable in Swagger UI at
<http://localhost:8000/docs>.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/jobs` | Submit a job. `201` when created, `200` when an idempotency key replays an existing job. |
| `GET` | `/jobs/{job_id}` | Fetch one job. `404` if unknown. |
| `GET` | `/jobs` | List jobs, newest first. Filters: `status`, `type`. Pagination: `limit` (max 100, default 50), `offset`. |
| `GET` | `/health` | Liveness only; queue statistics come in a later phase. |

Job types are `email`, `webhook`, `report` and `batch`, each with a payload
validated against its own schema before the job is persisted. `priority` accepts
0-100 where higher runs sooner. A future timezone-aware `scheduled_at` creates
the job as `scheduled`; otherwise it is `pending`. `max_attempts` is fixed at 3
by the server.

Submitting with an `Idempotency-Key` header stores the key for 24 hours. Reusing
a live key returns the original job with `200` instead of creating a duplicate;
once the key expires it can be reused for a new job.

A new `pending` job is committed to PostgreSQL and then published to the ready
queue. A `scheduled` job is only persisted, and an idempotency replay never
enqueues again. If the Redis publish fails after the commit, the job stays
durably `pending` but invisible to workers until a reconciliation sweep (a later
phase) re-queues it; the failure is logged as `job_enqueue_failed`.

## Job execution

Handlers are mocks that simulate work with async sleeps: `email` returns a
message ID, `webhook` succeeds 80% of the time and otherwise fails, `report`
returns a file URL, and `batch` processes each item while reporting progress.

A successful job goes `pending` -> `processing` -> `completed`, storing its
result, `progress = 100` and `completed_at`. In this phase a failure goes
`pending` -> `processing` -> `failed`, storing the error. There is no retry yet,
so a failure is terminal even when attempts remain.

Workers respond to `SIGTERM`/`SIGINT` by stopping their polling loop. Waiting for
an in-flight job to finish before exiting is part of a later phase, together with
heartbeat leases and crash recovery.

## Testing

```bash
docker compose run --rm api pytest -v
```

Tests run against a real PostgreSQL instance, in a separate `jobqueue_test`
database that is created and migrated automatically. SQLite is deliberately not
used, because the behaviour under test (JSONB, the native status enum, and the
partial unique index behind idempotency) is PostgreSQL-specific. Tables are
truncated between tests, so the suite is repeatable.
