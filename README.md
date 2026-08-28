# Job Queue Service

A distributed job queue. Clients submit work through an HTTP API; worker
processes claim and execute it. PostgreSQL is the durable source of truth for
every job. Redis is only a ready-work index: appearing in it grants no
ownership.

Execution is **at-least-once**. A stalled or killed worker may already have had
an external effect before its attempt is recovered. The service guarantees that
only one attempt can write job state at a time, not that handlers run exactly
once.

Author: Avi

## Architecture

```
Client ──► FastAPI ──► PostgreSQL (job state)
                │
                └──► Redis ZSET (ready index)

Worker process (N replicas)
  ├── runner      peek Redis → atomic PENDING→PROCESSING claim → execute
  ├── scheduler   due SCHEDULED → PENDING → enqueue
  └── recovery    expired PROCESSING leases → retry or fail
```

A worker owns a job only after a single conditional `UPDATE` moves it from
`pending` to `processing`. Pickup is peek-then-claim: the Redis entry is read
without removing it, the database claim is attempted, and only then is the
observed queue token deleted. Popping Redis first would lose the job if the
worker died before durable ownership existed.

Ready-queue ordering keeps priority and FIFO in separate places. The score is
`100 - priority` and nothing else, so higher priority always sorts first. FIFO
within a priority comes from a zero-padded sequence in the member
(`0000000000000000123:<job-uuid>`), which Redis compares lexicographically when
scores tie. Workers remove the exact token they observed, so a late cleanup
cannot delete a newer entry for the same job.

## Technology stack

| Piece | Choice |
| --- | --- |
| Language | Python 3.12 |
| API | FastAPI |
| Database | PostgreSQL 16 |
| Ready index | Redis 7 sorted set |
| Migrations | Alembic |
| Workers | Separate process, same image |
| Runtime | Docker Compose |

## Run from a fresh clone

Compose defaults are enough. A local `.env` is optional.

```bash
git clone https://github.com/avi-backend/job-queue-service.git
cd job-queue-service
docker compose up --build -d
```

Wait until the API is healthy, then:

```bash
curl -s http://localhost:8000/
curl -s http://localhost:8000/health
```

Swagger UI is at <http://localhost:8000/docs>.

Stop everything with `docker compose down`. Add `-v` only if you also want to
discard the database volume.

## Migrations

A one-shot `migrate` service runs `alembic upgrade head` and must exit
successfully before the API and worker start. That keeps a single schema writer
and lets the worker start independently of API health.

```bash
docker compose run --rm migrate
```

Revision `0002` adds `execution_token` and releases any pre-token `processing`
row that has no lease. Those rows cannot be recovered (`NULL < now()` is
unknown), so the upgrade moves them to `scheduled` with `scheduled_at = now()`
and preserves `attempt_count`.

## Tests

```bash
docker compose run --rm api pytest -v
```

Tests use real PostgreSQL and Redis (a dedicated `jobqueue_test` database and
Redis index 15). SQLite is not used: JSONB, the native status enum, partial
unique indexes, `FOR UPDATE SKIP LOCKED`, and Redis Lua semantics are what the
suite is proving. Tables are truncated between tests.

## Submit a job

```bash
curl -s http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"email","payload":{"to":"user@example.com","subject":"Hello"}}'
```

Job types: `email`, `webhook`, `report`, `batch`. Each payload is validated
before persist. `priority` is 0–100 (higher runs sooner). `max_attempts` is
fixed at 3 by the server.

Idempotent submit — reuse of a live key returns the original job with `200`:

```bash
curl -s http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"type":"email","payload":{"to":"user@example.com","subject":"Hello"}}'
```

## Inspect, list, cancel, retry

```bash
# one job
curl -s http://localhost:8000/jobs/<job-id>

# newest first; optional filters
curl -s 'http://localhost:8000/jobs?status=pending&limit=20'
curl -s 'http://localhost:8000/jobs?type=email&offset=0&limit=50'

# cancel pending or scheduled (already-cancelled is idempotent 200)
curl -s -X POST http://localhost:8000/jobs/<job-id>/cancel

# new three-attempt cycle for a failed job (attempt_count returns to 0)
curl -s -X POST http://localhost:8000/jobs/<job-id>/retry
```

`processing`, `completed`, and `failed` cannot be cancelled (`409`). Only
`failed` can be retried (`409` otherwise). Unknown id: `404`.

## Scheduled jobs

A future timezone-aware `scheduled_at` creates the job as `scheduled`. It is
not enqueued until the scheduler sees `scheduled_at <= now()` and atomically
moves it to `pending`.

```bash
curl -s http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"email","payload":{"to":"user@example.com","subject":"Later"},"scheduled_at":"2030-01-01T00:00:00Z"}'
```

The same `scheduled` state is used for delayed retries.

## Priority and idempotency

Higher `priority` is always served first, regardless of how many jobs have
already been enqueued. Equal priority is FIFO by **enqueue order**, not by
`created_at`. Two commits could in principle be published to Redis in the
opposite order.

`Idempotency-Key` is stored for 24 hours. The partial unique index is the
concurrency guarantee; the application releases an expired key with a
conditional `UPDATE` that re-checks expiry. A replay never enqueues again.

## Retry timing

| After | Next step |
| --- | --- |
| attempt 1 fails | retry in 30 seconds |
| attempt 2 fails | retry in 120 seconds |
| attempt 3 fails | `failed` permanently |

Attempt 1 is immediate (the first claim). Nothing sleeps for the backoff: the
delay is `scheduled_at`. Manual retry of a `failed` job starts a **new** cycle
with `attempt_count = 0`.

## Crash recovery and fencing

Each claim mints a new `execution_token` and a lease
(`lease_expires_at = now() + JOB_LEASE_SECONDS`). While the handler runs, a
heartbeat extends that lease. If the owner dies, `lease_expires_at < now()`
lets recovery treat the attempt as failed: retry with the same backoff, or
`failed` with `worker lease expired` when attempts are exhausted.

Every owner write (heartbeat, progress, complete, fail) is:

```sql
WHERE id = :job_id AND status = 'processing'
  AND worker_id = :worker_id AND execution_token = :execution_token
```

Zero rows means the attempt was taken away. A stale worker cannot complete or
update a job that recovery has already handed to someone else.

Short-lease demo (defaults stay 60s / 20s in code):

```bash
JOB_LEASE_SECONDS=10 JOB_HEARTBEAT_SECONDS=3 RECOVERY_INTERVAL_SECONDS=2 \
  docker compose up -d --build
```

## Graceful shutdown and multiple workers

`SIGTERM`/`SIGINT` stop new claims and new scheduler/recovery sweeps. An
attempt this process already owns is drained with its heartbeat still running,
then the process exits. Compose `stop_grace_period` is two minutes. `SIGKILL`,
or a grace period that expires, is the crash path.

```bash
docker compose up -d --scale worker=3
```

Correctness does not depend on the number of workers. Atomic database
transitions are the only coordination.

## Batch progress

A `batch` job reports progress while it runs (`0–100`). Updates are fenced by
`worker_id` and `execution_token`, so a recovered attempt cannot overwrite a
newer owner's progress.

```bash
curl -s http://localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"batch","payload":{"items":[{"i":1},{"i":2},{"i":3}]}}'
```

## Health

```bash
curl -s http://localhost:8000/health
```

```json
{
  "status": "healthy",
  "database": "healthy",
  "redis": "healthy",
  "queue": {
    "ready": 5,
    "pending": 5,
    "scheduled": 2,
    "processing": 1,
    "completed": 20,
    "failed": 3,
    "cancelled": 1
  }
}
```

`ready` is Redis; the rest are PostgreSQL `GROUP BY` counts. They can differ.
That mismatch is the enqueue window below, not an outage, and does not mark
the service unhealthy. Either store unreachable returns `503`.

## Configuration

Copy `.env.example` to `.env` only to override defaults.

| Variable | Default | Meaning |
| --- | --- | --- |
| `JOB_LEASE_SECONDS` | `60` | Claim ownership before recovery may take it |
| `JOB_HEARTBEAT_SECONDS` | `20` | Must be smaller than the lease |
| `SCHEDULER_INTERVAL_SECONDS` | `1` | Due-schedule poll |
| `RECOVERY_INTERVAL_SECONDS` | `5` | Expired-lease poll |
| `WORKER_POLL_INTERVAL_SECONDS` | `1` | Idle ready-queue poll |

## Known limitations

- Redis is not authoritative. A job is runnable only if PostgreSQL says so.
- If PostgreSQL commits `pending` and Redis enqueue then fails, the job is
  durable but invisible to workers until something re-queues it. `/health`
  shows `pending` vs `ready`. This is not closed with a two-store transaction.
- Handlers are mocks (sleep + a result). External side effects are
  at-least-once; they are not recalled on retry or recovery.
- `processing` jobs cannot be cancelled.
- There is no dead-letter queue, hard execution timeout, or authentication.

## Requirements coverage

| Requirement | Where | Tests |
| --- | --- | --- |
| Python 3.11+ | `Dockerfile` (`python:3.12-slim`) | image build |
| Web framework | FastAPI in `app/main.py`, `app/api/jobs.py` | `tests/test_jobs_api.py` |
| Relational persistence | PostgreSQL + SQLAlchemy + Alembic | suite (real Postgres) |
| Queue/cache | Redis ZSET in `app/services/queue_service.py` | `tests/test_ready_queue.py` |
| Separate worker | `worker/main.py`, Compose `worker` service | `tests/test_worker.py` |
| 3 attempts, 30s / 120s backoff | `app/services/retry_policy.py` | `tests/test_retry.py` |
| Priority processing | score `100 - priority` | `tests/test_ready_queue.py`, `tests/test_worker.py` |
| Cancellation | `POST /jobs/{id}/cancel` | `tests/test_cancel.py` |
| Docker Compose | `docker-compose.yml` | documented cold start |
| Submit / retrieve | `POST/GET /jobs` | `tests/test_jobs_api.py` |
| Completion | claim → handler → `completed` | `tests/test_worker.py`, `tests/test_claim.py` |
| Failure / retry | `processing → scheduled` then fail | `tests/test_retry.py`, `tests/test_worker.py` |
| Idempotency | `Idempotency-Key`, partial unique index | `tests/test_idempotency.py` |
| Scheduled jobs | `scheduled_at`, scheduler loop | `tests/test_scheduler.py` |
| Crash recovery | lease + `worker/recovery.py` | `tests/test_recovery.py`, `tests/test_fencing.py` |
| JSON logs with job context | `app/core/logging.py` | worker events (`job_id`, token, attempt) |
| Graceful drain | SIGTERM finishes the owned attempt | `tests/test_shutdown.py` |
| Health + queue stats | `GET /health` | `tests/test_health.py` |
| Multiple workers | `--scale worker=N` | `tests/test_worker.py`, `tests/test_claim.py` |
| Batch progress | fenced `progress` updates | `tests/test_worker.py`, `tests/test_handlers.py` |

Not implemented (and not claimed): dead-letter queue, hard job timeout,
authentication, frontend.

Design rationale is in `DECISIONS.md`. How AI was used is in `AI_USAGE.md`.
