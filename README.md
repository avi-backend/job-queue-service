# Job Queue Service

Distributed job queue service. PostgreSQL is the durable source of truth for job
state; Redis is used only as the ready-job delivery queue.

Author: Avi

> Status: Phase 2. Job submission, retrieval, listing and idempotency are
> implemented. Jobs are persisted to PostgreSQL as `pending` or `scheduled` and
> nothing executes them yet: the Redis ready queue, worker execution loop,
> scheduler, retries and crash recovery come in later phases.

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

_To be completed._

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

## Testing

```bash
docker compose run --rm api pytest -v
```

Tests run against a real PostgreSQL instance, in a separate `jobqueue_test`
database that is created and migrated automatically. SQLite is deliberately not
used, because the behaviour under test (JSONB, the native status enum, and the
partial unique index behind idempotency) is PostgreSQL-specific. Tables are
truncated between tests, so the suite is repeatable.
