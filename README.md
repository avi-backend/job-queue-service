# Job Queue Service

Distributed job queue service. PostgreSQL is the durable source of truth for job
state; Redis is used only as the ready-job delivery queue.

Author: Avi

> Status: Phase 4B. Submission, claiming, fencing, leases, retries, scheduling,
> crash recovery, cancellation of pending/scheduled jobs, manual retry of failed
> jobs, graceful worker drain and `/health` queue statistics are implemented.
> A dead letter queue, hard execution timeouts and authentication are **not**
> implemented.

## Requirements

- Docker and Docker Compose

## Running

```bash
docker compose up --build
```

The API is available on <http://localhost:8000>, with Swagger UI at
<http://localhost:8000/docs>.

A one-shot `migrate` service runs `alembic upgrade head` and must exit
successfully before the API and worker start. Revision `0002` adds the fencing
token and also releases any Phase 3 `processing` row that has no token or lease:
those rows cannot be recovered (`NULL < now()` is unknown), so the upgrade
moves them to `scheduled` with `scheduled_at = now()` and preserves
`attempt_count`. To run migrations manually:

```bash
docker compose run --rm migrate
```

## Configuration

Copy `.env.example` to `.env` to override defaults. Compose has working defaults
for local development, so a `.env` file is optional.

Distributed-safety timings:

| Variable | Default | Meaning |
| --- | --- | --- |
| `JOB_LEASE_SECONDS` | `60` | How long a claim owns a job before recovery may take it |
| `JOB_HEARTBEAT_SECONDS` | `20` | Lease extension interval; must be smaller than the lease |
| `SCHEDULER_INTERVAL_SECONDS` | `1` | How often each worker looks for due scheduled jobs |
| `SCHEDULER_BATCH_SIZE` | `100` | Maximum jobs activated per scheduler pass |
| `RECOVERY_INTERVAL_SECONDS` | `5` | How often each worker looks for expired leases |
| `RECOVERY_BATCH_SIZE` | `100` | Maximum jobs recovered per pass |
| `WORKER_POLL_INTERVAL_SECONDS` | `1` | Idle wait before polling the ready queue again |

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

## Execution safety

Execution is **at-least-once**, not exactly-once. A job whose worker stalls or
dies is retried, and the first attempt may already have had an effect on the
outside world that nothing can recall. What the service does guarantee is that
only one attempt can ever *write job state*, which is what keeps the database
consistent while allowing recovery.

### Ownership, leases and fencing

Each worker process has a `worker_id`. Each successful claim also mints a random
`execution_token` and takes a lease:

```text
status = processing, worker_id, execution_token = <new uuid>,
lease_expires_at = now() + JOB_LEASE_SECONDS, started_at = now(),
attempt_count = attempt_count + 1
```

`worker_id` says which process owns the job; `execution_token` says which
*ownership generation*. Every later write by that worker is conditional on all
four of job id, `status = processing`, `worker_id` and `execution_token`:

```sql
WHERE id = :job_id AND status = 'processing'
  AND worker_id = :worker_id AND execution_token = :execution_token
```

Zero updated rows means the attempt was taken away, and the worker raises
`OwnershipLostError` instead of assuming the write landed. This applies to the
heartbeat, progress updates, completion and failure alike, so the classic race
is closed: worker A stalls, its lease expires, recovery releases the job, worker
B claims it with a new token, and A's late completion updates nothing.

`started_at` is restamped on every claim, so it describes the attempt currently
running rather than an attempt that already failed. Lease logic never reads it.

### Heartbeat

While a handler runs, a background heartbeat pushes `lease_expires_at` forward
every `JOB_HEARTBEAT_SECONDS`, using the same fenced update. The heartbeat
interval must be smaller than the lease, and configuration refuses to start
otherwise. If a beat finds the attempt gone it logs
`job_heartbeat_ownership_lost` and the worker cancels the handler coroutine;
external side effects already in flight cannot be cancelled.

### Retry policy

Three attempts, with fixed backoff (`app/services/retry_policy.py`):

| Failure after | Next attempt waits |
| --- | --- |
| attempt 1 | 30 seconds |
| attempt 2 | 120 seconds |
| attempt 3 | never; the job is `failed` |

A failure with attempts remaining moves `processing -> scheduled` with
`scheduled_at = now() + backoff` and clears the ownership fields. The final
failure moves `processing -> failed` and sets `completed_at`. Nothing sleeps for
the backoff: the delay lives in `scheduled_at`.

### Scheduler and recovery loops

Every worker process runs three loops concurrently, and all of them are safe to
run in every replica:

| Loop | Responsibility |
| --- | --- |
| `worker/runner.py` | claim and execute ready jobs |
| `worker/scheduler.py` | promote due `scheduled` jobs (future jobs and retries) to `pending`, then enqueue |
| `worker/recovery.py` | release `processing` jobs whose lease expired |

Both background loops select their rows with `FOR UPDATE SKIP LOCKED` and then
re-prove their conditions in the `UPDATE`, so parallel loops take disjoint work
and a row can only be activated or recovered once. Recovery additionally proves
the lease is *still* expired at write time, so a worker whose heartbeat arrives
mid-sweep keeps its job.

A recovered attempt counts as a failed attempt, because `attempt_count` was
already consumed at claim time. It therefore follows the same policy: retry with
backoff while attempts remain, otherwise `failed` with the error
`worker lease expired`.

### Remaining failure window: PostgreSQL committed, Redis not

The scheduler commits `scheduled -> pending` before enqueueing to Redis, because
PostgreSQL is authoritative. If the Redis enqueue then fails, the job stays
durably `pending` but invisible to workers, and the failure is logged as
`scheduled_job_enqueue_failed` (`job_enqueue_failed` for direct submissions).
This window is real and is not hidden behind a two-store pseudo-transaction:
rolling the row back after a Redis timeout of unknown outcome can produce a
double enqueue just as easily as a lost one. Closing it needs a reconciliation
sweep that compares `pending` rows against the queue's entry mapping and
re-queues only what is genuinely missing; that belongs with the queue-statistics
work of a later phase, and blind re-enqueueing would destroy FIFO order.

## API

Full request and response schemas are browsable in Swagger UI at
<http://localhost:8000/docs>.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/jobs` | Submit a job. `201` when created, `200` when an idempotency key replays an existing job. |
| `GET` | `/jobs/{job_id}` | Fetch one job. `404` if unknown. |
| `GET` | `/jobs` | List jobs, newest first. Filters: `status`, `type`. Pagination: `limit` (max 100, default 50), `offset`. |
| `POST` | `/jobs/{job_id}/cancel` | Cancel a `pending` or `scheduled` job. Idempotent for an already-cancelled job (`200`). `409` if processing, completed or failed. `404` if unknown. |
| `POST` | `/jobs/{job_id}/retry` | Re-queue a `failed` job as a new three-attempt cycle (`attempt_count = 0`). `409` otherwise. `404` if unknown. |
| `GET` | `/health` | PostgreSQL and Redis probes plus queue/job-status counts. `503` if either store is unreachable. |

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
durably `pending` but invisible to workers until something re-queues it; the
failure is logged as `job_enqueue_failed`. `/health` reports `pending` and
`ready` separately so that window is visible.

## Job execution

Handlers are mocks that simulate work with async sleeps: `email` returns a
message ID, `webhook` succeeds 80% of the time and otherwise fails, `report`
returns a file URL, and `batch` processes each item while reporting progress.

A successful job goes `pending` -> `processing` -> `completed`, storing its
result, `progress = 100` and `completed_at`. A failure with attempts remaining
goes `processing` -> `scheduled` and waits out its backoff before the scheduler
returns it to `pending`; the final failure goes `processing` -> `failed`.
Ownership fields (`worker_id`, `execution_token`, `lease_expires_at`) are cleared
whenever a job leaves `processing`, so they only ever describe a live attempt.

### Cancellation

`POST /jobs/{id}/cancel` is a single conditional `UPDATE` that accepts only
`pending` or `scheduled`. The database transition is the race against a worker
claim: if cancel wins, the claim matches zero rows; if the claim wins, cancel
returns `409`. `processing` jobs are never cancelled, because an in-flight
attempt must finish under its owner (or be recovered when its lease expires).

Cancelling a job that is already `cancelled` returns the existing row with
`200`. That is the one idempotent case; `completed` and `failed` are conflicts.

A cancelled `pending` job has its *current* Redis entry removed by token. If
that cleanup fails, the job stays durably `cancelled` and the leftover entry
cannot be claimed. Correctness does not depend on Redis.

### Manual retry

`POST /jobs/{id}/retry` accepts only `failed` jobs. It transitions
`failed -> pending`, resets `attempt_count` to 0 (a new three-attempt cycle,
so the `attempt_count <= max_attempts` constraint stays valid), and clears
`error`, `result`, `progress`, `completed_at`, `started_at`, `scheduled_at` and
the ownership fields. `max_attempts` stays 3. Previous attempts remain in
structured logs.

PostgreSQL is committed first. A Redis enqueue failure is logged as
`job_retry_enqueue_failed` and leaves the job durably `pending`, the same
window as submit and scheduler publish.

### Graceful shutdown

`SIGTERM`/`SIGINT` stop the worker accepting new work: no further claims, and
no new scheduler or recovery sweep. An attempt this process already owns is
drained to completion with its heartbeat still running, then the process
exits. Compose gives the worker a `stop_grace_period` of two minutes so
`docker stop` can wait for that drain. There is no application-level hard
timeout; a `SIGKILL` (or a grace period that expires) is the crash path and
lease recovery retries the abandoned attempt.

Ownership-loss cancellation of the handler is unchanged and separate: that is
what happens when recovery takes the job away, not when the process is asked
to shut down.

### Health

`GET /health` probes PostgreSQL (`SELECT 1` plus a `GROUP BY status` count) and
Redis (`ReadyQueue.size()`). Both counts are reported honestly. `ready`
(Redis) and `pending` (PostgreSQL) may differ; that mismatch is the documented
enqueue window and does not mark the service unhealthy. `completed` is included
so the response shows throughput, not only backlog. Either store unreachable
yields `503` with the failing component set to `unhealthy`.

## Crash-recovery demonstration

The lease and heartbeat values are configurable, so the demo can run with a short
lease without changing the defaults:

```bash
JOB_LEASE_SECONDS=10 JOB_HEARTBEAT_SECONDS=3 RECOVERY_INTERVAL_SECONDS=2 \
  docker compose up -d --build

# Submit a slow job and watch it get claimed.
curl -s localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"type":"report","payload":{"report_type":"sales","format":"pdf"}}'
curl -s localhost:8000/jobs/<id>   # processing, with worker_id and lease_expires_at

# Kill the owning worker outright, so no SIGTERM handler runs.
docker kill --signal=SIGKILL <worker-container>

# After the lease expires, another worker's recovery loop releases the job
# (job_recovered), the scheduler re-activates it after the backoff, and it runs
# again with attempt_count = 2 and a different execution_token.
```

Because recovery clears `execution_token`, the killed worker could not have
written the job even if it came back: its token no longer matches any row.

## Testing

```bash
docker compose run --rm api pytest -v
```

Tests run against a real PostgreSQL instance, in a separate `jobqueue_test`
database that is created and migrated automatically. SQLite is deliberately not
used, because the behaviour under test (JSONB, the native status enum, and the
partial unique index behind idempotency) is PostgreSQL-specific. Tables are
truncated between tests, so the suite is repeatable.
