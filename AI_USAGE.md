# AI Usage

AI was used as a drafting and review assistant. It did not have merge
authority. Every suggestion was treated as a proposal: read it, try to break
it under concurrency or failure, keep it only if the tests and the database
still agreed.

## Tools

- **ChatGPT** — architecture review, failure-mode analysis, and critique of
  pickup / recovery / queue designs before and during implementation.
- **Cursor**, with **Claude Opus 5 High** and later **Grok 4.6 High Fast** —
  implementation, tests, migrations, and in-editor review.

The human (Avi) specified the architecture constraints, wrote or rewrote the
invariants that mattered, ran the suite and the manual crash/cancel/drain
demos, and rejected designs that only looked correct.

## Where AI helped

- Boilerplate that is easy to get mechanically right: FastAPI routers,
  Pydantic payload models, Alembic revision scaffolding, pytest fixtures
  against a real Postgres/Redis pair.
- Turning a stated invariant into a first-draft conditional `UPDATE` or a
  focused test (claim races, fencing, scheduler `SKIP LOCKED`).
- Expanding a known failure into a deterministic test once the failure was
  understood (stale queue token, batch-limit CTE, legacy `NULL` lease).

## Where review corrected AI output

These are ordinary engineering findings. The first draft was reasonable and
wrong in a way that only showed up under a precise reading or a real
database.

1. **Destructive Redis pop before durable ownership.** An early pickup sketch
   popped the ready entry and then claimed in PostgreSQL. A worker that dies
   in that window takes the only record of readiness with it, and no row is
   owned. Pickup is peek-then-claim; Redis is deleted only after the
   `UPDATE` returns a row.

2. **`ORDER BY created_at DESC` and indexes.** An early comment claimed a
   descending btree would become an expression index that Alembic could not
   reflect. That is incorrect. A plain btree is enough; PostgreSQL scans it
   backwards. The index design stayed simple and the comment was fixed.

3. **SQLAlchemy `UPDATE ... RETURNING` and the identity map.** The statement
   succeeded in the database, but the returned ORM instance still held
   pre-update attributes when the object was already in the session.
   `populate_existing=True` was required. Caught by claim/complete tests that
   asserted on the returned row.

4. **Priority score mixed with an unbounded sequence.**
   `(100 - priority) * 10^12 + global_sequence` looks fine until the
   sequence grows past the priority band. Then an old low-priority job sorts
   ahead of a new high-priority one. The score is now `100 - priority`
   only; FIFO lives in a zero-padded member token.

5. **Legacy `processing` rows after adding `execution_token`.** The first
   `0002` draft assumed crash recovery would pick up old in-flight jobs.
   Phase-3 rows have `lease_expires_at IS NULL`, and `NULL < now()` is
   unknown, so recovery never matches. The migration now moves those rows to
   `scheduled`. Caught by reviewing the recovery predicate, then
   `tests/test_migration_0002.py`.

6. **Scheduler batch limit.**
   `UPDATE ... WHERE id IN (SELECT ... LIMIT :n FOR UPDATE SKIP LOCKED)`
   reads as a batch. PostgreSQL cannot hash a subquery that carries
   `FOR UPDATE`, so the sub-plan re-runs per candidate row and the limit
   evaporates. A CTE evaluates the lock set once.
   `test_activation_respects_the_batch_size` failed on the first version.

Other rejected proposals (heartbeat owning the handler task; rolling back a
DB commit after a Redis timeout; treating a cancelled job as `409` on repeat)
are recorded in git history and in review notes. They are not product
features.

## Review standard

Generated code was run against PostgreSQL and Redis, not against a mental
model of them. Concurrency tests were repeated. Suggestions that could not
be reconciled with a `WHERE` clause, a lease predicate, or a failing test
were not kept.
