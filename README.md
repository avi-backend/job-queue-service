# Job Queue Service

Distributed job queue service. PostgreSQL is the durable source of truth for job
state; Redis is used only as the ready-job delivery queue.

Author: Avi

> Status: Phase 1 (skeleton, infrastructure, database schema). The queue,
> worker execution loop, scheduler, retries, recovery, and job API endpoints are
> implemented in later phases.

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

_To be completed._

## Testing

_To be completed._
