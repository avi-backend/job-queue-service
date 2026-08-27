#!/usr/bin/env bash
# Migrations are applied by the one-shot `migrate` compose service, which the
# API depends on, so this script only starts the server.
set -euo pipefail

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
