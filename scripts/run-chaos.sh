#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
project="wf005-chaos-$(python -c 'import uuid; print(uuid.uuid4().hex[:12])')"
chaos_ports=$(python - <<'PY'
import socket
sockets = []
for _ in range(2):
    item = socket.socket()
    item.bind(("127.0.0.1", 0))
    sockets.append(item)
print(f"{sockets[0].getsockname()[1]}\t{sockets[1].getsockname()[1]}")
for item in sockets:
    item.close()
PY
)
IFS=$'\t' read -r WORKFLOW_CHAOS_POSTGRES_PORT WORKFLOW_CHAOS_REDIS_PORT <<< "$chaos_ports"
export WORKFLOW_CHAOS_POSTGRES_PORT WORKFLOW_CHAOS_REDIS_PORT
compose=(docker compose --project-name "$project" -f docker-compose.chaos.yml)
cleanup() { "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM
"${compose[@]}" up -d --wait
pg_port=$("${compose[@]}" port postgres 5432 | sed 's/.*://')
redis_port=$("${compose[@]}" port redis 6379 | sed 's/.*://')
export WORKFLOW_TEST_POSTGRES_DSN="postgresql://workflow:chaos-only@127.0.0.1:${pg_port}/workflow"
export WORKFLOW_TEST_REDIS_URL="redis://127.0.0.1:${redis_port}/0"
export WORKFLOW_CHAOS_PROJECT="$project"
export CI=1
python -m unittest discover -s tests -v
python -m unittest discover -s tests/chaos -p 'chaos_*.py' -v
