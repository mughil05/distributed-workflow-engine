#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env.demo ]]; then
  echo "A demo environment already exists; run scripts/demo-down.sh first." >&2
  exit 1
fi
project="$(python scripts/demo_project.py new)"
python - "$project" <<'PY' > .env.demo
import secrets
import sys
print("WORKFLOW_DEMO_PROJECT=" + sys.argv[1])
print("WORKFLOW_DB_PASSWORD=" + secrets.token_urlsafe(24))
print("WORKFLOW_API_TOKEN=" + secrets.token_urlsafe(32))
PY
chmod 600 .env.demo
set -a
source .env.demo
set +a
cleanup_on_error() { docker compose --project-name "$project" -f docker-compose.demo.yml down --volumes --remove-orphans >/dev/null 2>&1 || true; rm -f .env.demo; }
trap cleanup_on_error ERR
docker compose --project-name "$project" -f docker-compose.demo.yml up --build -d --scale worker=3
pg_port=$(docker compose --project-name "$project" -f docker-compose.demo.yml port postgres 5432 | sed 's/.*://')
redis_port=$(docker compose --project-name "$project" -f docker-compose.demo.yml port redis 6379 | sed 's/.*://')
printf 'WORKFLOW_DEMO_PG_PORT=%s\nWORKFLOW_DEMO_REDIS_PORT=%s\n' "$pg_port" "$redis_port" >> .env.demo
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8000/health/ready >/dev/null; then break; fi
  sleep 1
done
curl -fsS http://127.0.0.1:8000/health/ready >/dev/null
WORKFLOW_API_TOKEN="$WORKFLOW_API_TOKEN" python scripts/seed_demo.py
printf 'WORKFLOW_DEMO_API_URL=http://127.0.0.1:8000\n' >> .env.demo
trap - ERR
echo "Dashboard: http://127.0.0.1:8000/dashboard/"
echo "API token (enter in dashboard; memory only): $WORKFLOW_API_TOKEN"
echo "Tear down only this demo with: scripts/demo-down.sh"
