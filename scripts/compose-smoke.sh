#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cleanup() { scripts/demo-down.sh >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM
scripts/demo-up.sh >/dev/null 2>&1
set -a
source .env.demo
set +a
curl -fsS -H "Authorization: Bearer $WORKFLOW_API_TOKEN" http://127.0.0.1:8000/api/v1/summary
curl -fsS http://127.0.0.1:8000/dashboard/ | grep -q '<div id="root"></div>'
echo "Compose demo and built dashboard smoke check passed."
