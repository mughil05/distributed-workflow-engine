#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -f .env.demo ]]; then
  echo "No local demo environment exists."
  exit 0
fi
set -a
source .env.demo
set +a
project="$(python scripts/demo_project.py validate "${WORKFLOW_DEMO_PROJECT:-}")"
docker compose --project-name "$project" -f docker-compose.demo.yml down --volumes --remove-orphans
rm -f .env.demo
