"""Submit a fixed, harmless local demo DAG."""

import json
import os
import urllib.request
import uuid
from pathlib import Path
from datetime import datetime, timedelta, timezone

base = "http://127.0.0.1:8000/api/v1/workflows"
headers = {"Authorization": "Bearer " + os.environ["WORKFLOW_API_TOKEN"],
           "Content-Type": "application/json",
           "Idempotency-Key": "demo-" + str(uuid.uuid4())}
until = (datetime.now(timezone.utc) + timedelta(seconds=9)).isoformat()
tasks = [
    {"id": "begin", "action": {"type": "demo.delay.v1", "parameters": {"seconds": 0.2}}},
    {"id": "branch-a", "dependencies": ["begin"], "action": {"type": "noop.v1", "parameters": {}}},
    {"id": "branch-b", "dependencies": ["begin"], "action": {"type": "demo.retry_until.v1", "parameters": {"until": until}}, "retry_policy": {"max_attempts": 6, "initial_delay": 2, "multiplier": 1, "max_delay": 2}},
    {"id": "permanent-failure", "dependencies": ["begin"], "action": {"type": "text.require_nonempty.v1", "parameters": {"text": " "}}, "retry_policy": {"max_attempts": 4}},
    {"id": "independent-after-failure", "dependencies": ["branch-a"], "action": {"type": "noop.v1", "parameters": {}}},
    {"id": "join", "dependencies": ["branch-a", "branch-b"], "action": {"type": "noop.v1", "parameters": {}}},
]
request = urllib.request.Request(base, data=json.dumps({"tasks": tasks}).encode(), headers=headers, method="POST")
with urllib.request.urlopen(request, timeout=10) as response:
    result = json.load(response)
with Path(".env.demo").open("a", encoding="utf-8") as env_file:
    env_file.write("WORKFLOW_DEMO_WORKFLOW_ID=" + result["workflow_id"] + "\n")
print("Submitted demo workflow:", result["workflow_id"])
