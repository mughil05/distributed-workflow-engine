#!/usr/bin/env python3
"""Reproducible, client-observed workflow benchmark; no synthetic timings."""

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone


def request(url, token, method="GET", body=None, idempotency=None, timeout=5):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if idempotency:
        headers["Idempotency-Key"] = idempotency
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body else None,
                                 headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        data = response.read()
        return json.loads(data) if data else None


def shape(kind, prefix, count, seconds):
    noop = {"type": "noop.v1", "parameters": {}}
    delay = {"type": "demo.delay.v1", "parameters": {"seconds": seconds}}
    if kind == "linear":
        return [{"id": f"{prefix}-{i}", "dependencies": [f"{prefix}-{i-1}"] if i else [],
                "action": delay if i == 0 else noop} for i in range(count)]
    if kind == "fan":
        branches = max(2, count - 2)
        tasks = [{"id": f"{prefix}-root", "action": delay}]
        tasks.extend({"id": f"{prefix}-b{i}", "dependencies": [f"{prefix}-root"],
                      "action": noop} for i in range(branches))
        tasks.append({"id": f"{prefix}-join",
                      "dependencies": [f"{prefix}-b{i}" for i in range(branches)], "action": noop})
        return tasks
    tasks = [{"id": f"{prefix}-root", "action": delay}]
    for i in range(1, count):
        deps = [f"{prefix}-root"] if i % 3 else [f"{prefix}-{i-1}"]
        if i > 2 and i % 4 == 0:
            deps.append(f"{prefix}-{i-2}")
        tasks.append({"id": f"{prefix}-{i}", "dependencies": deps, "action": noop})
    return tasks


def percentile(values, p, minimum):
    if len(values) < minimum:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * p))]


def worker_command():
    """Return benchmark child args without binding shared metrics ports."""
    return [sys.executable, "-m", "workflow_engine", "worker", "--no-metrics",
            "--poll-interval", "0.1"]


def metadata(args, redis_mode):
    commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                            text=True, check=False).stdout.strip() or "unknown"
    pg_version = redis_version = None
    try:
        import psycopg
        with psycopg.connect(os.environ["WORKFLOW_DATABASE_URL"], connect_timeout=2) as conn:
            pg_version = conn.execute("SHOW server_version").fetchone()[0]
    except Exception:
        pass
    if redis_mode and os.environ.get("WORKFLOW_REDIS_URL"):
        try:
            import redis
            redis_version = redis.Redis.from_url(os.environ["WORKFLOW_REDIS_URL"], socket_timeout=1).info()["redis_version"]
        except Exception:
            redis_version = "unavailable"
    try:
        cpu = os.cpu_count()
        memory_bytes = None
        if os.path.exists("/proc/meminfo"):
            with open("/proc/meminfo", encoding="utf8") as f:
                memory_bytes = int(f.readline().split()[1]) * 1024
        elif platform.system() == "Darwin":
            result = subprocess.run(["sysctl", "-n", "hw.memsize"],
                                    capture_output=True, text=True, timeout=2, check=False)
            if result.returncode == 0:
                memory_bytes = int(result.stdout.strip())
    except OSError:
        cpu, memory_bytes = None, None
    return {"git_commit": commit, "python": platform.python_version(),
            "postgresql": pg_version, "redis": redis_version, "os": platform.platform(),
            "cpu_count": cpu, "memory_bytes": memory_bytes, "workers": args.workers,
            "redis_mode": "enabled" if redis_mode else "disabled"}


def run(args):
    token = os.environ["WORKFLOW_API_TOKEN"]
    dsn = os.environ["WORKFLOW_DATABASE_URL"]
    redis_url = os.environ.get("WORKFLOW_REDIS_URL") if args.redis else None
    children = []
    env = os.environ.copy()
    env["WORKFLOW_DATABASE_URL"] = dsn
    if redis_url:
        env["WORKFLOW_REDIS_URL"] = redis_url
    else:
        env.pop("WORKFLOW_REDIS_URL", None)
    base = args.api.rstrip("/") + "/api/v1/workflows"
    count = 3 if args.smoke else args.sample_size
    repetitions = 1 if args.smoke else args.repetitions
    try:
        for index in range(args.workers):
            children.append(subprocess.Popen(worker_command(), env=env))
        time.sleep(1.0)  # Worker startup is outside the measured interval.
        # One unmeasured warm-up workflow to stabilize connections and discovery.
        warm_id = "warm-" + uuid.uuid4().hex
        warm_result = request(base, token, "POST", {"tasks": [{"id": "warm", "action": {"type": "noop.v1", "parameters": {}}}]}, warm_id)
        if not wait_terminal(args.api.rstrip("/") + f"/api/v1/workflows/{warm_result['workflow_id']}", token, 30):
            raise TimeoutError("unmeasured benchmark warm-up did not complete")
        samples = []
        for rep in range(repetitions):
            records = []
            for idx in range(count):
                prefix = f"b-{uuid.uuid4().hex[:10]}"
                tasks = shape(args.workload, prefix, args.task_count, args.delay_seconds)
                started = time.monotonic()
                submitted = datetime.now(timezone.utc)
                result = request(base, token, "POST", {"tasks": tasks}, prefix)
                records.append({"id": result["workflow_id"], "submitted": submitted,
                                "created_at": datetime.fromisoformat(result["created_at"]),
                                "mono": started, "task_count": len(tasks)})
            start_wall = min(item["mono"] for item in records)
            terminals = []
            first_claims = []
            pending = {item["id"]: item for item in records}
            deadline = time.monotonic() + args.deadline
            while pending and time.monotonic() < deadline:
                for workflow_id, record in list(pending.items()):
                    # Keep the timed observation loop to one lightweight workflow read.
                    detail = request(args.api.rstrip("/") + f"/api/v1/workflows/{workflow_id}", token)
                    if detail["state"] in ("succeeded", "failed"):
                        terminal_at = time.monotonic()
                        terminals.append((workflow_id, terminal_at - record["mono"], detail["state"], record["task_count"], terminal_at))
                        del pending[workflow_id]
                time.sleep(0.1)
            if pending:
                raise TimeoutError(f"{len(pending)} benchmark workflows did not reach terminal state")
            elapsed = max(item[4] for item in terminals) - start_wall
            latency = [item[1] for item in terminals]
            total_tasks = sum(item[3] for item in terminals)
            # Attempt history is fetched only after the timed terminal-observation interval.
            attempt_durations = []
            first_claims = []
            for record in records:
                task_cursor = None
                workflow_claims = []
                while True:
                    query = "?limit=100" + ("&cursor=" + urllib.parse.quote(task_cursor) if task_cursor else "")
                    task_page = request(args.api.rstrip("/") + f"/api/v1/workflows/{record['id']}/tasks{query}", token)
                    for task in task_page["items"]:
                        cursor = None
                        while True:
                            query = "?limit=100" + ("&cursor=" + urllib.parse.quote(cursor) if cursor else "")
                            page = request(args.api.rstrip("/") + f"/api/v1/workflows/{record['id']}/tasks/{urllib.parse.quote(task['task_id'])}/attempts{query}", token)
                            for attempt in page["items"]:
                                claimed = datetime.fromisoformat(attempt["claimed_at"])
                                workflow_claims.append(claimed)
                                if attempt["finished_at"]:
                                    finished = datetime.fromisoformat(attempt["finished_at"])
                                    attempt_durations.append((finished - claimed).total_seconds())
                            cursor = page["next_cursor"]
                            if not cursor:
                                break
                    task_cursor = task_page["next_cursor"]
                    if not task_cursor:
                        break
                if workflow_claims:
                    first_claims.append(max(0.0, (min(workflow_claims) - record["created_at"]).total_seconds()))
            outcomes = {state: sum(1 for item in terminals if item[2] == state)
                        for state in ("succeeded", "failed")}
            samples.append({"repetition": rep + 1, "workflows": len(terminals), "tasks": total_tasks,
                "workflow_outcomes": outcomes,
                "wall_seconds": elapsed, "workflow_throughput_per_second": len(terminals) / elapsed,
                "task_throughput_per_second": total_tasks / elapsed,
                "submit_to_terminal_client_observed_seconds": {
                    "p50": percentile(latency, .50, 1), "p95": percentile(latency, .95, 20),
                    "p99": percentile(latency, .99, 100)},
                "submit_to_first_claim_durable_timestamp_seconds": {
                    "p50": percentile(first_claims, .50, 1), "p95": percentile(first_claims, .95, 20),
                    "p99": percentile(first_claims, .99, 100)},
                "attempt_elapsed_claimed_to_finished_not_action_time_seconds": {
                    "p50": percentile(attempt_durations, .50, 1), "p95": percentile(attempt_durations, .95, 20),
                    "p99": percentile(attempt_durations, .99, 100)}})
        output = {"metadata": metadata(args, bool(redis_url)), "workload": args.workload,
                  "workload_definition": {"shape": args.workload,
                      "requested_task_count": args.task_count,
                      "first_task_delay_seconds": args.delay_seconds,
                      "shape_implementation": "benchmarks/run.py:shape"}, "sample_size": count,
                  "repetitions": repetitions, "smoke_mode": args.smoke,
                  "classification": "smoke_validation" if args.smoke else
                      ("exploratory_below_p99_sample" if count < 100 else "local_measurement_p99_sample_eligible"),
                  "methodology": "startup and one warm-up workflow excluded; during the timed interval the client polls only each workflow detail (one bounded request per workflow per 100ms cycle); submit-to-terminal is client-observed from submission start until the first poll that reports a terminal state, so it includes up to one polling interval plus API/DB request overhead; attempt histories and durable claim timestamps are collected only after all timed observations finish; attempt elapsed uses claimed_at to finished_at and includes orchestration overhead, not pure action time; p95 requires 20 samples and p99 requires 100; results are workload/client observation measurements, not a general system-throughput claim",
                  "results": samples}
        print(json.dumps(output, indent=2))
    finally:
        for child in children:
            child.terminate()
        for child in children:
            try: child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                child.kill(); child.wait(timeout=3)


def wait_terminal(url, token, deadline):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        try:
            workflow = request(url, token, "GET")
            if workflow["state"] in ("succeeded", "failed"):
                return True
        except Exception:
            pass
        time.sleep(.1)
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://127.0.0.1:8000")
    parser.add_argument("--workers", type=int, choices=(1, 2, 4), default=1)
    parser.add_argument("--redis", action="store_true")
    parser.add_argument("--workload", choices=("linear", "fan", "mixed"), default="linear")
    parser.add_argument("--task-count", type=int, default=8)
    parser.add_argument("--sample-size", type=int, default=20,
                        help="workflows per repetition (manual matrix uses 100 for p99 eligibility)")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=.02)
    parser.add_argument("--deadline", type=int, default=120)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if not 2 <= args.task_count <= 100 or args.sample_size < 1 or args.repetitions < 1:
        parser.error("task count must be 2..100 and sample/repetitions positive")
    if args.smoke:
        print("SMOKE MODE: validation only; do not interpret these as performance results.", file=sys.stderr)
    run(args)


if __name__ == "__main__":
    main()
