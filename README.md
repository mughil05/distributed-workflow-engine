# Distributed Workflow Orchestrator

Copyright © 2026 Mughil Vinayagam. All rights reserved.

A portfolio project for durable DAG execution and distributed worker coordination. SQLite provides a lightweight local engine; PostgreSQL is authoritative for concurrent workers, leases, fencing, retries, attempt history, and workflow state. Redis is an optional wake-up channel, never a source of truth.

> **At a glance:** Built a durable workflow engine with PostgreSQL-backed distributed workers, leases and fencing, retries, crash recovery, a FastAPI API, and a React DAG dashboard. The documented local verification includes **98 Python tests**, **3 service-recovery tests**, **10 dashboard tests**, and a Compose demo. Start with [Quick local demo](#quick-local-demo) or [Architecture](#architecture).


Key features include validated DAGs, server-registered typed actions, an idempotent FastAPI submission API, a React dashboard with read-only DAG visualization, a transactional notification outbox, structured logs, Prometheus metrics, reproducible benchmarks, and isolated recovery tests. Delivery is at least once; external side effects are not exactly once. See the [runtime architecture diagram](#runtime-architecture-diagram) and detailed limitations below.

## Quick local demo

Requires Docker Compose v2. This demo is for a trusted local machine, binds published ports to loopback, and is not a hardened remote deployment:

```sh
scripts/demo-up.sh
```

Open `http://127.0.0.1:8000/dashboard/` and enter the generated token printed by the script. The demo starts PostgreSQL 16, Redis 7, FastAPI, three workers, and the built dashboard in a unique Compose project. Its teardown removes only that run's containers and volume:

```sh
scripts/demo-down.sh
```

### Verified checks

On 2026-11-14, the full Python suite passed **98 tests** with SQLite, PostgreSQL 16, Redis 7, and API/discovery integration enabled. The isolated service-recovery suite passed **3 tests**. The dashboard passed **10 tests**, TypeScript type checking, and its production build. The loopback Compose demo smoke check also passed. The recorded benchmark below is a single local run and must not be treated as a general performance claim.

## SQLite CLI demo

Requires Python 3.9 or newer. From this directory:

```sh
python -m workflow_engine --db workflow.db
python -m unittest discover -s tests -v
```

The CLI runs `extract → clean → analyze → publish`. The corresponding definition is in [`examples/demo.json`](examples/demo.json). Re-running against the same database keeps successful task checkpoints and does not invoke those tasks again.

## Architecture

- `definitions.py` contains immutable `Task` and `Workflow` objects.
- `dag.py` validates dependencies and cycles, then produces a lexicographically deterministic topological order.
- `storage.py` owns the SQLite workflow and task state tables.
- `engine.py` executes ready tasks, writes state transitions, and resumes interrupted runs.
- `cli.py` provides the runnable demonstration.

Task and workflow states are `pending`, `running`, `succeeded`, or `failed`. A task exception marks that task failed. Its downstream dependents are marked failed with a blocked-dependency reason; independent tasks continue. The workflow ends as failed if any task failed. Ordinary failed runs are terminal for that workflow ID; use a new ID and database (or reset the relevant rows yourself) for an intentional fresh attempt. A process crash that leaves a task `running` is different: the next run requeues that task.

## Durability and delivery semantics

The engine commits `running` before invoking an action and commits `succeeded` only after the action returns. A crash after an external side effect but before the success commit can cause that action to run again after recovery. Execution is therefore **at least once**, not exactly once. Successfully checkpointed tasks are skipped on recovery, but actions that were running at interruption may be repeated. Make external actions idempotent (for example, use stable operation keys or upserts) when duplicate effects matter. SQLite cannot atomically commit arbitrary external side effects with its local checkpoint.

SQLite execution remains sequential and local. Do not point multiple independent SQLite engine processes at one workflow ID as a coordination mechanism. PostgreSQL workers support concurrent scheduling, leases, and configured retries. Task actions are at least once; exactly-once external side effects are not provided.

## Database ownership

Opening a file-backed `SQLiteStore` acquires a non-blocking OS advisory lock **before** opening SQLite, reading the database, or creating its schema. The lock is database-wide, independent of workflow ID, and remains held through initialization, workflow runs, recovery, reads, and writes until `SQLiteStore.close()` (or process termination). A conflicting store raises `WorkflowAlreadyRunning`. A failed constructor closes any connection and releases every lock it acquired.

Existing files are locked by `(device, inode)`, so symlinks, relative paths, and canonical paths to the same inode converge on the same lock. Hard-linked SQLite database files are unsupported, including in WAL mode, because SQLite's WAL and shared-memory sidecars are path-specific. The store checks that a database has exactly one hard link before opening SQLite, after ownership is acquired, and before store operations. It fails closed with an actionable `WorkflowLockUnavailable` error when it detects multiple links or cannot reliably read the link count. A database that does not yet exist is first protected by a lock keyed by its canonical path. That bootstrap lock stays held while SQLite creates the file and the store acquires the resulting inode lock. The store holds both for its lifetime. Replacement or identity change at the opened path is detected by store operations and fails closed with `WorkflowLockUnavailable`.

Lock files are stored under the directory named by `WORKFLOW_ENGINE_LOCK_DIR`, if set; otherwise they live in a per-user directory under the system temporary directory. The directory must be private and user-owned on POSIX. Lock files are intentionally persistent: deleting or replacing one while stores may be open can split ownership. Symlink lock files and unsafe lock directories are rejected. Use a trusted local filesystem with working advisory locks and stable device/inode identifiers. The supported guarantee is for cooperating processes on the same host, using the same lock directory and OS account. Cross-user coordination is not supported: the lock directory must be owned by the current account and inaccessible to other accounts. Hosts, containers with isolated temporary directories, and network/virtual filesystems with nonstandard lock or identity semantics are also outside the guarantee.

The advisory ownership protocol coordinates cooperating `SQLiteStore` processes. It does not prevent an external actor from creating a hard link while a store is open. Store operations check link count immediately before database access and reject a link they observe, but filesystem mutation can race between that check and SQLite access; this is not comprehensive protection against concurrent external filesystem mutation. Direct use of the exposed `store.connection` also bypasses store checks. Network filesystems and filesystems without stable nonzero inode identity are unsupported and fail closed where detectable. Lock-file deletion by a privileged or otherwise untrusted actor, filesystem replacement races by non-cooperating actors, and direct use of the exposed `store.connection` are outside the engine's cooperative-process guarantee. `:memory:` databases are process-local and do not require file ownership.

## PostgreSQL workers

The PostgreSQL backend uses PostgreSQL 16 and psycopg 3. Start the local database and install the driver:

```sh
docker compose up -d --wait postgres redis
python -m pip install -r requirements.txt
```

Run the demo against PostgreSQL with `--backend postgres --dsn 'postgresql://workflow:workflow@localhost:54329/workflow'`, or set `WORKFLOW_DATABASE_URL`. Set `WORKFLOW_REDIS_URL` or pass `--redis-url redis://localhost:56379/0` to enable Redis wake-up notifications. Redis is optional: workers continue to scan PostgreSQL on a bounded schedule if it is unavailable or not configured. The demo idempotently initializes its definition before workers poll that workflow ID. Applications using `PostgresWorker` should initialize the workflow definition through `PostgresStore.initialize` before starting workers. The first `PostgresStore` applies numbered SQL migrations in a transaction. A PostgreSQL advisory transaction lock serializes concurrent migration startup; each version is recorded in `schema_migrations` only after its SQL succeeds. Migrations are additive and are not applied to SQLite files.

Each worker handles one workflow ID and claims one task at a time. Run multiple copies of the command to execute independent ready tasks concurrently. Redis Pub/Sub broadcasts wake-up hints; the message never assigns a task. PostgreSQL claims remain authoritative and use a short transaction with `FOR UPDATE SKIP LOCKED`. A transactional PostgreSQL outbox publishes wake-up hints after state changes. Workers subscribe before scanning, and periodically rescan PostgreSQL so lost messages, Redis restarts, and stale notifications cannot lose work. Idle workers sleep until a notification, the next retry or lease deadline, or the bounded fallback scan. Lease duration defaults to 60 seconds and renewal defaults to every 15 seconds. Lease and renewal durations must be finite and strictly positive, and renewal must occur sooner than expiry. These values are configurable with `--lease-seconds` and `--renewal-interval`; the idle scan bound is configurable with `--fallback-scan-interval`.

Claims record a process-unique worker identity, expiration calculated using the PostgreSQL clock, attempt count, and monotonically increasing per-task fencing token. Lease renewals and terminal updates require the task still be running, the same owner and token, and an unexpired lease. Every committed lease grant has one row in `task_attempts`; task state, attempt count, fencing token, and attempt history are updated atomically. `attempt_count` counts lease grants, including recovery after worker crashes or lease expiry. A crash therefore consumes an attempt. Once the configured limit is exhausted, an expired lease is dead-lettered during a later PostgreSQL scan without granting another lease. A stale worker cannot update the orchestrator's task state after ownership changes. Database transactions are not held open while user actions run.

Retry policies are configured per `Task` and apply to PostgreSQL workers. The default is one lease grant, preserving immediate terminal failure for existing workflows. Example:

```python
from workflow_engine import RetryPolicy, Task

Task("fetch", retry_policy=RetryPolicy(
    max_attempts=4, initial_delay=1, multiplier=2,
    max_delay=30, jitter=1,
))
```

The delay is capped exponential backoff; jitter ranges from 0 (disabled) to 1 (full jitter). Raise `PermanentTaskError` for an action failure that should not be retried even when attempts remain. Retry eligibility timestamps are stored in PostgreSQL, and a task cannot be claimed early. A task waiting for a retry keeps its workflow running. Exhausted or permanent failures become `dead_lettered`; downstream tasks are marked failed with a blocked-dependency reason, while independent work can continue. WF-003 does not include manual replay. SQLite retains its existing sequential recovery behavior and rejects non-default PostgreSQL retry policies.

Workers do not forcibly stop arbitrary Python actions when renewal fails or ownership is lost. Renewal failures emit warning log records with event, workflow, task, worker, fencing token, and failure reason fields. The worker abandons completion for that lease; PostgreSQL retry policy controls whether an expired lease can be granted again. An action can continue and produce an external side effect after its lease expires; if its checkpoint was not committed, the action may run again. Delivery is at least once, never exactly once. Use stable idempotency keys or downstream fencing where duplicate effects matter. PostgreSQL fencing protects this database's task state, not arbitrary external systems.

Migration 002 preserves existing workflow/task rows and records a currently running WF-002 lease as its active attempt. WF-002 did not retain completed per-attempt history, so those older historical details cannot be reconstructed.

To run the SQLite suite and real PostgreSQL integration suite:

```sh
python -m unittest discover -s tests -v
docker compose up -d --wait postgres
WORKFLOW_TEST_POSTGRES_DSN='postgresql://workflow:workflow@localhost:54329/workflow' \
  python -m unittest discover -s tests -p 'test_postgres_integration.py' -v
WORKFLOW_TEST_POSTGRES_DSN='postgresql://workflow:workflow@localhost:54329/workflow' \
WORKFLOW_TEST_REDIS_URL='redis://localhost:56379/0' \
  python -m unittest discover -s tests -p 'test_redis_integration.py' -v
```

The PostgreSQL and Redis tests use real services, multiple processes, and synchronization barriers. They are skipped locally if their test URLs are unset; CI requires both URLs and fails rather than silently skipping integration tests. SQLite execution requires no PostgreSQL or Redis service; installing `requirements.txt` adds the PostgreSQL and Redis worker dependencies.

## REST API and discovered workflows

WF-004 adds a FastAPI service over the existing PostgreSQL store. PostgreSQL remains authoritative; Redis Pub/Sub only provides optional wake-up hints. Start PostgreSQL 16 and run separate API and worker processes:

```sh
export WORKFLOW_DATABASE_URL='postgresql://workflow:workflow@localhost:54329/workflow'
export WORKFLOW_REDIS_URL='redis://localhost:56379/0' # optional
python -m workflow_engine api
python -m workflow_engine worker
```

The API binds to `127.0.0.1` by default. A non-loopback `WORKFLOW_API_HOST` requires `WORKFLOW_API_TOKEN`; clients then send `Authorization: Bearer <token>`. Configure browser origins explicitly with comma-separated `WORKFLOW_CORS_ORIGINS`. Requests are limited to 256 KiB, workflow definitions to 500 tasks, and read pages to at most 100 rows. The API exposes OpenAPI at `/docs` and `/openapi.json`.

Submit a JSON DAG to `POST /api/v1/workflows` with a required stable `Idempotency-Key` header. Repeating the same canonical definition returns its existing workflow ID (HTTP 200); reusing that key for a different definition returns HTTP 409. The API accepts only registered action names and validated JSON parameters. Built-ins are `noop.v1` and `text.require_nonempty.v1`. Deployments may register additional trusted server-side handlers by setting `WORKFLOW_ACTIONS_MODULE` to a server-controlled Python module that defines `register_actions(registry)`. Never configure that setting from client input or accept source code, shell commands, import paths, or expressions in workflow payloads.

The `worker` command discovers pending and running API workflows from PostgreSQL in bounded rotating turns and claims tasks using the existing lease and fencing protocol. Redis notifications can make discovery faster, but periodic scans recover missed notifications and Redis outages do not stop execution. A worker only executes action types that it has registered. Set `--discovery-batch-size` to bound each discovery page. Workers expose their own process-local metrics on `127.0.0.1:9101`; the API exposes its separate process-local metrics at `/metrics`. Database-derived workflow, attempt, retry, dead-letter, and active-lease gauges are cached snapshots, refreshed at most every 30 seconds. Counters and histograms are per process and are not complete global totals; aggregate them across processes in Prometheus where appropriate. In particular, a lost PostgreSQL completion acknowledgment can leave a committed transition absent from that worker process's counter because the worker deliberately does not guess whether the commit succeeded. Use the committed PostgreSQL-derived workflow and attempt outcome gauges as the reliable overall state; process-local counters are diagnostic signals only.

The read API provides workflow listing/filtering and workflow, task, and attempt inspection with bounded cursor pagination. Error text is mapped to stable sanitized error codes; raw task payloads, worker identities, fencing tokens, and exception messages are not returned. `/health/live` checks process liveness. `/health/ready` requires PostgreSQL; Redis is explicitly optional and is reported as disabled or degraded without making the API unavailable. PostgreSQL unavailability returns a generic HTTP 503 response.

Migration 003 is additive and is applied through the existing migration runner. Existing direct `PostgresWorker.run(workflow_id, actions)` and SQLite commands remain available. To run all tests, including API/discovery against real PostgreSQL 16 and Redis 7, set `WORKFLOW_TEST_POSTGRES_DSN` and `WORKFLOW_TEST_REDIS_URL` and run `python -m unittest discover -s tests -v`.

## WF-005 dashboard and local demo

The optional React, TypeScript, Vite dashboard is served by FastAPI at `/dashboard/`. It reads only the existing REST API plus `/api/v1/summary`; it has no database connection. The DAG view is read-only, uses deterministic topological layers, and switches to a searchable task table for larger graphs. Workflow summary cards identify their PostgreSQL snapshot age. Active leases are leases in PostgreSQL, not a count of live worker processes. Summary queries refresh a per-API-process database snapshot at most every 30 seconds.

Start the isolated local demo with one command:

```sh
scripts/demo-up.sh
```

It builds the dashboard and API image, starts PostgreSQL 16, Redis 7, the API and three workers in a unique Compose project, then records that project identity with ephemeral credentials in `.env.demo` and submits a fixed server-registered demonstration DAG. Teardown validates that recorded identity and removes only that project and its volume. It binds the API only to `127.0.0.1:8000`. Enter the printed API token into the dashboard when prompted; the UI holds it in memory and does not persist it. Stop and remove this demo and its volume with:

```sh
scripts/demo-down.sh
```

The demo deliberately uses ephemeral credentials in an ignored local `.env.demo` file. The local setup is not a hardened remote deployment: it has a single shared bearer token, no user accounts or RBAC, and is intended for a trusted developer machine. For remote use, put the API behind TLS and an authenticated reverse proxy, use managed secret handling, and configure strict origins and network policy. Dashboard assets are same-origin; no credentials are baked into the frontend build.

The sample workflow uses only fixed actions in `workflow_engine.demo_actions`: a bounded sleep, a time-window retry example, a permanent validation failure, dependency branches, and a join. The retry action intentionally raises until its submitted deadline, and the permanent failure exercises dead-letter propagation while an independent branch completes. No payload can choose an import path or supply executable code.

### Benchmarking

`benchmarks/run.py` runs against a running API and PostgreSQL-backed workers. The benchmark launches and later terminates only its own worker child processes. Configure `WORKFLOW_API_TOKEN`, `WORKFLOW_DATABASE_URL`, and optionally `WORKFLOW_REDIS_URL`; the server and workers must have the trusted `workflow_engine.demo_actions` registry loaded for the delay workload. Example:

```sh
WORKFLOW_API_TOKEN=... WORKFLOW_DATABASE_URL=postgresql://... \
  WORKFLOW_REDIS_URL=redis://... \
  python benchmarks/run.py --workload fan --workers 2 --redis \
    --sample-size 100 --repetitions 3 > fan-2-workers.json
```

Run `python benchmarks/run.py --help` for controls. `--smoke` uses only three workflows and is a harness check, not a meaningful performance benchmark. Startup and one warm-up run are excluded. During timed execution the client polls only workflow detail (one request per workflow each observation cycle); attempt history is fetched after every workflow has been observed terminal. Submit-to-terminal is client-observed from submission start until the first detail response reporting a terminal state. It includes polling-cycle granularity, sequential API/database request overhead, and this bounded client polling load. Submit-to-first-claim is calculated after timing from committed `created_at` and earliest durable `claimed_at`; it is not a client observation. Attempt elapsed is durable `claimed_at` through `finished_at`, including worker/orchestration overhead; it is not pure action time. p95 is withheld below 20 samples and p99 below 100. Outputs classify smoke runs, undersized exploratory runs, and samples eligible for p99; p99 eligibility does not establish general performance. The manual 18-configuration matrix uses 100 workflows per repetition and three repetitions. Results record git, Python/PostgreSQL/Redis, OS, CPU/memory, worker count, Redis mode, workload, sample sizes, and methodology as JSON. Benchmark workers disable their external metrics listeners to avoid port collisions. Benchmark workflows are retained in PostgreSQL; use a dedicated throwaway database, as the demo and CI do.

One corrected measurement run is recorded in [`docs/benchmarks/local-fanout-2-workers.json`](docs/benchmarks/local-fanout-2-workers.json). It used commit `129afadf9464b0576b6600772b86d108ae26ed3b`, macOS 15 arm64 (12 logical CPUs, 16 GiB RAM), Python 3.12.4, PostgreSQL 16.16, Redis 7.4.12, two workers, Redis enabled, and one repetition of 100 fan-out/fan-in workflows with eight tasks each. It completed 100 workflows and 800 tasks in 5.817 seconds of client-observed interval time. Client-observed submit-to-terminal p50/p95/p99 were 5.185/5.547/5.586 seconds; durable submit-to-first-claim p50/p95/p99 were 0.860/1.799/1.963 seconds. Durable attempt elapsed p50/p95/p99 were 0.0025/0.0274/0.0289 seconds, including orchestration and not representing pure action time. This single run is an example of the harness output and does not establish a general throughput or scaling claim; polling/API overhead is included in the terminal observation window.

### Isolated recovery tests

The destructive integration harness uses a unique Docker Compose project, a project-owned PostgreSQL volume, and dynamically assigned loopback ports. Its cleanup trap tears down only that project:

```sh
scripts/run-chaos.sh
```

The harness runs the real PostgreSQL, Redis, and API integration suites. They cover killed worker processes during leased execution and after an external effect, lease expiry, stale fencing, PostgreSQL transient outages/recovery, Redis outage/restart and lost notifications, duplicate wake-up hints, concurrent claims, retry exhaustion, and dead-letter propagation. Synchronization gates are used for claim/finalization races, and individual tests have deadlines. The Redis restart case owns a uniquely named Redis 7 child container with a dynamic loopback port and removes that exact container in `finally`. Never point these commands at a production database. Manual recovery and full benchmark jobs are available via GitHub Actions `workflow_dispatch`; normal PR CI runs non-destructive suites and a local Compose smoke.

### Runtime architecture diagram

```mermaid
flowchart LR
  UI[React dashboard] -->|same-origin HTTP| API[FastAPI API]
  Client[API client] --> API
  API -->|authoritative writes and read queries| PG[(PostgreSQL 16)]
  API -->|transactional outbox wake-up hints| R[(Redis 7 optional)]
  R --> W[Separate worker processes]
  PG -->|discovery, SKIP LOCKED claims, leases, fencing, attempts| W
  W -->|committed transitions and outbox| PG
  W -. optional wake-up publish .-> R
  API -->|cached committed summary snapshot| UI
```

PostgreSQL remains the source of truth for workflow/task state, attempts, leases, retries, and outbox events. Redis only reduces discovery latency; periodic PostgreSQL discovery recovers lost notifications or Redis outages. Work execution remains at least once. A worker may have performed an external side effect before dying or before its completion transaction commits, so clients should make side effects idempotent. Database fencing rejects stale task-state writes but cannot fence arbitrary external systems. Workflow rows currently do not persist exact `started_at` or `completed_at`; the dashboard shows only stored creation/update timestamps, and the benchmark uses explicitly labeled client-observed terminal timing.

### Dashboard screenshots

Captured from the running local PostgreSQL 16, Redis 7, FastAPI, and three-worker demo:

![Workflow overview](docs/screenshots/dashboard-overview.png)

![Workflow DAG and selected task attempt history](docs/screenshots/dashboard-workflow-detail.png)
