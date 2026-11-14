"""Runnable four-task workflow demonstration."""

import argparse
import os
import sys
from dataclasses import asdict

from .definitions import Task, Workflow
from .engine import WorkflowEngine
from .storage import SQLiteStore
from .postgres import PostgresStore
from .worker import PostgresWorker
from .retries import RetryPolicy


def _run_api(argv):
    parser = argparse.ArgumentParser(prog="workflow-engine api",
                                     description="Run the workflow REST API")
    parser.add_argument("--dsn", default=os.environ.get("WORKFLOW_DATABASE_URL"))
    parser.add_argument("--host", default=os.environ.get("WORKFLOW_API_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    if not args.dsn:
        parser.error("--dsn or WORKFLOW_DATABASE_URL is required")
    import uvicorn
    from .api import create_app
    from .observability import configure_json_logging
    configure_json_logging()
    uvicorn.run(create_app(dsn=args.dsn, host=args.host), host=args.host,
                port=args.port, log_config=None)


def _run_worker(argv):
    parser = argparse.ArgumentParser(prog="workflow-engine worker",
                                     description="Discover and run submitted workflows")
    parser.add_argument("--dsn", default=os.environ.get("WORKFLOW_DATABASE_URL"))
    parser.add_argument("--redis-url", default=os.environ.get("WORKFLOW_REDIS_URL"))
    parser.add_argument("--worker-id")
    parser.add_argument("--lease-seconds", type=float, default=60.0)
    parser.add_argument("--renewal-interval", type=float, default=15.0)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--fallback-scan-interval", type=float, default=5.0)
    parser.add_argument("--discovery-batch-size", type=int, default=32)
    parser.add_argument("--metrics-host", default="127.0.0.1")
    parser.add_argument("--metrics-port", type=int, default=9101)
    parser.add_argument("--no-metrics", action="store_true",
                        help="disable the worker Prometheus HTTP listener")
    args = parser.parse_args(argv)
    if not args.dsn:
        parser.error("--dsn or WORKFLOW_DATABASE_URL is required")
    from psycopg_pool import ConnectionPool
    import redis
    if not args.no_metrics:
        from prometheus_client import start_http_server
    from .observability import configure_json_logging
    from .postgres import PostgresStore
    from .worker import PostgresWorker
    configure_json_logging()
    pool = ConnectionPool(conninfo=args.dsn, min_size=1, max_size=8, timeout=2,
                          kwargs={"options": "-c statement_timeout=3000 -c lock_timeout=500"})
    store = PostgresStore(args.dsn, connection_pool=pool)
    redis_client = (redis.Redis.from_url(args.redis_url, socket_connect_timeout=0.25,
                                         socket_timeout=0.25, health_check_interval=30)
                    if args.redis_url else None)
    worker = PostgresWorker(store, lease_seconds=args.lease_seconds,
                            renewal_interval=args.renewal_interval,
                            poll_interval=args.poll_interval,
                            fallback_scan_interval=args.fallback_scan_interval,
                            redis_client=redis_client, worker_id=args.worker_id)
    if not args.no_metrics:
        start_http_server(args.metrics_port, addr=args.metrics_host,
                          registry=worker.metrics.registry)
    try:
        worker.run_forever(discovery_batch_size=args.discovery_batch_size)
    except KeyboardInterrupt:
        pass
    finally:
        if redis_client:
            redis_client.close()
        pool.close()


def main() -> None:
    argv = sys.argv[1:]
    if argv and argv[0] == "api":
        _run_api(argv[1:])
        return
    if argv and argv[0] == "worker":
        _run_worker(argv[1:])
        return
    parser = argparse.ArgumentParser(description="Run the durable workflow demo")
    parser.add_argument("--backend", choices=("sqlite", "postgres"), default="sqlite")
    parser.add_argument("--db", default="workflow.db", help="SQLite database path")
    parser.add_argument("--dsn", default=os.environ.get("WORKFLOW_DATABASE_URL"),
                        help="PostgreSQL DSN (or WORKFLOW_DATABASE_URL)")
    parser.add_argument("--lease-seconds", type=float, default=60.0)
    parser.add_argument("--renewal-interval", type=float, default=15.0)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--fallback-scan-interval", type=float, default=5.0,
                        help="Maximum idle wait before a PostgreSQL recovery scan")
    parser.add_argument("--redis-url", default=os.environ.get("WORKFLOW_REDIS_URL"),
                        help="Optional Redis URL for Pub/Sub worker wake-up notifications")
    parser.add_argument("--worker-id", help="Optional worker label; process UUID is added automatically")
    parser.add_argument("--max-attempts", type=int, default=1,
                        help="Maximum PostgreSQL lease grants per demo task (default: 1)")
    parser.add_argument("--retry-initial-delay", type=float, default=0.0)
    parser.add_argument("--retry-multiplier", type=float, default=2.0)
    parser.add_argument("--retry-max-delay", type=float, default=300.0)
    parser.add_argument("--retry-jitter", type=float, default=0.0,
                        help="Jitter mix from 0 (none) to 1 (full jitter)")
    args = parser.parse_args()
    retry_policy = RetryPolicy(max_attempts=args.max_attempts,
                               initial_delay=args.retry_initial_delay,
                               multiplier=args.retry_multiplier,
                               max_delay=args.retry_max_delay, jitter=args.retry_jitter)
    workflow = Workflow.from_tasks("demo", [
        Task("extract", retry_policy=retry_policy),
        Task("clean", ("extract",), retry_policy),
        Task("analyze", ("clean",), retry_policy),
        Task("publish", ("analyze",), retry_policy),
    ])
    actions = {task.task_id: (lambda name=task.task_id: print(f"running {name}"))
               for task in workflow.tasks}
    if args.backend == "postgres":
        if not args.dsn:
            parser.error("--dsn or WORKFLOW_DATABASE_URL is required for PostgreSQL")
        store = PostgresStore(args.dsn)
        definition_tasks = []
        for task in workflow.tasks:
            item = {"id": task.task_id, "dependencies": list(task.dependencies)}
            if task.retry_policy != RetryPolicy():
                item["retry_policy"] = asdict(task.retry_policy)
            definition_tasks.append(item)
        definition = {"tasks": definition_tasks}
        store.initialize(workflow.workflow_id, definition,
                         [task.task_id for task in workflow.tasks])
        redis_client = None
        if args.redis_url:
            import redis
            redis_client = redis.Redis.from_url(args.redis_url, socket_connect_timeout=1,
                                                socket_timeout=2, health_check_interval=30)
        state = PostgresWorker(store, lease_seconds=args.lease_seconds,
                               renewal_interval=args.renewal_interval,
                               poll_interval=args.poll_interval,
                               fallback_scan_interval=args.fallback_scan_interval,
                               redis_client=redis_client,
                               worker_id=args.worker_id).run(workflow.workflow_id, actions)
        print(f"workflow demo: {state} (PostgreSQL workflow: {workflow.workflow_id})")
    else:
        if retry_policy != RetryPolicy():
            parser.error("retry policy options require --backend postgres")
        store = SQLiteStore(args.db)
        try:
            state = WorkflowEngine(store).run(workflow, actions)
            print(f"workflow demo: {state} (database: {args.db})")
        finally:
            store.close()


if __name__ == "__main__":
    main()
