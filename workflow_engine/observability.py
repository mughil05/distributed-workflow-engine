"""Low-cardinality Prometheus metrics and allow-listed JSON logs."""

import json
import logging
import threading
import time
from datetime import datetime, timezone

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class EngineMetrics:
    """A process-local metric registry; database gauges are cached snapshots."""

    def __init__(self):
        self.registry = CollectorRegistry(auto_describe=True)
        self.api_requests = Counter(
            "workflow_api_requests", "API requests", ("method", "route", "status"),
            registry=self.registry)
        self.api_latency = Histogram(
            "workflow_api_request_duration_seconds", "API request latency",
            ("method", "route"), registry=self.registry)
        self.workflow_submissions = Counter(
            "workflow_submissions", "New workflows committed", ("result",),
            registry=self.registry)
        self.task_attempts = Counter(
            "workflow_task_attempts", "Confirmed task outcome transitions", ("outcome",),
            registry=self.registry)
        self.task_duration = Histogram(
            "workflow_task_execution_duration_seconds", "Confirmed task execution duration",
            ("outcome",), registry=self.registry)
        self.postgres_failures = Counter(
            "workflow_postgres_operation_failures", "PostgreSQL operation failures",
            ("operation",), registry=self.registry)
        self.redis_failures = Counter(
            "workflow_redis_publication_failures", "Redis publication failures",
            registry=self.registry)
        self.workflow_snapshot = Gauge(
            "workflow_records", "Committed workflow records by state", ("state",),
            registry=self.registry)
        self.attempt_snapshot = Gauge(
            "workflow_attempt_records", "Committed attempt records by outcome", ("outcome",),
            registry=self.registry)
        self.retry_snapshot = Gauge(
            "workflow_retry_scheduled_attempt_records",
            "Committed retry-scheduled attempts in PostgreSQL", registry=self.registry)
        self.dead_letter_snapshot = Gauge(
            "workflow_dead_lettered_task_records",
            "Committed dead-lettered tasks in PostgreSQL", registry=self.registry)
        self.active_leases = Gauge(
            "workflow_active_task_leases", "Unexpired running task leases in PostgreSQL",
            registry=self.registry)
        self.snapshot_age = Gauge(
            "workflow_database_snapshot_age_seconds", "Age of database metrics snapshot",
            registry=self.registry)
        self._snapshot_lock = threading.Lock()
        self._snapshot_time = 0.0
        self._database_snapshot = None

    def refresh_database_snapshot(self, store, *, max_age_seconds=30.0) -> None:
        now = time.monotonic()
        if now - self._snapshot_time < max_age_seconds:
            self.snapshot_age.set(now - self._snapshot_time)
            return
        if not self._snapshot_lock.acquire(blocking=False):
            return
        try:
            now = time.monotonic()
            if now - self._snapshot_time < max_age_seconds:
                return
            snapshot = store.metrics_snapshot()
            for state in ("pending", "running", "succeeded", "failed"):
                self.workflow_snapshot.labels(state=state).set(snapshot["workflows"].get(state, 0))
            for outcome in ("running", "succeeded", "retry_scheduled", "failed",
                            "dead_lettered", "lease_expired"):
                self.attempt_snapshot.labels(outcome=outcome).set(
                    snapshot["attempts"].get(outcome, 0))
            self.retry_snapshot.set(snapshot["attempts"].get("retry_scheduled", 0))
            self.dead_letter_snapshot.set(snapshot["dead_lettered_tasks"])
            self.active_leases.set(snapshot["active_leases"])
            self._snapshot_time = time.monotonic()
            self._database_snapshot = (snapshot, datetime.now(timezone.utc),
                                       self._snapshot_time)
            self.snapshot_age.set(0)
        finally:
            self._snapshot_lock.release()

    def cached_database_snapshot(self, *, max_age_seconds=30.0):
        """Return the last committed database snapshot and an explicit freshness age."""
        cached = self._database_snapshot
        if cached is None:
            return None
        snapshot, captured_at, monotonic_at = cached
        age = max(0.0, time.monotonic() - monotonic_at)
        return {"snapshot": snapshot, "as_of": captured_at,
                "age_seconds": age, "stale": age > max_age_seconds}


class JsonLogFormatter(logging.Formatter):
    """Serialize only known-safe logging fields; omit messages and tracebacks."""

    SAFE_FIELDS = ("event", "operation", "reason", "workflow_id", "task_id",
                   "worker_id", "attempt_number", "fencing_token", "duration_ms",
                   "error_type", "request_id", "status_code", "method", "route")

    def format(self, record):
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
        }
        for field in self.SAFE_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        return json.dumps(payload, separators=(",", ":"), default=str)


def configure_json_logging(level=logging.INFO):
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        root.addHandler(logging.StreamHandler())
    for handler in root.handlers:
        handler.setFormatter(JsonLogFormatter())
