"""Lease-aware PostgreSQL worker loop."""

import logging
import math
import random
import threading
import time
import uuid
from collections import OrderedDict
from typing import Callable, Dict, Optional

from .actions import ActionRegistry, UnknownActionType, configured_action_registry
from .observability import EngineMetrics
from .postgres import PostgresStore, psycopg
from .retries import PermanentTaskError

try:
    from psycopg_pool import PoolTimeout
except ImportError:  # Pool support is optional for direct PostgresWorker users.
    PoolTimeout = None


logger = logging.getLogger(__name__)


class _RedisWakeup:
    """Reconnectable Pub/Sub listener. Messages only set a local wake event."""

    def __init__(self, client, channel: str, wake: threading.Event) -> None:
        self.client = client
        self.channel = channel
        self.wake = wake
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._listen, name="workflow-redis-wakeup", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        self.stop.set()
        self.wake.set()
        self.thread.join(timeout=3)

    def wait_ready(self, timeout: float) -> None:
        self.ready.wait(timeout)

    def _listen(self) -> None:
        delay = 0.25
        while not self.stop.is_set():
            pubsub = None
            try:
                pubsub = self.client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.channel)
                self.ready.set()
                delay = 0.25
                while not self.stop.is_set():
                    message = pubsub.get_message(timeout=1.0)
                    if message:
                        self.wake.set()
            except Exception as exc:
                if not self.stop.is_set():
                    logger.warning("Redis wake-up subscription unavailable; PostgreSQL scans remain active",
                                   extra={"event": "workflow_redis_disconnected",
                                          "error_type": type(exc).__name__})
                    self.wake.set()
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass
            self.stop.wait(delay)
            delay = min(delay * 2.0, 10.0)


class PostgresWorker:
    def __init__(self, store: PostgresStore, *, lease_seconds: float = 60.0,
                 renewal_interval: float = 15.0, poll_interval: float = 0.25,
                 worker_id: Optional[str] = None, redis_client=None,
                 fallback_scan_interval: float = 5.0, metrics=None) -> None:
        for name, value in (("lease_seconds", lease_seconds),
                            ("renewal_interval", renewal_interval),
                            ("poll_interval", poll_interval),
                            ("fallback_scan_interval", fallback_scan_interval)):
            try:
                valid = math.isfinite(value) and value > 0
            except (TypeError, ValueError):
                valid = False
            if not valid:
                raise ValueError(f"{name} must be finite and strictly positive")
        if renewal_interval >= lease_seconds:
            raise ValueError("renewal_interval must be less than lease_seconds")
        self.store = store
        self.lease_seconds = lease_seconds
        self.renewal_interval = renewal_interval
        self.poll_interval = poll_interval
        self.fallback_scan_interval = fallback_scan_interval
        self.redis_client = redis_client
        self._publisher_failures = 0
        self._publisher_retry_at = 0.0
        self._retention_cleanup_at = 0.0
        self.metrics = metrics or EngineMetrics()
        self._discovery_action_cache = OrderedDict()
        self._discovery_action_cache_limit = 128
        # Include a process-instance UUID so duplicate configured worker names are safe.
        self.worker_id = f"{worker_id or 'worker'}:{uuid.uuid4()}"

    def run(self, workflow_id: str, actions: Dict[str, Callable[[], object]]) -> str:
        wake = threading.Event()
        listener = (_RedisWakeup(self.redis_client, self._channel(workflow_id), wake)
                    if self.redis_client is not None else None)
        if listener:
            listener.start()
            # Give Pub/Sub a brief opportunity to subscribe before the initial
            # PostgreSQL scan. Redis outage remains bounded by this timeout.
            listener.wait_ready(min(0.5, self.fallback_scan_interval))
        try:
            return self._run_loop(workflow_id, actions, wake)
        finally:
            if listener:
                listener.close()

    def run_forever(self, *, registry: Optional[ActionRegistry] = None,
                    stop_event: Optional[threading.Event] = None,
                    discovery_batch_size: int = 32) -> None:
        """Discover API workflows and take at most one task turn per workflow scan."""
        if discovery_batch_size < 1:
            raise ValueError("discovery_batch_size must be positive")
        registry = registry or configured_action_registry()
        stop_event = stop_event or threading.Event()
        wake = threading.Event()
        listener = (_RedisWakeup(self.redis_client, "workflow:dispatch", wake)
                    if self.redis_client is not None else None)
        if listener:
            listener.start()
            listener.wait_ready(min(0.5, self.fallback_scan_interval))
        cursor = None
        cycle_made_progress = False
        idle_delay = min(self.fallback_scan_interval, max(0.05, self.poll_interval))
        try:
            while not stop_event.is_set():
                wake.clear()
                self._dispatch_notifications()
                try:
                    self.metrics.refresh_database_snapshot(self.store)
                    workflow_ids = self.store.discoverable_workflows(
                        limit=discovery_batch_size, after_workflow_id=cursor)
                except self._transient_database_errors() as exc:
                    self.metrics.postgres_failures.labels(operation="workflow_discovery").inc()
                    logger.warning("Workflow discovery unavailable; will retry",
                                   extra={"event": "workflow_database_retry",
                                          "operation": "workflow_discovery",
                                          "error_type": type(exc).__name__})
                    self._sleep_database_backoff(1)
                    continue
                if not workflow_ids:
                    cursor = None
                    if cycle_made_progress:
                        idle_delay = min(self.fallback_scan_interval,
                                         max(0.05, self.poll_interval))
                    else:
                        self._wait_for_discovery(wake, idle_delay)
                        idle_delay = min(self.fallback_scan_interval, idle_delay * 2)
                    cycle_made_progress = False
                    continue

                made_progress = False
                for workflow_id in workflow_ids:
                    if stop_event.is_set():
                        break
                    cursor = workflow_id
                    try:
                        state = self.store.start_workflow(workflow_id)
                        if state is None or state in ("succeeded", "failed"):
                            self._evict_terminal_actions(workflow_id, state)
                            continue
                        claim = self.store.claim_task(workflow_id, self.worker_id,
                                                      self.lease_seconds)
                        if claim is not None:
                            made_progress = True
                            try:
                                actions = self._registered_actions(workflow_id, registry)
                            except Exception as exc:
                                if isinstance(exc, self._transient_database_errors()):
                                    raise
                                logger.error("Persisted workflow action is unavailable on this worker",
                                             extra={"event": "workflow_action_unavailable",
                                                    "workflow_id": workflow_id,
                                                    "error_type": type(exc).__name__})
                                # A claim cannot be left running until lease expiry if
                                # this worker lacks a trusted handler. Fenced failure
                                # settlement is terminal and never invokes client code.
                                self._settle_task(claim, "failed",
                                                  "registered action unavailable on worker",
                                                  retryable=False)
                                self._evict_terminal_actions(workflow_id,
                                                             self.store.workflow_state(workflow_id))
                                continue
                            self._execute(claim, actions)
                        state = self.store.workflow_state(workflow_id)
                        self._evict_terminal_actions(workflow_id, state)
                    except self._transient_database_errors() as exc:
                        self.metrics.postgres_failures.labels(
                            operation="workflow_turn").inc()
                        logger.warning("Workflow turn failed because PostgreSQL is unavailable",
                                       extra={"event": "workflow_database_retry",
                                              "operation": "workflow_turn",
                                              "workflow_id": workflow_id,
                                              "error_type": type(exc).__name__})
                cycle_made_progress = cycle_made_progress or made_progress
                if len(workflow_ids) < discovery_batch_size:
                    cursor = None
                    if cycle_made_progress:
                        idle_delay = min(self.fallback_scan_interval,
                                         max(0.05, self.poll_interval))
                    else:
                        self._wait_for_discovery(wake, idle_delay)
                        idle_delay = min(self.fallback_scan_interval, idle_delay * 2)
                    cycle_made_progress = False

        finally:
            if listener:
                listener.close()

    def _registered_actions(self, workflow_id, registry):
        actions = self._discovery_action_cache.get(workflow_id)
        if actions is not None:
            self._discovery_action_cache.move_to_end(workflow_id)
            return actions
        definition = self.store.workflow_definition(workflow_id)
        if not definition or definition.get("api_definition_version") != 1:
            return {}
        actions = {}
        for task in definition.get("tasks", []):
            action = task.get("action")
            if not isinstance(action, dict) or not isinstance(action.get("type"), str):
                raise UnknownActionType("missing action declaration")
            actions[task["id"]] = registry.resolve(action["type"], action.get("parameters", {}))
        self._discovery_action_cache[workflow_id] = actions
        while len(self._discovery_action_cache) > self._discovery_action_cache_limit:
            self._discovery_action_cache.popitem(last=False)
        return actions

    def _evict_terminal_actions(self, workflow_id, state):
        if state in (None, "succeeded", "failed"):
            self._discovery_action_cache.pop(workflow_id, None)

    def _wait_for_discovery(self, wake, delay):
        """Wait interruptibly after an idle discovery cycle."""
        wake.wait(delay)

    @staticmethod
    def _channel(workflow_id: str) -> str:
        return f"workflow:{workflow_id}:wake"

    def _run_loop(self, workflow_id: str, actions, wake: threading.Event) -> str:
        db_error_count = 0
        while True:
            try:
                state = self.store.start_workflow(workflow_id)
                break
            except self._transient_database_errors() as exc:
                self.metrics.postgres_failures.labels(operation="start_workflow").inc()
                logger.warning("Could not start workflow because PostgreSQL is unavailable; retrying",
                               extra={"event": "workflow_database_retry", "operation": "start_workflow",
                                      "workflow_id": workflow_id,
                                      "error_type": type(exc).__name__})
                db_error_count += 1
                self._sleep_database_backoff(db_error_count)
        if state is None:
            raise KeyError(f"unknown workflow ID {workflow_id!r}")
        if state in ("succeeded", "failed"):
            return state
        while True:
            # Clear before the database scan. A notification arriving after this
            # point remains set and makes the subsequent wait return immediately.
            wake.clear()
            self._dispatch_notifications()
            try:
                claim = self.store.claim_task(workflow_id, self.worker_id, self.lease_seconds)
            except self._transient_database_errors() as exc:
                self.metrics.postgres_failures.labels(operation="claim_task").inc()
                logger.warning("Could not claim workflow task because PostgreSQL is unavailable; retrying",
                               extra={"event": "workflow_database_retry", "operation": "claim_task",
                                      "workflow_id": workflow_id, "error_type": type(exc).__name__,
                                      })
                db_error_count += 1
                self._sleep_database_backoff(db_error_count)
                continue
            db_error_count = 0
            if claim is None:
                try:
                    state = self.store.workflow_state(workflow_id)
                except self._transient_database_errors() as exc:
                    self.metrics.postgres_failures.labels(operation="workflow_state").inc()
                    logger.warning("Could not read workflow state because PostgreSQL is unavailable; retrying",
                                   extra={"event": "workflow_database_retry", "operation": "workflow_state",
                                          "workflow_id": workflow_id, "error_type": type(exc).__name__})
                    db_error_count += 1
                    self._sleep_database_backoff(db_error_count)
                    continue
                if state in ("succeeded", "failed"):
                    return state
                if state is None:
                    raise KeyError(f"unknown workflow ID {workflow_id!r}")
                try:
                    due_in = self.store.next_wakeup_delay(workflow_id)
                except self._transient_database_errors() as exc:
                    self.metrics.postgres_failures.labels(operation="next_wakeup_delay").inc()
                    logger.warning("Could not find next workflow wake time; retrying",
                                   extra={"event": "workflow_database_retry",
                                          "operation": "next_wakeup_delay",
                                          "workflow_id": workflow_id,
                                          "error_type": type(exc).__name__})
                    db_error_count += 1
                    self._sleep_database_backoff(db_error_count)
                    continue
                timeout = self.fallback_scan_interval
                if due_in is not None:
                    timeout = min(timeout, max(0.1, float(due_in)))
                wake.wait(timeout)
                continue
            self._execute(claim, actions)

    def _sleep_database_backoff(self, attempt: int) -> None:
        ceiling = min(5.0, self.poll_interval * (2 ** min(attempt - 1, 10)))
        time.sleep(random.uniform(ceiling * 0.5, ceiling))

    def _dispatch_notifications(self) -> None:
        now = time.monotonic()
        if now >= self._retention_cleanup_at:
            self._retention_cleanup_at = now + 60.0
            try:
                self.store.cleanup_notification_retention(limit=500)
            except self._transient_database_errors() as exc:
                self.metrics.postgres_failures.labels(
                    operation="cleanup_notification_retention").inc()
                logger.warning("Could not clean notification retention",
                               extra={"event": "workflow_database_retry",
                                      "operation": "cleanup_notification_retention",
                                      "error_type": type(exc).__name__})
        if self.redis_client is None or now < self._publisher_retry_at:
            return
        try:
            events = self.store.lease_notifications(self.worker_id, limit=10)
        except self._transient_database_errors() as exc:
            self.metrics.postgres_failures.labels(operation="lease_notifications").inc()
            logger.warning("Could not lease notification outbox rows",
                           extra={"event": "workflow_database_retry", "operation": "lease_notifications",
                                  "error_type": type(exc).__name__})
            return
        for event in events:
            try:
                self.redis_client.publish(self._channel(event["workflow_id"]),
                                          str(event["event_type"]))
            except Exception as exc:
                logger.warning("Could not publish Redis wake-up; PostgreSQL scans remain active",
                               extra={"event": "workflow_redis_publish_failed",
                                      "workflow_id": event["workflow_id"],
                                      "error_type": type(exc).__name__})
                self._publisher_failures += 1
                self.metrics.redis_failures.inc()
                ceiling = min(30.0, self.poll_interval *
                              (2 ** min(self._publisher_failures, 12)))
                self._publisher_retry_at = time.monotonic() + random.uniform(ceiling * 0.5, ceiling)
                break
            if event["event_type"] == "workflow_initialized":
                try:
                    self.redis_client.publish("workflow:dispatch", event["workflow_id"])
                except Exception as exc:
                    self.metrics.redis_failures.inc()
                    logger.warning("Could not publish workflow discovery hint",
                                   extra={"event": "workflow_redis_publish_failed",
                                          "workflow_id": event["workflow_id"],
                                          "error_type": type(exc).__name__})
                    self._publisher_failures += 1
                    ceiling = min(30.0, self.poll_interval *
                                  (2 ** min(self._publisher_failures, 12)))
                    self._publisher_retry_at = time.monotonic() + random.uniform(
                        ceiling * 0.5, ceiling)
                    break
            try:
                self.store.mark_notification_published(event["event_id"], self.worker_id)
            except self._transient_database_errors() as exc:
                # The publish may be duplicated after lease expiry; notifications
                # are idempotent hints and never carry ownership.
                self.metrics.postgres_failures.labels(
                    operation="mark_notification_published").inc()
                logger.warning("Could not acknowledge published Redis wake-up",
                               extra={"event": "workflow_database_retry",
                                      "operation": "mark_notification_published",
                                      "error_type": type(exc).__name__})
            else:
                self._publisher_failures = 0
                self._publisher_retry_at = 0.0

    @staticmethod
    def _transient_database_errors():
        if psycopg is None:
            return ()
        errors = (psycopg.OperationalError, psycopg.InterfaceError)
        return errors + ((PoolTimeout,) if PoolTimeout is not None else ())

    def _execute(self, claim, actions) -> None:
        lost = threading.Event()
        stop = threading.Event()

        def renew() -> None:
            while not stop.wait(self.renewal_interval):
                try:
                    valid = self.store.renew_lease(
                        claim["workflow_id"], claim["task_id"], self.worker_id,
                        claim["fencing_token"], self.lease_seconds,
                    )
                except Exception as exc:
                    self.metrics.postgres_failures.labels(operation="renew_lease").inc()
                    logger.warning("Lease renewal failed; worker has relinquished task ownership",
                                   extra={"event": "workflow_lease_renewal_failed",
                                          "reason": "database_error", "workflow_id": claim["workflow_id"],
                                          "task_id": claim["task_id"], "worker_id": self.worker_id,
                                          "fencing_token": claim["fencing_token"],
                                          "error_type": type(exc).__name__})
                    # Ownership is uncertain. Do not write a terminal state; expiry permits recovery.
                    lost.set()
                    return
                if not valid:
                    logger.warning("Lease renewal rejected; worker has lost task ownership",
                                   extra={"event": "workflow_lease_renewal_failed",
                                          "reason": "lease_expired_or_replaced",
                                          "workflow_id": claim["workflow_id"], "task_id": claim["task_id"],
                                          "worker_id": self.worker_id,
                                          "fencing_token": claim["fencing_token"]})
                    lost.set()
                    return

        heartbeat = threading.Thread(target=renew, name="workflow-lease-renewal", daemon=True)
        heartbeat.start()
        state = None
        error = None
        retryable = True
        action_started = time.monotonic()
        try:
            action = actions.get(claim["task_id"])
            if action is None:
                raise KeyError(f"no action registered for task {claim['task_id']!r}")
            action()  # Never hold a database transaction while user code runs.
            state = "succeeded"
        except Exception as exc:
            state = "failed"
            error = f"{type(exc).__name__}: {exc}"
            retryable = not isinstance(exc, PermanentTaskError)
        finally:
            stop.set()
            heartbeat.join()

        if lost.is_set():
            # Arbitrary Python actions cannot safely be forcibly cancelled. Fencing protects storage.
            return
        duration = time.monotonic() - action_started
        if self._settle_task(claim, state, error, retryable):
            self.metrics.task_attempts.labels(outcome=state).inc()
            self.metrics.task_duration.labels(outcome=state).observe(duration)

    def _settle_task(self, claim, state, error, retryable):
        """Retry only the fenced state write; never rerun the completed action."""
        attempt = 0
        while True:
            try:
                if state == "succeeded":
                    settled = self.store.complete_task(
                        claim["workflow_id"], claim["task_id"], self.worker_id,
                        claim["fencing_token"])
                else:
                    settled = self.store.fail_task(
                        claim["workflow_id"], claim["task_id"], self.worker_id,
                        claim["fencing_token"], error, retryable=retryable)
                # False means fencing/lease validation rejected this write, or
                # an earlier attempt committed and its acknowledgement was lost.
                return settled
            except self._transient_database_errors() as exc:
                attempt += 1
                self.metrics.postgres_failures.labels(
                    operation="complete_task" if state == "succeeded" else "fail_task").inc()
                logger.warning("Could not persist task outcome; retrying without rerunning action",
                               extra={"event": "workflow_database_retry",
                                      "operation": "complete_task" if state == "succeeded" else "fail_task",
                                      "workflow_id": claim["workflow_id"],
                                      "task_id": claim["task_id"],
                                      "fencing_token": claim["fencing_token"],
                                      "error_type": type(exc).__name__})
                self._sleep_database_backoff(attempt)
                try:
                    valid = self.store.renew_lease(
                        claim["workflow_id"], claim["task_id"], self.worker_id,
                        claim["fencing_token"], self.lease_seconds)
                except self._transient_database_errors() as renew_exc:
                    self.metrics.postgres_failures.labels(operation="renew_lease").inc()
                    logger.warning("Could not confirm task lease while retrying its outcome write",
                                   extra={"event": "workflow_database_retry",
                                          "operation": "renew_lease",
                                          "workflow_id": claim["workflow_id"],
                                          "task_id": claim["task_id"],
                                          "error_type": type(renew_exc).__name__})
                    continue
                if not valid:
                    # State may already have committed, or ownership may have
                    # moved. In either case this process must not run the action again.
                    return False
