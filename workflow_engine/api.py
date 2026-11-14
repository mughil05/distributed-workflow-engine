"""FastAPI surface for PostgreSQL-backed workflow submission and inspection."""

import base64
import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import redis
from fastapi import FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import ValidationError
from starlette.middleware.cors import CORSMiddleware

from .actions import ActionRegistry, UnknownActionType, configured_action_registry
from .api_schemas import WorkflowSubmission
from .dag import InvalidWorkflow
from .definitions import Task, Workflow
from .observability import EngineMetrics, configure_json_logging
from .postgres import PostgresStore, WorkflowSubmissionConflict, psycopg
from .retries import RetryPolicy

try:
    from psycopg_pool import PoolTimeout
except ImportError:  # Pool support is only needed by the HTTP/worker services.
    PoolTimeout = ()


logger = logging.getLogger(__name__)


class APIProblem(Exception):
    def __init__(self, status_code, code, message):
        self.status_code = status_code
        self.code = code
        self.message = message


def _is_transient_database_error(exc):
    database_errors = (() if psycopg is None else
                       (psycopg.OperationalError, psycopg.InterfaceError,
                        psycopg.errors.QueryCanceled))
    if PoolTimeout:
        database_errors += (PoolTimeout,)
    return bool(database_errors) and isinstance(exc, database_errors)


def _encode_cursor(value):
    raw = json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor):
    if not cursor or len(cursor) > 512:
        raise APIProblem(422, "invalid_cursor", "The pagination cursor is invalid.")
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4),
                               altchars=b"-_", validate=True)
        return json.loads(raw)
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        raise APIProblem(422, "invalid_cursor", "The pagination cursor is invalid.")


def _invalid_cursor():
    return APIProblem(422, "invalid_cursor", "The pagination cursor is invalid.")


def _workflow_cursor(cursor):
    value = _decode_cursor(cursor)
    if not isinstance(value, list) or len(value) != 2:
        raise _invalid_cursor()
    timestamp, workflow_id = value
    if not isinstance(timestamp, str) or not isinstance(workflow_id, str):
        raise _invalid_cursor()
    if not workflow_id or len(workflow_id) > 256 or any(
            ord(char) < 33 or ord(char) > 126 for char in workflow_id):
        raise _invalid_cursor()
    try:
        parsed_timestamp = datetime.fromisoformat(timestamp)
    except (TypeError, ValueError, OverflowError):
        raise _invalid_cursor()
    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
        raise _invalid_cursor()
    return parsed_timestamp, workflow_id


def _task_cursor(cursor):
    value = _decode_cursor(cursor)
    if not isinstance(value, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value):
        raise _invalid_cursor()
    return value


def _attempt_cursor(cursor):
    value = _decode_cursor(cursor)
    if type(value) is not int or value < 1:
        raise _invalid_cursor()
    return value


def _canonical_definition(request_model: WorkflowSubmission, registry: ActionRegistry):
    tasks = []
    definitions = []
    for item in request_model.tasks:
        action = item.action
        params = registry.validate(action.type, action.parameters)
        policy = RetryPolicy(**item.retry_policy.model_dump())
        action_data = {"type": action.type,
                       "parameters": params.model_dump(mode="json", exclude_defaults=False)}
        tasks.append({"id": item.id,
                      "dependencies": sorted(item.dependencies),
                      "action": action_data,
                      "retry_policy": policy.__dict__.copy()})
        definitions.append(Task(item.id, tuple(sorted(item.dependencies)), policy))
    # The persisted representation is canonical so retries compare semantically,
    # regardless of object-key or task/dependency input order.
    tasks.sort(key=lambda task: task["id"])
    workflow = Workflow("validation-only", tuple(definitions))
    from .dag import topological_order
    topological_order(workflow)
    return {"api_definition_version": 1, "tasks": tasks}


def _safe_task_error(state, error):
    if not error:
        return None
    if error == "blocked by unsuccessful dependency":
        return {"code": "dependency_failed", "message": "A dependency did not succeed."}
    if error.startswith("lease expired"):
        return {"code": "lease_expired", "message": "The task lease expired before completion."}
    if state == "dead_lettered":
        return {"code": "attempts_exhausted", "message": "The task exhausted its retry policy."}
    return {"code": "task_execution_failed", "message": "The task action failed."}


def _safe_attempt_error(outcome, error):
    if not error:
        return None
    if outcome == "lease_expired":
        code, message = "lease_expired", "The task lease expired before completion."
    elif outcome == "retry_scheduled":
        code, message = "retry_scheduled", "The attempt failed and a retry was scheduled."
    elif outcome == "dead_lettered":
        code, message = "attempts_exhausted", "The task exhausted its retry policy."
    else:
        code, message = "task_execution_failed", "The task action failed."
    return {"code": code, "message": message}


class RequestBodyLimitMiddleware:
    """Bound request-body memory even when Content-Length is not supplied."""

    def __init__(self, app, max_bytes=256 * 1024):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") not in ("POST", "PUT", "PATCH"):
            await self.app(scope, receive, send)
            return
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        declared = headers.get(b"content-length")
        if declared:
            try:
                if int(declared) > self.max_bytes:
                    await self._too_large(send)
                    return
            except ValueError:
                await self._too_large(send)
                return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > self.max_bytes:
                await self._too_large(send)
                return
            if not message.get("more_body", False):
                break
        replayed = False

        async def replay_receive():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return {"type": "http.disconnect"}

        await self.app(scope, replay_receive, send)

    @staticmethod
    async def _too_large(send):
        body = b'{"error":{"code":"request_too_large","message":"Request body exceeds the configured limit.","request_id":""}}'
        await send({"type": "http.response.start", "status": 413,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


def create_app(*, dsn=None, store=None, redis_client=None,
               registry=None, metrics=None, host=None, api_token=None,
               cors_origins=None, pool=None):
    """Create the API app; tests may inject a real PostgreSQL store/pool."""
    configured_host = host or os.environ.get("WORKFLOW_API_HOST", "127.0.0.1")
    token = api_token if api_token is not None else os.environ.get("WORKFLOW_API_TOKEN")
    loopback = configured_host in ("127.0.0.1", "localhost", "::1")
    if not loopback and not token:
        raise ValueError("WORKFLOW_API_TOKEN is required when binding beyond loopback")
    database_url = dsn or os.environ.get("WORKFLOW_DATABASE_URL")
    action_registry = registry or configured_action_registry()
    process_metrics = metrics or EngineMetrics()
    configured_origins = cors_origins
    if configured_origins is None:
        configured_origins = [item.strip() for item in
                              os.environ.get("WORKFLOW_CORS_ORIGINS", "").split(",")
                              if item.strip()]
    migration_lock = threading.Lock()

    @asynccontextmanager
    async def lifespan(app):
        active_pool = pool
        active_store = store
        active_redis = redis_client
        if active_store is None:
            if not database_url:
                raise RuntimeError("WORKFLOW_DATABASE_URL is required for the API")
            from psycopg_pool import ConnectionPool
            if active_pool is None:
                active_pool = ConnectionPool(
                    conninfo=database_url, min_size=1, max_size=10, timeout=2,
                    kwargs={"options": "-c statement_timeout=3000 -c lock_timeout=500"},
                    open=False,
                )
                active_pool.open(wait=False)
            active_store = PostgresStore(database_url, migrate=False,
                                         connection_pool=active_pool)
        if active_redis is None and os.environ.get("WORKFLOW_REDIS_URL"):
            active_redis = redis.Redis.from_url(
                os.environ["WORKFLOW_REDIS_URL"], socket_connect_timeout=0.25,
                socket_timeout=0.25, health_check_interval=30)
        app.state.store = active_store
        app.state.pool = active_pool
        app.state.redis = active_redis
        app.state.schema_ready = store is not None
        if not app.state.schema_ready:
            try:
                active_store.migrate()
                app.state.schema_ready = True
            except Exception as exc:
                logger.warning("API database schema is not ready", extra={
                    "event": "workflow_api_schema_unavailable",
                    "error_type": type(exc).__name__})
        configure_json_logging()
        try:
            yield
        finally:
            if active_redis is not None and redis_client is None:
                active_redis.close()
            if active_pool is not None and pool is None:
                active_pool.close()

    app = FastAPI(title="Distributed Workflow Orchestrator API", version="1.0.0",
                  lifespan=lifespan)
    app.state.store = store
    app.state.pool = pool
    app.state.redis = redis_client
    app.state.schema_ready = store is not None
    app.state.metrics = process_metrics
    app.state.registry = action_registry
    app.state.api_token = token
    app.add_middleware(RequestBodyLimitMiddleware, max_bytes=256 * 1024)
    if configured_origins:
        app.add_middleware(CORSMiddleware, allow_origins=configured_origins,
                           allow_credentials=False, allow_methods=["GET", "POST"],
                           allow_headers=["Authorization", "Content-Type", "Idempotency-Key"])

    def ensure_store(request):
        active_store = request.app.state.store
        if active_store is None:
            raise APIProblem(503, "postgres_unavailable", "PostgreSQL is unavailable.")
        if not request.app.state.schema_ready:
            if not migration_lock.acquire(blocking=False):
                raise APIProblem(503, "postgres_unavailable", "PostgreSQL is unavailable.")
            try:
                if not request.app.state.schema_ready:
                    active_store.migrate()
                    request.app.state.schema_ready = True
            except Exception as exc:
                if _is_transient_database_error(exc):
                    request.app.state.metrics.postgres_failures.labels(
                        operation="migration").inc()
                    raise APIProblem(503, "postgres_unavailable",
                                     "PostgreSQL is unavailable.")
                raise
            finally:
                migration_lock.release()
        return active_store

    @app.middleware("http")
    async def request_middleware(request: Request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        started = time.monotonic()
        if (request.app.state.api_token and request.url.path not in
                ("/health/live", "/health/ready") and
                request.url.path != "/dashboard" and
                not request.url.path.startswith("/dashboard/")):
            import hmac
            supplied = request.headers.get("authorization", "")
            expected = "Bearer " + request.app.state.api_token
            if not hmac.compare_digest(supplied, expected):
                elapsed = time.monotonic() - started
                app.state.metrics.api_requests.labels(
                    method=request.method, route="/auth", status="401").inc()
                app.state.metrics.api_latency.labels(
                    method=request.method, route="/auth").observe(elapsed)
                response = JSONResponse(status_code=401, content={"error": {
                    "code": "authentication_required",
                    "message": "Valid bearer authentication is required.",
                    "request_id": request_id}})
                response.headers["X-Request-ID"] = request_id
                logger.info("API request completed", extra={
                    "event": "api_request_completed", "request_id": request_id,
                    "method": request.method, "route": "/auth", "status_code": 401,
                    "duration_ms": round(elapsed * 1000, 2)})
                return response
        response = await call_next(request)
        elapsed = time.monotonic() - started
        route = getattr(request.scope.get("route"), "path", "unmatched")
        status = str(response.status_code)
        app.state.metrics.api_requests.labels(
            method=request.method, route=route, status=status).inc()
        app.state.metrics.api_latency.labels(method=request.method, route=route).observe(elapsed)
        response.headers["X-Request-ID"] = request_id
        logger.info("API request completed", extra={
            "event": "api_request_completed", "request_id": request_id,
            "method": request.method, "route": route,
            "status_code": response.status_code,
            "duration_ms": round(elapsed * 1000, 2)})
        return response

    @app.exception_handler(APIProblem)
    async def api_problem_handler(request, exc):
        return JSONResponse(status_code=exc.status_code, content={"error": {
            "code": exc.code, "message": exc.message,
            "request_id": getattr(request.state, "request_id", "")}})

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request, exc):
        errors = exc.errors()
        if any("retry_policy" in tuple(error.get("loc", ())) for error in errors):
            code, message = "invalid_retry_policy", "The retry policy is invalid."
        else:
            code, message = "invalid_request", "The request does not match the API schema."
        return JSONResponse(status_code=422, content={"error": {
            "code": code, "message": message,
            "request_id": getattr(request.state, "request_id", "")}})

    @app.exception_handler(WorkflowSubmissionConflict)
    async def idempotency_conflict_handler(request, _exc):
        return JSONResponse(status_code=409, content={"error": {
            "code": "idempotency_key_conflict",
            "message": "This idempotency key was used for a different workflow definition.",
            "request_id": getattr(request.state, "request_id", "")}})

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request, exc):
        logger.error("Unhandled API error", extra={
            "event": "workflow_api_internal_error", "request_id":
            getattr(request.state, "request_id", ""), "error_type": type(exc).__name__})
        return JSONResponse(status_code=500, content={"error": {
            "code": "internal_error", "message": "The request could not be completed.",
            "request_id": getattr(request.state, "request_id", "")}})

    async def database_error_handler(request, _exc):
        app.state.metrics.postgres_failures.labels(operation="api_request").inc()
        return JSONResponse(status_code=503, content={"error": {
            "code": "postgres_unavailable", "message": "PostgreSQL is unavailable.",
            "request_id": getattr(request.state, "request_id", "")}})

    database_error_types = []
    if psycopg is not None:
        database_error_types.extend((psycopg.OperationalError, psycopg.InterfaceError,
                                     psycopg.errors.QueryCanceled))
    if PoolTimeout:
        database_error_types.append(PoolTimeout)
    for error_type in database_error_types:
        app.add_exception_handler(error_type, database_error_handler)

    @app.get("/health/live")
    def live():
        return {"status": "live"}

    @app.get("/health/ready")
    def ready(request: Request):
        active_store = request.app.state.store
        if active_store is None:
            return JSONResponse(status_code=503, content={"status": "not_ready",
                "dependencies": {"postgres": "unavailable", "redis": "unknown"}})
        try:
            ensure_store(request)
            active_store.health_check()
        except Exception as exc:
            if _is_transient_database_error(exc):
                request.app.state.metrics.postgres_failures.labels(
                    operation="health_check").inc()
                return JSONResponse(status_code=503, content={"status": "not_ready",
                    "dependencies": {"postgres": "unavailable", "redis": "unknown"}})
            raise
        redis_status = "disabled"
        active_redis = request.app.state.redis
        if active_redis is not None:
            try:
                active_redis.ping()
                redis_status = "ok"
            except Exception:
                redis_status = "degraded"
        return {"status": "ready" if redis_status != "degraded" else "degraded",
                "dependencies": {"postgres": "ok", "redis": redis_status,
                                 "redis_required": False}}

    @app.post("/api/v1/workflows", status_code=201)
    def submit_workflow(request: Request,
                        payload: WorkflowSubmission,
                        idempotency_key: str = Header(alias="Idempotency-Key",
                                                       min_length=1, max_length=128)):
        active_store = ensure_store(request)
        if any(ord(char) < 33 or ord(char) > 126 for char in idempotency_key):
            raise APIProblem(422, "invalid_idempotency_key",
                             "The idempotency key contains unsupported characters.")
        try:
            definition = _canonical_definition(payload, request.app.state.registry)
            result = active_store.submit_workflow(idempotency_key, definition)
        except WorkflowSubmissionConflict:
            raise
        except UnknownActionType:
            raise APIProblem(422, "unknown_action_type", "The action type is not registered.")
        except InvalidWorkflow:
            raise APIProblem(422, "invalid_workflow", "The workflow DAG is invalid.")
        except ValidationError:
            raise APIProblem(422, "invalid_action_parameters",
                             "Action parameters do not match the registered action schema.")
        except ValueError as exc:
            # Validation errors do not include user values or arbitrary exception text.
            raise APIProblem(422, "invalid_workflow", "The workflow definition is invalid.") from exc
        except Exception as exc:
            if _is_transient_database_error(exc):
                request.app.state.metrics.postgres_failures.labels(
                    operation="submit_workflow").inc()
                raise APIProblem(503, "postgres_unavailable",
                                 "PostgreSQL is unavailable.")
            raise
        request.app.state.metrics.workflow_submissions.labels(
            result="created" if result["created"] else "idempotent_replay").inc()
        if not result["created"]:
            response_status = 200
        else:
            response_status = 201
        response = JSONResponse(status_code=response_status, content={
            "workflow_id": result["workflow_id"], "state": "pending",
            "created_at": result["created_at"].isoformat(),
            "status_url": f"/api/v1/workflows/{result['workflow_id']}"})
        if not result["created"]:
            response.headers["Idempotency-Replayed"] = "true"
        return response

    @app.get("/api/v1/workflows")
    def list_workflows(request: Request,
                       limit: int = Query(default=50, ge=1, le=100),
                       state: str = Query(default=None,
                                          pattern="^(pending|running|succeeded|failed)$"),
                       cursor: str = Query(default=None, max_length=512)):
        active_store = ensure_store(request)
        before = None
        if cursor:
            before = _workflow_cursor(cursor)
        rows = active_store.list_workflows(limit=limit + 1, state=state, before=before)
        more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = None
        if more and rows:
            next_cursor = _encode_cursor([rows[-1]["created_at"].isoformat(),
                                          rows[-1]["workflow_id"]])
        return {"items": [{"workflow_id": row["workflow_id"], "state": row["state"],
                           "created_at": row["created_at"].isoformat(),
                           "updated_at": row["updated_at"].isoformat()} for row in rows],
                "next_cursor": next_cursor}

    @app.get("/api/v1/workflows/{workflow_id}")
    def get_workflow(workflow_id: str, request: Request):
        active_store = ensure_store(request)
        try:
            row = active_store.workflow_detail(workflow_id)
        except Exception as exc:
            if _is_transient_database_error(exc):
                request.app.state.metrics.postgres_failures.labels(
                    operation="workflow_detail").inc()
                raise APIProblem(503, "postgres_unavailable", "PostgreSQL is unavailable.")
            raise
        if row is None:
            raise APIProblem(404, "workflow_not_found", "The workflow does not exist.")
        counts = {state: int(row["task_counts"].get(state, 0)) for state in
                  ("pending", "running", "succeeded", "failed", "dead_lettered")}
        return {"workflow_id": row["workflow_id"], "state": row["state"],
                "error": _safe_task_error(row["state"], row["error"]),
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
                "task_counts": counts, "task_count": sum(counts.values())}

    @app.get("/api/v1/workflows/{workflow_id}/tasks")
    def list_tasks(workflow_id: str, request: Request,
                   limit: int = Query(default=50, ge=1, le=100),
                   cursor: str = Query(default=None, max_length=512)):
        active_store = ensure_store(request)
        if active_store.workflow_state(workflow_id) is None:
            raise APIProblem(404, "workflow_not_found", "The workflow does not exist.")
        after = _task_cursor(cursor) if cursor else None
        rows = active_store.list_tasks(workflow_id, limit=limit + 1, after_task_id=after)
        more = len(rows) > limit
        rows = rows[:limit]
        items = []
        for row in rows:
            item = {"task_id": row["task_id"], "state": row["state"],
                    "dependencies": row["dependencies"],
                    "attempt_count": row["attempt_count"],
                    "max_attempts": row["max_attempts"],
                    "retry_eligible_at": (row["next_eligible_at"].isoformat()
                        if row["state"] == "pending" and row["attempt_count"] else None),
                    "dead_lettered_at": (row["dead_lettered_at"].isoformat()
                        if row["dead_lettered_at"] else None),
                    "error": _safe_task_error(row["state"], row["error"])}
            items.append(item)
        next_cursor = _encode_cursor(rows[-1]["task_id"]) if more and rows else None
        return {"items": items, "next_cursor": next_cursor}

    @app.get("/api/v1/workflows/{workflow_id}/tasks/{task_id}")
    def get_task(workflow_id: str, task_id: str, request: Request):
        active_store = ensure_store(request)
        if active_store.workflow_state(workflow_id) is None:
            raise APIProblem(404, "workflow_not_found", "The workflow does not exist.")
        row = active_store.task_detail(workflow_id, task_id)
        if row is None:
            raise APIProblem(404, "task_not_found", "The task does not exist.")
        return {"task_id": row["task_id"], "state": row["state"],
                "dependencies": row["dependencies"],
                "attempt_count": row["attempt_count"],
                "max_attempts": row["max_attempts"],
                "retry_eligible_at": (row["next_eligible_at"].isoformat()
                    if row["state"] == "pending" and row["attempt_count"] else None),
                "dead_lettered_at": (row["dead_lettered_at"].isoformat()
                    if row["dead_lettered_at"] else None),
                "lease_expires_at": (row["lease_expires_at"].isoformat()
                    if row.get("lease_expires_at") else None),
                "error": _safe_task_error(row["state"], row["error"]),
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat()}

    @app.get("/api/v1/workflows/{workflow_id}/tasks/{task_id}/attempts")
    def list_attempts(workflow_id: str, task_id: str, request: Request,
                      limit: int = Query(default=50, ge=1, le=100),
                      cursor: str = Query(default=None, max_length=512)):
        active_store = ensure_store(request)
        if active_store.workflow_state(workflow_id) is None:
            raise APIProblem(404, "workflow_not_found", "The workflow does not exist.")
        if active_store.task_detail(workflow_id, task_id) is None:
            raise APIProblem(404, "task_not_found", "The task does not exist.")
        after = _attempt_cursor(cursor) if cursor else None
        rows = active_store.list_attempts(workflow_id, task_id, limit=limit + 1,
                                          after_attempt_number=after)
        more = len(rows) > limit
        rows = rows[:limit]
        items = [{"attempt_number": row["attempt_number"],
                  "claimed_at": row["claimed_at"].isoformat(),
                  "lease_expires_at": row["lease_expires_at"].isoformat(),
                  "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
                  "outcome": row["outcome"],
                  "error": _safe_attempt_error(row["outcome"], row["error"])}
                 for row in rows]
        next_cursor = _encode_cursor(rows[-1]["attempt_number"]) if more and rows else None
        return {"items": items, "next_cursor": next_cursor}

    @app.get("/metrics")
    def metrics_endpoint(request: Request):
        active_store = request.app.state.store
        if active_store is not None and request.app.state.schema_ready:
            try:
                request.app.state.metrics.refresh_database_snapshot(active_store)
            except Exception as exc:
                if _is_transient_database_error(exc):
                    request.app.state.metrics.postgres_failures.labels(
                        operation="metrics_snapshot").inc()
                else:
                    logger.warning("Could not refresh cached metrics snapshot", extra={
                        "event": "workflow_metrics_snapshot_failed",
                        "error_type": type(exc).__name__})
        return Response(content=generate_latest(request.app.state.metrics.registry),
                        media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/v1/summary")
    def dashboard_summary(request: Request):
        active_store = ensure_store(request)
        try:
            request.app.state.metrics.refresh_database_snapshot(active_store)
        except Exception as exc:
            if _is_transient_database_error(exc):
                request.app.state.metrics.postgres_failures.labels(
                    operation="dashboard_summary").inc()
            else:
                raise
        cached = request.app.state.metrics.cached_database_snapshot()
        if cached is None:
            raise APIProblem(503, "postgres_unavailable",
                             "A database summary is not available yet.")
        snapshot = cached["snapshot"]
        return {
            "workflows": {state: int(snapshot["workflows"].get(state, 0))
                          for state in ("pending", "running", "succeeded", "failed")},
            "attempts": {outcome: int(snapshot["attempts"].get(outcome, 0))
                         for outcome in ("running", "succeeded", "retry_scheduled", "failed",
                                         "dead_lettered", "lease_expired")},
            "active_leases": int(snapshot["active_leases"]),
            "retry_scheduled_attempts": int(
                snapshot["attempts"].get("retry_scheduled", 0)),
            "dead_lettered_tasks": int(snapshot["dead_lettered_tasks"]),
            "snapshot_as_of": cached["as_of"].isoformat(),
            "snapshot_age_seconds": round(cached["age_seconds"], 3),
            "snapshot_stale": cached["stale"],
        }

    dashboard_directory = Path(__file__).resolve().parent.parent / "dashboard" / "dist"
    if dashboard_directory.is_dir():
        app.mount("/dashboard", StaticFiles(directory=dashboard_directory, html=True),
                  name="dashboard")

    return app
