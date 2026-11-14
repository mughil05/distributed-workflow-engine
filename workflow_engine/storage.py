"""SQLite persistence with database-wide, lifetime process ownership."""

import hashlib
import errno
import json
import os
import sqlite3
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Optional


class WorkflowAlreadyRunning(RuntimeError):
    """Raised when another process owns the database."""


class WorkflowLockUnavailable(RuntimeError):
    """Raised when safe database ownership cannot be established."""


class _FileLock:
    def __init__(self, path: Path) -> None:
        flags = os.O_RDWR | os.O_CREAT
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags, 0o600)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                os.close(fd)
                raise WorkflowLockUnavailable(f"lock is not a regular file: {path}")
            self.handle = os.fdopen(fd, "r+b", buffering=0)
        except Exception as exc:
            if isinstance(exc, WorkflowLockUnavailable):
                raise
            raise WorkflowLockUnavailable(f"cannot open ownership lock {path}: {exc}") from exc
        try:
            if os.name == "nt":
                import msvcrt
                if os.fstat(self.handle.fileno()).st_size == 0:
                    self.handle.write(b"\0")
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                self._unlock = lambda: (self.handle.seek(0), msvcrt.locking(
                    self.handle.fileno(), msvcrt.LK_UNLCK, 1))
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._unlock = lambda: fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        except BlockingIOError as exc:
            self.handle.close()
            raise WorkflowAlreadyRunning("another process owns this SQLite database") from exc
        except OSError as exc:
            self.handle.close()
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise WorkflowAlreadyRunning("another process owns this SQLite database") from exc
            raise WorkflowLockUnavailable(f"filesystem locking failed: {exc}") from exc

    def close(self) -> None:
        try:
            self._unlock()
        finally:
            self.handle.close()


class SQLiteStore:
    VALID_STATES = {"pending", "running", "succeeded", "failed"}
    LOCK_DIR_ENV = "WORKFLOW_ENGINE_LOCK_DIR"

    def __init__(self, path: str) -> None:
        self.path = str(path)
        self.connection = None
        self._locks = []
        self._database_path = None
        self.database_identity = None
        try:
            if self.path == ":memory:":
                self.connection = self._connect(self.path)
                self.connection.row_factory = sqlite3.Row
                self.connection.execute("PRAGMA foreign_keys = ON")
                self.connection.executescript("""
                    CREATE TABLE IF NOT EXISTS workflows (
                        workflow_id TEXT PRIMARY KEY, state TEXT NOT NULL,
                        definition TEXT NOT NULL, error TEXT
                    );
                    CREATE TABLE IF NOT EXISTS tasks (
                        workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
                        task_id TEXT NOT NULL, state TEXT NOT NULL, error TEXT,
                        PRIMARY KEY (workflow_id, task_id)
                    );
                """)
                self.connection.commit()
            else:
                candidate = Path(self.path).expanduser()
                candidate.parent.mkdir(parents=True, exist_ok=True)
                self._database_path = candidate.resolve(strict=False)
                self._validate_lock_dir()
                # Every opener takes the canonical path lock before inspecting the
                # file. Otherwise a contender can see a newly-created inode and
                # bypass the creator's still-held bootstrap lock.
                bootstrap = self._acquire(self._key("path", os.path.normcase(str(self._database_path))))
                self._locks.append(bootstrap)
                existed = self._database_path.exists()
                if existed:
                    self._validate_link_count(self._database_path)
                    identity = self._identity(self._database_path)
                    if self._identity(self._database_path) != identity:
                        raise WorkflowLockUnavailable("database identity changed while acquiring ownership")
                    self._locks.append(self._acquire(self._key("inode", identity)))
                    self._validate_link_count(self._database_path)
                    self.connection = self._connect(str(self._database_path))
                    opened = self._identity(self._database_path)
                    if opened != identity:
                        raise WorkflowLockUnavailable("database was replaced while opening SQLite")
                    self.database_identity = identity
                else:
                    self.connection = self._connect(str(self._database_path))
                    identity = self._identity(self._database_path)
                    self._locks.append(self._acquire(self._key("inode", identity)))
                    if self._identity(self._database_path) != identity:
                        raise WorkflowLockUnavailable("database identity changed during ownership bootstrap")
                    self._validate_link_count(self._database_path)
                    self.database_identity = identity
                self.connection.row_factory = sqlite3.Row
                self._check_identity()
                self.connection.execute("PRAGMA foreign_keys = ON")
                self.connection.executescript("""
                    CREATE TABLE IF NOT EXISTS workflows (
                        workflow_id TEXT PRIMARY KEY, state TEXT NOT NULL,
                        definition TEXT NOT NULL, error TEXT
                    );
                    CREATE TABLE IF NOT EXISTS tasks (
                        workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
                        task_id TEXT NOT NULL, state TEXT NOT NULL, error TEXT,
                        PRIMARY KEY (workflow_id, task_id)
                    );
                """)
                self.connection.commit()
        except BaseException:
            self._cleanup()
            raise

    @classmethod
    def _lock_dir(cls) -> Path:
        configured = os.environ.get(cls.LOCK_DIR_ENV)
        if configured:
            return Path(configured).expanduser().absolute()
        # Per-user local temporary storage coordinates hard links in unrelated
        # parent directories without depending on HOME.
        if hasattr(os, "getuid"):
            return Path(tempfile.gettempdir()) / f"workflow-engine-locks-{os.getuid()}"
        return Path(tempfile.gettempdir()) / "workflow-engine-locks"

    def _validate_lock_dir(self) -> None:
        root = self._lock_dir()
        try:
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = root.lstat()
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise WorkflowLockUnavailable(f"unsafe ownership lock directory: {root}")
            if hasattr(os, "getuid") and (info.st_uid != os.getuid() or info.st_mode & 0o077):
                raise WorkflowLockUnavailable(f"ownership lock directory must be private and user-owned: {root}")
        except WorkflowLockUnavailable:
            raise
        except OSError as exc:
            raise WorkflowLockUnavailable(f"cannot use ownership lock directory {root}: {exc}") from exc

    @staticmethod
    def _identity(path: Path):
        try:
            info = path.stat()
        except OSError as exc:
            raise WorkflowLockUnavailable(f"cannot inspect database identity: {exc}") from exc
        if not stat.S_ISREG(info.st_mode) or not getattr(info, "st_ino", 0):
            raise WorkflowLockUnavailable("database must be a regular file with stable device/inode identity")
        return int(info.st_dev), int(info.st_ino)

    @staticmethod
    def _validate_link_count(path: Path) -> None:
        """Reject SQLite files whose path-specific WAL sidecars can diverge."""
        try:
            info = path.stat()
        except OSError as exc:
            raise WorkflowLockUnavailable(f"cannot inspect SQLite database link count: {exc}") from exc
        links = getattr(info, "st_nlink", None)
        if not isinstance(links, int) or links < 1:
            raise WorkflowLockUnavailable(
                "cannot reliably determine SQLite database hard-link count; "
                "this platform or filesystem is unsupported"
            )
        if links != 1:
            raise WorkflowLockUnavailable(
                f"SQLite database has {links} hard links; hard-linked database files "
                "are unsupported (including in WAL mode). Remove the extra link or "
                "copy the database to a single path before opening it."
            )

    @classmethod
    def _key(cls, kind: str, value: object) -> Path:
        digest = hashlib.sha256(f"{kind}:{value}".encode()).hexdigest()
        return cls._lock_dir() / f"{digest}.lock"

    @classmethod
    def _acquire(cls, path: Path) -> _FileLock:
        try:
            return _FileLock(path)
        except WorkflowAlreadyRunning:
            raise
        except WorkflowLockUnavailable:
            raise
        except OSError as exc:
            raise WorkflowLockUnavailable(f"cannot acquire ownership lock {path}: {exc}") from exc

    @staticmethod
    def _connect(path: str):
        try:
            return sqlite3.connect(path)
        except sqlite3.Error:
            raise

    def _check_identity(self) -> None:
        if self._database_path is not None:
            if self._identity(self._database_path) != self.database_identity:
                raise WorkflowLockUnavailable("database path identity changed while store is open")
            self._validate_link_count(self._database_path)

    def initialize(self, workflow_id: str, definition: Dict[str, Any], task_ids: list) -> None:
        self._check_identity()
        encoded = json.dumps(definition, sort_keys=True)
        row = self.connection.execute("SELECT definition FROM workflows WHERE workflow_id=?", (workflow_id,)).fetchone()
        if row and row["definition"] != encoded:
            raise ValueError(f"workflow ID {workflow_id!r} already has a different definition")
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO workflows(workflow_id,state,definition) VALUES(?,?,?)", (workflow_id, "pending", encoded))
            for task_id in task_ids:
                self.connection.execute("INSERT OR IGNORE INTO tasks(workflow_id,task_id,state) VALUES(?,?,?)", (workflow_id, task_id, "pending"))

    @contextmanager
    def workflow_lock(self, workflow_id: str) -> Iterator[None]:
        """Compatibility context; database ownership is held for store lifetime."""
        self._check_identity()
        yield

    def set_task(self, workflow_id: str, task_id: str, state: str, error: Optional[str] = None) -> None:
        self._check_identity()
        if state not in self.VALID_STATES:
            raise ValueError(f"invalid state: {state}")
        with self.connection:
            self.connection.execute("UPDATE tasks SET state=?, error=? WHERE workflow_id=? AND task_id=?", (state, error, workflow_id, task_id))

    def set_workflow(self, workflow_id: str, state: str, error: Optional[str] = None) -> None:
        self._check_identity()
        if state not in self.VALID_STATES:
            raise ValueError(f"invalid state: {state}")
        with self.connection:
            self.connection.execute("UPDATE workflows SET state=?, error=? WHERE workflow_id=?", (state, error, workflow_id))

    def recover_running(self, workflow_id: str) -> None:
        self._check_identity()
        with self.connection:
            self.connection.execute("UPDATE tasks SET state='pending', error=NULL WHERE workflow_id=? AND state='running'", (workflow_id,))

    def task_rows(self, workflow_id: str) -> Dict[str, Dict[str, Any]]:
        self._check_identity()
        rows = self.connection.execute("SELECT task_id,state,error FROM tasks WHERE workflow_id=?", (workflow_id,))
        return {row["task_id"]: dict(row) for row in rows}

    def workflow_row(self, workflow_id: str) -> Dict[str, Any]:
        self._check_identity()
        row = self.connection.execute("SELECT * FROM workflows WHERE workflow_id=?", (workflow_id,)).fetchone()
        return dict(row) if row else {}

    def _cleanup(self) -> None:
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection = None
        for lock in reversed(self._locks):
            try:
                lock.close()
            except Exception:
                pass
        self._locks.clear()

    def close(self) -> None:
        self._cleanup()
