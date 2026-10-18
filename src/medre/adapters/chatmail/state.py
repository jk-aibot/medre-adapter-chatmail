"""Private native-queue correlation; MEDRE owns delivery lifecycle authority."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path

import msgspec

from medre.core.events.delivery import DeliveryAttemptProvenance


def private_database(path: Path) -> sqlite3.Connection:
    """Create SQLite files with private permissions before writing content."""
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    os.close(fd)
    return sqlite3.connect(path, timeout=1.0, check_same_thread=False)


class ProfileLease:
    """Cross-process profile exclusion using SQLite's portable OS file lock."""

    def __init__(self, accounts_dir: str) -> None:
        profile = Path(accounts_dir).resolve()
        if not profile.is_dir():
            raise ValueError(
                "Chatmail profile is missing; provision a dedicated native profile"
            )
        # The native manager refuses a nonempty directory without its own
        # accounts.toml. Keep the lease beside that directory, and resolve
        # symlinks so aliases cannot acquire separate leases for one profile.
        self.connection = private_database(
            profile.parent / f".{profile.name}.medre-lock.sqlite"
        )
        try:
            self.connection.execute("PRAGMA busy_timeout=0")
            self.connection.execute("BEGIN EXCLUSIVE")
        except BaseException:
            self.connection.close()
            raise RuntimeError(
                "Chatmail profile is in use by another MEDRE command"
            ) from None

    def close(self) -> None:
        self.connection.close()


class PendingLimitError(RuntimeError):
    """The bounded durable queue cannot accept another delivery."""


class Journal:
    """Retain exact attempts until their feedback sink acknowledges them."""

    def __init__(self, state_dir: str, scope: str) -> None:
        self.connection = private_database(Path(state_dir) / "chatmail-pending.sqlite")
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        try:
            with self.connection:
                self.connection.execute(
                    "CREATE TABLE IF NOT EXISTS scope (value TEXT NOT NULL)"
                )
                row = self.connection.execute("SELECT value FROM scope").fetchone()
                if row is None:
                    self.connection.execute("INSERT INTO scope VALUES (?)", (scope,))
                elif row[0] != scope:
                    raise ValueError(
                        "Chatmail state belongs to a different native profile"
                    )
                self.connection.execute(
                    "CREATE TABLE IF NOT EXISTS pending ("
                    "identity TEXT PRIMARY KEY, provenance TEXT NOT NULL, chat INTEGER NOT NULL, "
                    "body TEXT NOT NULL, status TEXT NOT NULL, native INTEGER)"
                )
                # Never replay unsent work using an attempt recovered outside
                # its dispatch lifetime. Native jobs with a correlated ID are
                # polled rather than submitted again.
                self.connection.execute(
                    "UPDATE pending SET status='abandoned' WHERE status IN ('queued', 'submitting')"
                )
        except BaseException:
            self.connection.close()
            raise

    @staticmethod
    def identity(provenance: DeliveryAttemptProvenance) -> str:
        return json.dumps([provenance.outbox_id, provenance.attempt_number])

    def enqueue(
        self, provenance: DeliveryAttemptProvenance, chat: int, body: str, limit: int
    ) -> None:
        identity = self.identity(provenance)
        encoded = msgspec.json.encode(provenance).decode()
        with self.lock, self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            row = self.connection.execute(
                "SELECT provenance, chat, body FROM pending WHERE identity=?",
                (identity,),
            ).fetchone()
            if row:
                if tuple(row) != (encoded, chat, body):
                    raise ValueError(
                        "Chatmail attempt payload contradicts its queued identity"
                    )
                return
            if (
                self.connection.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
                >= limit
            ):
                raise PendingLimitError("Chatmail durable queue is full")
            self.connection.execute(
                "INSERT INTO pending VALUES (?, ?, ?, ?, 'queued', NULL)",
                (identity, encoded, chat, body),
            )

    def rows(self) -> list[dict]:
        with self.lock:
            return [
                dict(row)
                for row in self.connection.execute(
                    "SELECT * FROM pending ORDER BY rowid"
                )
            ]

    def update(self, identity: str, status: str, native: int | None = None) -> None:
        with self.lock, self.connection:
            self.connection.execute(
                "UPDATE pending SET status=?, native=? WHERE identity=?",
                (status, native, identity),
            )

    def remove(self, identity: str) -> None:
        with self.lock, self.connection:
            self.connection.execute("DELETE FROM pending WHERE identity=?", (identity,))

    @staticmethod
    def provenance(row: dict) -> DeliveryAttemptProvenance:
        return msgspec.convert(
            json.loads(row["provenance"]), type=DeliveryAttemptProvenance
        )

    def close(self) -> None:
        with self.lock:
            self.connection.close()
