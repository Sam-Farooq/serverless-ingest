"""Run the real SQL against sqlite, so the tests need no Postgres.

What this buys: the statements in ``ingest.repository`` are the statements the
tests execute. The conflict clauses, the primary keys and the transaction
boundaries are all exercised for real, and a typo in a column name fails a
test rather than a deployment.

What it does not buy: anything dialect-specific. sqlite is not checking
NUMERIC precision, timezone handling, row-level locking, or the speculative
insertion lock that makes the ledger claim a mutex under real concurrency.
Those are Postgres behaviour and this harness says nothing about them.

Since 0.8.0 that is no longer the end of the sentence. The statements here
are also run against a real Postgres by
``tests/test_repository_contract.py``, and the four behaviours above are
asserted against one in ``tests/test_postgres_live.py``. The three
translations below are each a question this shim answers differently, so each
has a test over there holding Postgres's answer.

The translation is three rules, listed in TRANSLATIONS below. If a fourth one
is ever needed, test_schema_translation.py is the thing that will say so.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "001_schema.sql"

TRANSLATIONS: list[tuple[str, str]] = [
    (r"\bBIGSERIAL\s+PRIMARY\s+KEY\b", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    (r"\bTIMESTAMPTZ\b", "TEXT"),
    (r"\bNUMERIC\s*\(\s*\d+\s*,\s*\d+\s*\)", "REAL"),
]


def translate_ddl(ddl: str) -> str:
    for pattern, replacement in TRANSLATIONS:
        ddl = re.sub(pattern, replacement, ddl, flags=re.IGNORECASE)
    return ddl


class ShimCursor:
    """A sqlite cursor that accepts psycopg's %s placeholders and closes itself."""

    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> ShimCursor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._cursor.close()

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def execute(self, query: str, params: tuple = ()) -> ShimCursor:
        self._cursor.execute(query.replace("%s", "?"), params)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def executescript(self, script: str) -> None:
        self._cursor.executescript(script)


class ShimConnection:
    """psycopg's Connection surface, as far as ingest.repository uses it."""

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> ShimCursor:
        return ShimCursor(self._conn.cursor())

    def commit(self) -> None:
        self.commits += 1
        self._conn.commit()

    def rollback(self) -> None:
        self.rollbacks += 1
        self._conn.rollback()

    def close(self) -> None:
        self.closed = True
        self._conn.close()

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        """Test-side read. Not part of the psycopg surface."""
        cur = self._conn.execute(sql.replace("%s", "?"), params)
        try:
            return cur.fetchall()
        finally:
            cur.close()


def _register_adapters() -> None:
    # sqlite3's built-in datetime adapter is deprecated and scheduled for
    # removal, so the shim declares its own rather than depending on it.
    sqlite3.register_adapter(datetime, lambda value: value.isoformat())


def connect() -> ShimConnection:
    _register_adapters()
    conn = ShimConnection()
    with conn.cursor() as cur:
        cur.executescript(translate_ddl(SCHEMA_PATH.read_text()))
    conn.commit()
    return conn
