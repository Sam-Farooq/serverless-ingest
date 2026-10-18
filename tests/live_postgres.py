"""Wiring for the tests that need a real Postgres, and the trap in them.

`INGEST_TEST_DSN` points at a server this suite may create a schema in. Unset,
every live test skips, which is what keeps the default run offline and
Docker free.

Each test gets its own schema, named with a uuid and dropped on the way out,
for two reasons. It may be pointed at a shared development server, where a
fixed name would collide with whatever else is on it. And `sql/001_schema.sql`
is applied verbatim, with none of `pgshim.TRANSLATIONS` applied: running the
untranslated DDL is half the point of the tier, because a type the shim
rewrites is a type nothing has ever checked.

The trap is the one the shim has always had and could never report. sqlite
answers every one of these questions wrongly and silently: it stores
`NUMERIC(12,4)` as a float, it has no timezone type, and it has no row level
locking to make `ON CONFLICT` a mutex. A suite that only runs there is green
on all three.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

DSN_ENV = "INGEST_TEST_DSN"
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "001_schema.sql"


def dsn() -> str:
    value = os.environ.get(DSN_ENV)
    if not value:
        pytest.skip(f"{DSN_ENV} is not set, so there is no server to run against")
    return value


def psycopg():
    try:
        import psycopg
    except ModuleNotFoundError:  # pragma: no cover - psycopg is a hard dependency
        pytest.skip("psycopg is not installed")
    return psycopg


class LiveCursor:
    """psycopg's cursor, counted and context managed the way the shim's is."""

    def __init__(self, cursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> LiveCursor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._cursor.close()

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def execute(self, query: str, params: tuple = ()) -> LiveCursor:
        self._cursor.execute(query, params)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()


class LiveConnection:
    """The same surface tests/pgshim.ShimConnection offers, over real Postgres.

    `commits` and `rollbacks` are counted here rather than inferred, because
    the test that matters most asserts a failed load committed nothing, and
    "nothing was written" and "nothing was committed" are different claims.
    """

    def __init__(self, schema: str) -> None:
        pg = psycopg()
        self._conn = pg.connect(dsn())
        self._conn.execute(f'SET search_path TO "{schema}"')
        self._conn.commit()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> LiveCursor:
        return LiveCursor(self._conn.cursor())

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
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


class Schema:
    """One throwaway schema holding the real DDL, dropped on the way out."""

    def __init__(self) -> None:
        self.name = f"ingest_test_{uuid.uuid4().hex[:12]}"

    def create(self) -> None:
        pg = psycopg()
        with pg.connect(dsn(), autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{self.name}"')
            conn.execute(f'SET search_path TO "{self.name}"')
            # Verbatim. No translation, which is the point of this tier.
            conn.execute(SCHEMA_PATH.read_text())

    def drop(self) -> None:
        pg = psycopg()
        with pg.connect(dsn(), autocommit=True) as conn:
            # Named, never a pattern. This may be a shared server.
            conn.execute(f'DROP SCHEMA IF EXISTS "{self.name}" CASCADE')

    def connect(self) -> LiveConnection:
        return LiveConnection(self.name)
