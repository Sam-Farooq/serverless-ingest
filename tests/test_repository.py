import json
from datetime import UTC, datetime

import pytest

from ingest.records import parse_ndjson
from ingest.repository import ObjectRef, load_object, loaded_object_versions
from tests.pgshim import connect

NOW = datetime(2026, 3, 1, 10, 30, tzinfo=UTC)
REF = ObjectRef(bucket="ingest-raw", key="raw/2026/03/01/batch-1.ndjson", etag="aaa111")


def body(*event_ids: str) -> bytes:
    rows = [
        json.dumps(
            {
                "event_id": event_id,
                "device_id": "d-1",
                "recorded_at": "2026-03-01T10:00:00Z",
                "metric": "temperature_c",
                "value": 20.5,
            }
        )
        for event_id in event_ids
    ]
    return ("\n".join(rows) + "\n").encode()


class ConnectionGone(Exception):
    pass


class FailAfter:
    """Wraps the shim connection and stops talking after N statements."""

    def __init__(self, conn, executes: int) -> None:
        self._conn = conn
        self._budget = executes

    def cursor(self):
        return _FailingCursor(self._conn.cursor(), self)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()


class _FailingCursor:
    def __init__(self, cursor, owner: FailAfter) -> None:
        self._cursor = cursor
        self._owner = owner

    def __enter__(self):
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._cursor.__exit__(*exc_info)

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def execute(self, query: str, params: tuple = ()):
        if self._owner._budget <= 0:
            raise ConnectionGone("connection closed by server")
        self._owner._budget -= 1
        return self._cursor.execute(query, params)

    def fetchall(self):
        return self._cursor.fetchall()


@pytest.fixture
def conn():
    connection = connect()
    yield connection
    connection.close()


def test_a_load_writes_rows_and_one_ledger_row(conn):
    parsed = parse_ndjson(body("e-1", "e-2"))
    result = load_object(conn, REF, parsed.readings, parsed.rejections, NOW)

    assert (result.rows_loaded, result.rows_rejected, result.skipped) == (2, 0, False)
    assert conn.query("SELECT count(*) FROM readings")[0][0] == 2
    ledger = conn.query("SELECT rows_loaded, rows_rejected FROM ingest_objects")
    assert ledger == [(2, 0)]


def test_the_same_object_version_delivered_twice_loads_once(conn):
    parsed = parse_ndjson(body("e-1", "e-2"))
    load_object(conn, REF, parsed.readings, parsed.rejections, NOW)
    second = load_object(conn, REF, parsed.readings, parsed.rejections, NOW)

    assert second.skipped is True
    assert second.rows_loaded == 0
    assert conn.query("SELECT count(*) FROM readings")[0][0] == 2
    assert conn.query("SELECT count(*) FROM ingest_objects")[0][0] == 1


def test_the_same_key_with_a_new_etag_is_a_new_ingest(conn):
    first = parse_ndjson(body("e-1"))
    load_object(conn, REF, first.readings, first.rejections, NOW)

    overwritten = ObjectRef(REF.bucket, REF.key, "bbb222")
    second = parse_ndjson(body("e-2"))
    result = load_object(conn, overwritten, second.readings, second.rejections, NOW)

    assert result.skipped is False
    assert result.rows_loaded == 1
    assert conn.query("SELECT count(*) FROM ingest_objects")[0][0] == 2


def test_an_event_id_seen_in_an_earlier_object_is_not_counted_as_loaded(conn):
    first = parse_ndjson(body("e-1"))
    load_object(conn, REF, first.readings, first.rejections, NOW)

    later = ObjectRef(REF.bucket, "raw/2026/03/01/batch-2.ndjson", "ccc333")
    overlapping = parse_ndjson(body("e-1", "e-9"))
    result = load_object(conn, later, overlapping.readings, overlapping.rejections, NOW)

    # Two rows offered, one already present, so the ledger says one.
    assert result.rows_loaded == 1
    assert conn.query("SELECT rows_loaded FROM ingest_objects WHERE etag = 'ccc333'") == [(1,)]
    assert conn.query("SELECT count(*) FROM readings")[0][0] == 2


def test_rejections_are_persisted_with_their_line_numbers(conn):
    parsed = parse_ndjson(body("e-1") + b"garbage\n")
    result = load_object(conn, REF, parsed.readings, parsed.rejections, NOW)

    assert result.rows_rejected == 1
    rows = conn.query("SELECT line_number, reason, payload FROM rejected_rows")
    assert rows[0][0] == 2
    assert rows[0][1].startswith("invalid json:")
    assert rows[0][2] == "garbage"


def test_an_object_of_only_bad_rows_still_commits_its_ledger_row(conn):
    parsed = parse_ndjson(b"garbage\nmore garbage\n")
    result = load_object(conn, REF, parsed.readings, parsed.rejections, NOW)

    assert (result.rows_loaded, result.rows_rejected) == (0, 2)
    assert conn.query("SELECT rows_loaded, rows_rejected FROM ingest_objects") == [(0, 2)]


def test_the_connection_dropping_midway_leaves_no_ledger_row_and_no_data(conn):
    """The case that decides whether a redelivery is safe.

    If the ledger row survived a failed load, the redelivery would skip an
    object whose rows were never written, and the gap would only surface in a
    reconcile days later.
    """
    parsed = parse_ndjson(body("e-1", "e-2"))
    # Claim, first reading, then the socket goes away.
    failing = FailAfter(conn, executes=2)

    with pytest.raises(ConnectionGone):
        load_object(failing, REF, parsed.readings, parsed.rejections, NOW)

    assert conn.query("SELECT count(*) FROM ingest_objects")[0][0] == 0
    assert conn.query("SELECT count(*) FROM readings")[0][0] == 0
    assert conn.rollbacks == 1
    assert conn.commits == 1  # the one from schema setup, not from the load


def test_loaded_object_versions_returns_key_and_etag_pairs(conn):
    parsed = parse_ndjson(body("e-1"))
    load_object(conn, REF, parsed.readings, parsed.rejections, NOW)

    assert loaded_object_versions(conn, "ingest-raw") == {(REF.key, "aaa111")}
    assert loaded_object_versions(conn, "other-bucket") == set()
