"""The three things sqlite cannot answer, asked of a real Postgres.

The contract suite says the shim and the server agree. This file is the other
half, and unlike the contract it is where they are *expected* to differ: each
case below is one `tests/pgshim.TRANSLATIONS` rewrites away, so the shim's
answer is not a worse version of Postgres's, it is an answer to a different
question.

All of it is marked `live` and skips without INGEST_TEST_DSN.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import live_postgres as live
import pytest

from ingest.records import parse_ndjson
from ingest.repository import CLAIM_OBJECT, ObjectRef, load_object

pytestmark = pytest.mark.live

NOW = datetime(2026, 3, 1, 10, 30, tzinfo=UTC)
REF = ObjectRef(bucket="ingest-raw", key="raw/2026/03/01/batch-1.ndjson", etag="aaa111")


@pytest.fixture
def schema():
    live.psycopg()
    made = live.Schema()
    made.create()
    yield made
    made.drop()


@pytest.fixture
def conn(schema):
    connection = schema.connect()
    yield connection
    connection.close()


def reading(event_id: str, value: float, metric: str = "pressure_hpa", recorded: str | None = None):
    return json.dumps(
        {
            "event_id": event_id,
            "device_id": "d-1",
            "recorded_at": recorded or "2026-03-01T10:00:00Z",
            "metric": metric,
            "value": value,
        }
    )


def load(conn, *lines: str, ref: ObjectRef = REF):
    parsed = parse_ndjson(("\n".join(lines) + "\n").encode())
    return load_object(conn, ref, parsed.readings, parsed.rejections, NOW)


def test_values_are_numeric_so_a_total_carries_no_float_error(conn):
    """The reason the column is NUMERIC and not DOUBLE PRECISION.

    Two readings whose float sum is famously not their decimal sum. Postgres
    stores each as NUMERIC(12,4) and adds them in decimal, so the total is
    exact. sqlite stores both as REAL and answers 0.30000000000000004, which
    is the error that shows up in a daily total and never in a row.
    """
    load(conn, reading("e-1", 800.1), reading("e-2", 800.2))

    total, kind = conn.query("SELECT sum(value), pg_typeof(sum(value)) FROM readings")[0]

    assert kind == "numeric"
    assert total == Decimal("1600.3000")


def test_a_value_finer_than_the_column_is_rounded_by_the_server(conn):
    """Within the metric's valid range, so this is reachable in production.

    pressure_hpa allows 800 to 1100 and the column keeps four decimal places,
    so a producer sending five is silently rounded. That is the right
    behaviour and it is worth pinning, because the shim keeps all of it and a
    test written there would assert the value came back unchanged.
    """
    load(conn, reading("e-1", 1013.25015))

    stored = conn.query("SELECT value FROM readings WHERE event_id = 'e-1'")[0][0]

    assert stored == Decimal("1013.2502")
    assert stored != Decimal("1013.25015")


def test_a_timestamp_keeps_its_instant_across_a_round_trip(conn):
    """TIMESTAMPTZ, which sqlite does not have at all.

    The reading is sent with a +05:00 offset. What comes back has to be the
    same instant and has to be aware. The assertion is on the instant and not
    on the printed offset, because Postgres renders it in the session's
    TimeZone and that differs between this machine and CI.
    """
    load(conn, reading("e-1", 1000.0, recorded="2026-03-01T15:00:00+05:00"))

    stored = conn.query("SELECT recorded_at FROM readings WHERE event_id = 'e-1'")[0][0]

    assert stored.tzinfo is not None
    assert stored == datetime(2026, 3, 1, 10, 0, tzinfo=UTC)
    assert stored.astimezone(timezone(timedelta(hours=5))).hour == 15


def test_the_watermark_comparison_works_across_offsets(conn):
    """What the timezone type is actually for.

    Two readings, one sent as 10:00Z and one as 14:00+05:00, which is 09:00Z
    and therefore earlier despite the larger wall clock. A naive string
    comparison, which is what the shim is doing, puts them the other way
    round.
    """
    load(
        conn,
        reading("e-utc", 1000.0, recorded="2026-03-01T10:00:00Z"),
        reading("e-plus5", 1000.0, recorded="2026-03-01T14:00:00+05:00"),
    )

    earliest = conn.query("SELECT event_id FROM readings ORDER BY recorded_at LIMIT 1")[0][0]

    assert earliest == "e-plus5"


def test_the_ledger_claim_is_a_mutex_and_not_only_a_check(conn, schema):
    """The claim the README makes, driven rather than reasoned about.

    Two loaders race on the same object. The first inserts its claim and does
    not commit. The second runs the same INSERT ... ON CONFLICT DO NOTHING and
    has to **block**, because Postgres takes a speculative insertion lock on
    the key it is about to write. When the first commits, the second sees the
    row and reports zero.

    sqlite cannot show this. It has no row level locking, the second statement
    returns immediately, and both loaders would proceed to write the object's
    rows twice.
    """
    args = (REF.bucket, REF.key, REF.etag, NOW)
    held = conn.cursor()
    held.execute(CLAIM_OBJECT, args)
    assert held.rowcount == 1

    outcome: dict[str, object] = {}

    def second_loader() -> None:
        other = schema.connect()
        try:
            with other.cursor() as cur:
                start = time.monotonic()
                cur.execute(CLAIM_OBJECT, args)
                outcome["waited"] = time.monotonic() - start
                outcome["rowcount"] = cur.rowcount
            other.commit()
        finally:
            other.close()

    racer = threading.Thread(target=second_loader)
    racer.start()
    try:
        time.sleep(0.4)
        # Still waiting, which is the whole assertion: a check that did not
        # lock would have returned by now.
        assert racer.is_alive(), "the second claim did not block on the first"
        conn.commit()
    finally:
        racer.join(timeout=10)

    assert not racer.is_alive()
    assert outcome["rowcount"] == 0
    assert outcome["waited"] >= 0.3

    held.__exit__()
    assert conn.query("SELECT count(*) FROM ingest_objects")[0][0] == 1


def test_a_rolled_back_claim_releases_the_lock_and_the_loser_proceeds(conn, schema):
    """The other side of the same lock, and the one that keeps a crash safe.

    If the first loader dies instead of committing, the second must get the
    claim rather than skip the object. This is what makes the redelivery after
    a crash do the work instead of believing it was already done.
    """
    args = (REF.bucket, REF.key, REF.etag, NOW)
    held = conn.cursor()
    held.execute(CLAIM_OBJECT, args)

    outcome: dict[str, object] = {}

    def second_loader() -> None:
        other = schema.connect()
        try:
            with other.cursor() as cur:
                cur.execute(CLAIM_OBJECT, args)
                outcome["rowcount"] = cur.rowcount
            other.commit()
        finally:
            other.close()

    racer = threading.Thread(target=second_loader)
    racer.start()
    try:
        time.sleep(0.4)
        assert racer.is_alive()
        conn.rollback()
    finally:
        racer.join(timeout=10)

    assert outcome["rowcount"] == 1
    held.__exit__()
    assert conn.query("SELECT count(*) FROM ingest_objects")[0][0] == 1
