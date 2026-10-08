"""Every statement that touches Postgres, and the transaction that wraps them.

The connection is a parameter, never a module import. That is what lets the
tests run the real statements against sqlite with no Postgres anywhere, and it
is also what lets the handler hand the same connection to three calls and
commit them together.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from ingest.records import Reading, Rejection


class Cursor(Protocol):
    rowcount: int

    def execute(self, query: str, params: Any = ...) -> Any: ...
    def fetchone(self) -> Any: ...
    def fetchall(self) -> Any: ...


class Connection(Protocol):
    """The slice of psycopg.Connection this module uses."""

    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


@dataclass(frozen=True)
class ObjectRef:
    """One version of one object. The etag is part of the identity.

    An object written twice at the same key is two different ingests, and a
    ledger keyed on the key alone would skip the second one. The etag is used
    as a version token and never as a content hash, because a multipart upload
    produces an etag that is not the MD5 of anything.
    """

    bucket: str
    key: str
    etag: str


@dataclass(frozen=True)
class LoadResult:
    rows_loaded: int
    rows_rejected: int
    skipped: bool = False


CLAIM_OBJECT = """
INSERT INTO ingest_objects (bucket, object_key, etag, rows_loaded, rows_rejected, loaded_at)
VALUES (%s, %s, %s, 0, 0, %s)
ON CONFLICT (bucket, object_key, etag) DO NOTHING
"""

FINALISE_OBJECT = """
UPDATE ingest_objects SET rows_loaded = %s, rows_rejected = %s
WHERE bucket = %s AND object_key = %s AND etag = %s
"""

INSERT_READING = """
INSERT INTO readings (event_id, device_id, recorded_at, metric, value, source_key)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""

INSERT_REJECTION = """
INSERT INTO rejected_rows
    (bucket, object_key, etag, line_number, reason, payload, rejected_at)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

LOADED_ETAGS = """
SELECT bucket, object_key, etag FROM ingest_objects WHERE bucket = %s
"""


def load_object(
    conn: Connection,
    ref: ObjectRef,
    readings: list[Reading],
    rejections: list[Rejection],
    now: datetime,
) -> LoadResult:
    """Load one object's rows and rejections in a single transaction.

    The first statement is the claim on the ledger, and it is doing two jobs.
    It is the idempotency check, because a redelivery of an object that already
    committed gets zero rows back and stops. It is also the mutex, because two
    invocations racing on the same object both try to insert the same primary
    key, and the second one waits on the first rather than double loading.

    Nothing is committed until the counts are written, so a crash anywhere in
    here leaves no ledger row, and the redelivery that follows does the whole
    object again from the start.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(CLAIM_OBJECT, (ref.bucket, ref.key, ref.etag, now))
            if cur.rowcount == 0:
                # Already committed by an earlier delivery. Release the read
                # locks this statement took and leave the data alone.
                conn.rollback()
                return LoadResult(rows_loaded=0, rows_rejected=0, skipped=True)

            loaded = 0
            for reading in readings:
                cur.execute(
                    INSERT_READING,
                    (
                        reading.event_id,
                        reading.device_id,
                        reading.recorded_at,
                        reading.metric,
                        reading.value,
                        ref.key,
                    ),
                )
                # 0 when the event_id was already present from another object.
                loaded += cur.rowcount

            for rejection in rejections:
                cur.execute(
                    INSERT_REJECTION,
                    (
                        ref.bucket,
                        ref.key,
                        ref.etag,
                        rejection.line_number,
                        rejection.reason,
                        rejection.payload,
                        now,
                    ),
                )

            cur.execute(
                FINALISE_OBJECT, (loaded, len(rejections), ref.bucket, ref.key, ref.etag)
            )
    except Exception:
        conn.rollback()
        raise

    conn.commit()
    return LoadResult(rows_loaded=loaded, rows_rejected=len(rejections))


def loaded_object_versions(conn: Connection, bucket: str) -> set[tuple[str, str]]:
    """Every (key, etag) the ledger has committed for a bucket.

    Read by the reconcile handler. It is one query rather than one per object
    because the comparison is a set difference and the ledger for a single
    bucket is small enough to hold.
    """
    with conn.cursor() as cur:
        cur.execute(LOADED_ETAGS, (bucket,))
        return {(row[1], row[2]) for row in cur.fetchall()}
