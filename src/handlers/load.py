"""SQS batch to Postgres.

Triggered by an event source mapping on the ingest queue, which is fed by S3
object-created notifications. One message is one object.

The return value is the contract with SQS. With ReportBatchItemFailures on the
mapping, returning a list of message ids tells SQS to redeliver exactly those
and delete the rest. Returning an empty list deletes the whole batch. Raising
instead of returning fails all ten messages, including the nine that
succeeded, which is how a single bad object turns into a batch that cannot
drain.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import psycopg

from ingest import clients
from ingest.errors import PermanentFailure
from ingest.metrics import MetricBatch
from ingest.records import parse_ndjson
from ingest.repository import ObjectRef, load_object
from ingest.s3source import fetch_object, object_refs

log = logging.getLogger()
log.setLevel(logging.INFO)


def ingest_object(ref: ObjectRef, batch: MetricBatch) -> None:
    body = fetch_object(ref)
    batch.count("object_bytes", len(body), unit="Bytes")

    parsed = parse_ndjson(body)
    result = load_object(
        clients.connection(),
        ref,
        parsed.readings,
        parsed.rejections,
        datetime.now(UTC),
    )

    if result.skipped:
        # A redelivery of something already committed. Normal, and worth
        # counting: a rate that stops being near zero means the visibility
        # timeout is too short for the batch size.
        batch.count("objects_skipped")
        log.info("s3://%s/%s already loaded, skipping", ref.bucket, ref.key)
        return

    batch.count("objects_loaded")
    batch.count("rows_loaded", result.rows_loaded)
    batch.count("rows_rejected", result.rows_rejected)
    log.info(
        "s3://%s/%s loaded %d rows, rejected %d",
        ref.bucket,
        ref.key,
        result.rows_loaded,
        result.rows_rejected,
    )


def handler(event: dict, context: object = None) -> dict:
    batch = MetricBatch(dimensions={"Handler": "load"})
    failures: list[dict[str, str]] = []
    records = event.get("Records", [])
    batch.count("messages_received", len(records))

    for record in records:
        message_id = record.get("messageId", "")
        try:
            for ref in object_refs(record):
                ingest_object(ref, batch)
        except PermanentFailure as exc:
            # Deliberately not added to failures. Another delivery would read
            # the same bytes and fail the same way, and the queue would spend
            # five attempts proving it.
            batch.count("messages_discarded")
            log.warning("message %s discarded: %s", message_id, exc)
        except psycopg.Error:
            # The connection might be the problem, so it does not survive into
            # the next invocation.
            batch.count("messages_failed")
            log.exception("message %s hit the database, will be redelivered", message_id)
            clients.reset_connection()
            failures.append({"itemIdentifier": message_id})
        except Exception:
            batch.count("messages_failed")
            log.exception("message %s failed, will be redelivered", message_id)
            failures.append({"itemIdentifier": message_id})

    batch.flush()
    return {"batchItemFailures": failures}
