"""Compare what is in the bucket against what the ledger says it loaded.

Run on a schedule by a CloudWatch rule. It exists because every other part of
this path can be correct and the path can still have a hole in it: a
notification configuration edited by hand, a queue policy that stopped
allowing s3.amazonaws.com, a DLQ nobody drained. None of those produce an
error anywhere. They produce an object that was never mentioned to anyone.

It reports and does not re-enqueue. Re-enqueueing would be safe, because loads
are idempotent, and it would still be wrong: the gap is almost always a
configuration change rather than a lost message, and a handler that silently
repairs the symptom removes the only signal that the cause exists. replay is
the lever for putting work back, and a person pulls it.

The comparison is on (key, etag), not on key alone, so an object overwritten
with new content that was never re-ingested shows up too.
"""

from __future__ import annotations

import logging

from ingest import clients
from ingest.config import settings
from ingest.metrics import MetricBatch
from ingest.repository import loaded_object_versions
from ingest.s3source import normalize_etag

log = logging.getLogger()
log.setLevel(logging.INFO)

# Enough keys to act on, not enough to blow the log line budget. The metric
# carries the true count either way.
MAX_KEYS_LOGGED = 20


def bucket_object_versions(bucket: str, prefix: str) -> set[tuple[str, str]]:
    """Every (key, etag) under a prefix, paginated.

    ListObjectsV2 quotes the etag and the event notification does not, so this
    goes through normalize_etag. Without that the two sets never intersect and
    the handler reports the whole bucket as missing, every run.
    """
    paginator = clients.client("s3").get_paginator("list_objects_v2")
    found: set[tuple[str, str]] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith("/") or item["Size"] == 0:
                continue  # a console-created folder marker, not an object
            found.add((item["Key"], normalize_etag(item["ETag"])))
    return found


def handler(event: dict | None = None, context: object = None) -> dict:
    config = settings()
    bucket = (event or {}).get("bucket") or config.bucket
    prefix = (event or {}).get("prefix") or config.prefix
    if not bucket:
        raise RuntimeError("INGEST_BUCKET must be set, or a bucket passed in the event")

    in_bucket = bucket_object_versions(bucket, prefix)
    in_ledger = loaded_object_versions(clients.connection(), bucket)

    unreconciled = sorted(in_bucket - in_ledger)
    # The other direction: a ledger row whose object has since been deleted or
    # overwritten. Not an error, but a count worth watching, because a sudden
    # jump means a lifecycle rule started expiring things earlier than anyone
    # expected.
    orphaned = len(in_ledger - in_bucket)

    batch = MetricBatch(dimensions={"Handler": "reconcile", "Bucket": bucket})
    batch.set("objects_in_bucket", len(in_bucket))
    batch.set("objects_in_ledger", len(in_ledger))
    batch.set("objects_unreconciled", len(unreconciled))
    batch.set("ledger_rows_without_object", orphaned)
    batch.flush()

    for key, etag in unreconciled[:MAX_KEYS_LOGGED]:
        log.warning("s3://%s/%s etag %s is in the bucket, not in the ledger", bucket, key, etag)
    if len(unreconciled) > MAX_KEYS_LOGGED:
        remaining = len(unreconciled) - MAX_KEYS_LOGGED
        log.warning("%d further unreconciled objects not listed", remaining)

    return {
        "bucket": bucket,
        "prefix": prefix,
        "objects_in_bucket": len(in_bucket),
        "objects_in_ledger": len(in_ledger),
        "unreconciled": [key for key, _ in unreconciled],
        "ledger_rows_without_object": orphaned,
    }
