"""Reading the S3 side of the event: what arrived, and where its bytes are."""

from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import ClientError

from ingest import clients
from ingest.errors import PermanentFailure
from ingest.repository import ObjectRef

# S3 posts one of these to the queue the moment a notification configuration
# is saved. It has no Records key, so a handler that assumes one fails on the
# very first message, retries it to the maximum, and parks it in the dead
# letter queue before a single real object has arrived.
TEST_EVENT = "s3:TestEvent"

# Codes where the object is not coming back. Everything else, including
# SlowDown, InternalError and a plain timeout, is worth another delivery.
PERMANENT_S3_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "AccessDenied", "404"})


def normalize_etag(etag: str) -> str:
    """Strip the quotes that only some of the S3 APIs add.

    An event notification carries ``d41d8c...``. ListObjectsV2 and HeadObject
    carry ``"d41d8c..."``, quotes included. The ledger is keyed on this value,
    so one unquoted write and one quoted read would make every object look
    unreconciled forever.
    """
    return etag.strip().strip('"')


def object_refs(record: dict[str, Any]) -> list[ObjectRef]:
    """Pull the object references out of one SQS message.

    A message body is an S3 event envelope, which is a list even though S3
    populates it with one record. Anything that cannot be read as that is a
    permanent failure: the bytes on the queue will not improve.
    """
    try:
        envelope = json.loads(record["body"])
    except KeyError as exc:
        raise PermanentFailure("message has no body") from exc
    except json.JSONDecodeError as exc:
        raise PermanentFailure(f"message body is not json: {exc.msg}") from exc

    if envelope.get("Event") == TEST_EVENT:
        return []

    refs: list[ObjectRef] = []
    for entry in envelope.get("Records", []):
        if not entry.get("eventName", "").startswith("ObjectCreated"):
            continue
        section = entry.get("s3")
        if not section:
            raise PermanentFailure("event record has no s3 section")
        refs.append(
            ObjectRef(
                bucket=section["bucket"]["name"],
                key=section["object"]["key"],
                etag=normalize_etag(section["object"].get("eTag", "")),
            )
        )
    return refs


def fetch_object(ref: ObjectRef) -> bytes:
    """Read an object, classifying the ways S3 says no."""
    try:
        response = clients.client("s3").get_object(Bucket=ref.bucket, Key=ref.key)
        return response["Body"].read()
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in PERMANENT_S3_CODES:
            raise PermanentFailure(f"s3://{ref.bucket}/{ref.key}: {code}") from exc
        raise
