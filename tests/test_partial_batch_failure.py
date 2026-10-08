"""The contract with the SQS event source mapping.

With ReportBatchItemFailures enabled on the mapping, the return value decides
what SQS does with ten messages. These tests are the reason the handler
catches per message instead of wrapping the loop: one unreadable object must
not send nine good ones back round.
"""

import json

import boto3
import psycopg
import pytest
from moto import mock_aws

from handlers import load
from tests.events import object_message, sqs_event
from tests.pgshim import connect

BUCKET = "ingest-raw"
REGION = "eu-west-1"


def ndjson(event_id: str) -> bytes:
    return (
        json.dumps(
            {
                "event_id": event_id,
                "device_id": "d-1",
                "recorded_at": "2026-03-01T10:00:00Z",
                "metric": "pressure_hpa",
                "value": 1013.2,
            }
        )
        + "\n"
    ).encode()


@pytest.fixture
def aws():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        yield s3


@pytest.fixture
def db(monkeypatch):
    conn = connect()
    monkeypatch.setattr(load.clients, "connection", lambda: conn)
    yield conn
    conn.close()


def three_messages(s3) -> dict:
    messages = []
    for index in (1, 2, 3):
        key = f"raw/batch-{index}.ndjson"
        etag = s3.put_object(Bucket=BUCKET, Key=key, Body=ndjson(f"e-{index}"))["ETag"]
        messages.append(object_message(BUCKET, key, etag.strip('"'), message_id=f"m-{index}"))
    return sqs_event(*messages)


def test_a_clean_batch_reports_no_failures(aws, db):
    assert load.handler(three_messages(aws)) == {"batchItemFailures": []}
    assert db.query("SELECT count(*) FROM readings")[0][0] == 3


def test_only_the_failed_message_is_sent_back(aws, db, monkeypatch):
    event = three_messages(aws)
    original = load.fetch_object

    def fetch(ref):
        if ref.key.endswith("batch-2.ndjson"):
            raise TimeoutError("read timed out")
        return original(ref)

    monkeypatch.setattr(load, "fetch_object", fetch)

    result = load.handler(event)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-2"}]}
    # The other two committed. SQS deletes them and redelivers only m-2.
    loaded = {row[0] for row in db.query("SELECT object_key FROM ingest_objects")}
    assert loaded == {"raw/batch-1.ndjson", "raw/batch-3.ndjson"}


def test_every_message_failing_reports_every_id_in_order(aws, db, monkeypatch):
    event = three_messages(aws)
    monkeypatch.setattr(
        load, "fetch_object", lambda ref: (_ for _ in ()).throw(TimeoutError("down"))
    )

    result = load.handler(event)

    assert result["batchItemFailures"] == [
        {"itemIdentifier": "m-1"},
        {"itemIdentifier": "m-2"},
        {"itemIdentifier": "m-3"},
    ]


def test_a_permanent_failure_is_not_reported_as_a_batch_item_failure(aws, db):
    event = sqs_event(
        object_message(BUCKET, "raw/missing.ndjson", "abc", message_id="m-1"),
        object_message(
            BUCKET,
            "raw/present.ndjson",
            aws.put_object(
                Bucket=BUCKET, Key="raw/present.ndjson", Body=ndjson("e-7")
            )["ETag"].strip('"'),
            message_id="m-2",
        ),
    )

    result = load.handler(event)

    assert result == {"batchItemFailures": []}
    assert db.query("SELECT object_key FROM ingest_objects") == [("raw/present.ndjson",)]


def test_a_database_error_drops_the_cached_connection(aws, db, monkeypatch):
    event = three_messages(aws)
    dropped: list[bool] = []

    def explode(*args, **kwargs):
        raise psycopg.OperationalError("server closed the connection unexpectedly")

    monkeypatch.setattr(load, "load_object", explode)
    monkeypatch.setattr(load.clients, "reset_connection", lambda: dropped.append(True))

    result = load.handler(event)

    assert len(result["batchItemFailures"]) == 3
    # Once per failed message, so the next invocation cannot inherit it.
    assert dropped == [True, True, True]


def test_the_returned_ids_are_the_message_ids_sqs_sent(aws, db, monkeypatch):
    event = three_messages(aws)
    monkeypatch.setattr(
        load, "fetch_object", lambda ref: (_ for _ in ()).throw(TimeoutError("down"))
    )

    sent = {record["messageId"] for record in event["Records"]}
    returned = {item["itemIdentifier"] for item in load.handler(event)["batchItemFailures"]}

    # An id that is not one SQS sent makes AWS treat the whole batch as failed.
    assert returned <= sent
    assert all(item["itemIdentifier"] for item in load.handler(event)["batchItemFailures"])
