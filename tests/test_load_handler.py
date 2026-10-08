import json

import boto3
import pytest
from moto import mock_aws

from handlers import load
from tests.events import object_message, s3_test_event, sqs_event, sqs_record
from tests.pgshim import connect

BUCKET = "ingest-raw"
REGION = "eu-west-1"


def reading(event_id: str, value: float = 20.5) -> str:
    return json.dumps(
        {
            "event_id": event_id,
            "device_id": "d-1",
            "recorded_at": "2026-03-01T10:00:00Z",
            "metric": "temperature_c",
            "value": value,
        }
    )


def ndjson(*event_ids: str) -> bytes:
    return ("\n".join(reading(e) for e in event_ids) + "\n").encode()


@pytest.fixture
def aws():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        yield s3


@pytest.fixture
def db(monkeypatch):
    conn = connect()
    monkeypatch.setattr(load.clients, "connection", lambda: conn)
    yield conn
    conn.close()


def put(s3, key: str, body: bytes) -> str:
    response = s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    return response["ETag"].strip('"')


def test_one_object_loads_and_the_batch_is_reported_clean(aws, db):
    etag = put(aws, "raw/batch-1.ndjson", ndjson("e-1", "e-2"))

    result = load.handler(sqs_event(object_message(BUCKET, "raw/batch-1.ndjson", etag)))

    assert result == {"batchItemFailures": []}
    assert db.query("SELECT count(*) FROM readings")[0][0] == 2
    assert db.query("SELECT rows_loaded FROM ingest_objects") == [(2,)]


def test_a_redelivery_of_the_same_object_is_a_no_op(aws, db):
    etag = put(aws, "raw/batch-1.ndjson", ndjson("e-1"))
    message = sqs_event(object_message(BUCKET, "raw/batch-1.ndjson", etag))

    load.handler(message)
    result = load.handler(message)

    assert result == {"batchItemFailures": []}
    assert db.query("SELECT count(*) FROM readings")[0][0] == 1


def test_the_s3_test_event_is_not_a_failure(aws, db):
    result = load.handler(sqs_event(s3_test_event(BUCKET)))

    assert result == {"batchItemFailures": []}
    assert db.query("SELECT count(*) FROM ingest_objects")[0][0] == 0


def test_a_delete_notification_is_ignored(aws, db):
    from tests.events import s3_envelope

    envelope = s3_envelope(BUCKET, "raw/gone.ndjson", "abc", event_name="ObjectRemoved:Delete")
    result = load.handler(sqs_event(sqs_record(envelope)))

    assert result == {"batchItemFailures": []}
    assert db.query("SELECT count(*) FROM ingest_objects")[0][0] == 0


def test_a_message_body_that_is_not_json_is_discarded_not_retried(aws, db):
    result = load.handler(sqs_event(sqs_record("<html>not json</html>", message_id="m-9")))

    assert result == {"batchItemFailures": []}


def test_a_missing_object_is_discarded_not_retried(aws, db):
    result = load.handler(sqs_event(object_message(BUCKET, "raw/never-written.ndjson", "abc")))

    # NoSuchKey will not become SomeKey on the next attempt.
    assert result == {"batchItemFailures": []}
    assert db.query("SELECT count(*) FROM ingest_objects")[0][0] == 0


def test_bad_rows_are_quarantined_and_the_object_still_commits(aws, db):
    body = ndjson("e-1") + b"garbage\n" + ndjson("e-2")
    etag = put(aws, "raw/mixed.ndjson", body)

    result = load.handler(sqs_event(object_message(BUCKET, "raw/mixed.ndjson", etag)))

    assert result == {"batchItemFailures": []}
    assert db.query("SELECT count(*) FROM readings")[0][0] == 2
    assert db.query("SELECT count(*) FROM rejected_rows")[0][0] == 1
    assert db.query("SELECT rows_loaded, rows_rejected FROM ingest_objects") == [(2, 1)]


def test_the_etag_is_stored_unquoted_whichever_api_it_came_from(aws, db):
    etag = put(aws, "raw/batch-1.ndjson", ndjson("e-1"))
    load.handler(sqs_event(object_message(BUCKET, "raw/batch-1.ndjson", etag)))

    stored = db.query("SELECT etag FROM ingest_objects")[0][0]
    assert '"' not in stored
    assert stored == aws.head_object(Bucket=BUCKET, Key="raw/batch-1.ndjson")["ETag"].strip('"')


def test_an_empty_event_returns_an_empty_failure_list(db):
    assert load.handler({"Records": []}) == {"batchItemFailures": []}
