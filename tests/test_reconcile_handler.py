import json
from datetime import UTC, datetime

import boto3
import pytest
from moto import mock_aws

from handlers import reconcile
from ingest.config import settings
from ingest.records import parse_ndjson
from ingest.repository import ObjectRef, load_object
from tests.pgshim import connect

BUCKET = "ingest-raw"
REGION = "eu-west-1"
NOW = datetime(2026, 3, 1, tzinfo=UTC)


def ndjson(event_id: str) -> bytes:
    return (
        json.dumps(
            {
                "event_id": event_id,
                "device_id": "d-1",
                "recorded_at": "2026-03-01T10:00:00Z",
                "metric": "humidity_pct",
                "value": 44.0,
            }
        )
        + "\n"
    ).encode()


@pytest.fixture
def aws(monkeypatch):
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
        monkeypatch.setenv("INGEST_BUCKET", BUCKET)
        monkeypatch.setenv("INGEST_PREFIX", "raw/")
        settings.cache_clear()
        yield s3


@pytest.fixture
def db(monkeypatch):
    conn = connect()
    monkeypatch.setattr(reconcile.clients, "connection", lambda: conn)
    yield conn
    conn.close()


def upload_and_record(s3, db, key: str, event_id: str) -> str:
    """Put an object in the bucket and tell the ledger it was loaded."""
    etag = s3.put_object(Bucket=BUCKET, Key=key, Body=ndjson(event_id))["ETag"].strip('"')
    parsed = parse_ndjson(ndjson(event_id))
    load_object(db, ObjectRef(BUCKET, key, etag), parsed.readings, parsed.rejections, NOW)
    return etag


def test_a_fully_loaded_bucket_reconciles_to_zero(aws, db):
    upload_and_record(aws, db, "raw/a.ndjson", "e-1")
    upload_and_record(aws, db, "raw/b.ndjson", "e-2")

    result = reconcile.handler({})

    assert result["objects_in_bucket"] == 2
    assert result["objects_in_ledger"] == 2
    assert result["unreconciled"] == []
    assert result["ledger_rows_without_object"] == 0


def test_an_object_the_ledger_never_saw_is_reported(aws, db):
    upload_and_record(aws, db, "raw/a.ndjson", "e-1")
    aws.put_object(Bucket=BUCKET, Key="raw/missed.ndjson", Body=ndjson("e-9"))

    result = reconcile.handler({})

    assert result["unreconciled"] == ["raw/missed.ndjson"]
    assert result["objects_in_bucket"] == 2
    assert result["objects_in_ledger"] == 1


def test_an_object_overwritten_after_loading_is_reported(aws, db):
    upload_and_record(aws, db, "raw/a.ndjson", "e-1")
    # Same key, different bytes, so a different etag and a second ingest that
    # never happened.
    aws.put_object(Bucket=BUCKET, Key="raw/a.ndjson", Body=ndjson("e-2"))

    result = reconcile.handler({})

    assert result["unreconciled"] == ["raw/a.ndjson"]
    assert result["ledger_rows_without_object"] == 1


def test_a_deleted_object_counts_as_a_ledger_row_without_an_object(aws, db):
    upload_and_record(aws, db, "raw/a.ndjson", "e-1")
    aws.delete_object(Bucket=BUCKET, Key="raw/a.ndjson")

    result = reconcile.handler({})

    assert result["unreconciled"] == []
    assert result["ledger_rows_without_object"] == 1


def test_objects_outside_the_prefix_are_not_compared(aws, db):
    upload_and_record(aws, db, "raw/a.ndjson", "e-1")
    aws.put_object(Bucket=BUCKET, Key="archive/old.ndjson", Body=ndjson("e-5"))

    result = reconcile.handler({})

    assert result["objects_in_bucket"] == 1
    assert result["unreconciled"] == []


def test_a_zero_byte_folder_marker_is_not_an_object(aws, db):
    aws.put_object(Bucket=BUCKET, Key="raw/2026/", Body=b"")

    result = reconcile.handler({})

    assert result["objects_in_bucket"] == 0
    assert result["unreconciled"] == []


def test_the_listing_pages_past_the_thousand_key_default(aws, db):
    for index in range(1001):
        aws.put_object(Bucket=BUCKET, Key=f"raw/bulk/{index:05d}.ndjson", Body=ndjson(f"e-{index}"))

    result = reconcile.handler({})

    # A single list_objects_v2 call returns at most 1000 keys. Without the
    # paginator the last one would be invisible and the count would be wrong.
    assert result["objects_in_bucket"] == 1001
    assert len(result["unreconciled"]) == 1001


def test_the_bucket_can_be_overridden_by_the_event(aws, db):
    aws.create_bucket(Bucket="other-raw", CreateBucketConfiguration={"LocationConstraint": REGION})
    aws.put_object(Bucket="other-raw", Key="raw/x.ndjson", Body=ndjson("e-1"))

    result = reconcile.handler({"bucket": "other-raw"})

    assert result["bucket"] == "other-raw"
    assert result["unreconciled"] == ["raw/x.ndjson"]


def test_no_bucket_anywhere_is_an_error_not_an_empty_report(aws, db, monkeypatch):
    monkeypatch.setenv("INGEST_BUCKET", "")
    settings.cache_clear()

    with pytest.raises(RuntimeError, match="INGEST_BUCKET"):
        reconcile.handler({})


def test_only_the_first_twenty_gaps_are_logged(aws, db, caplog):
    for index in range(25):
        aws.put_object(Bucket=BUCKET, Key=f"raw/{index:03d}.ndjson", Body=ndjson(f"e-{index}"))

    result = reconcile.handler({})

    assert len(result["unreconciled"]) == 25
    logged = [line for line in caplog.text.splitlines() if "not in the ledger" in line]
    assert len(logged) == reconcile.MAX_KEYS_LOGGED
    assert "5 further unreconciled objects not listed" in caplog.text
