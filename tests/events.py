"""Synthetic Lambda events, built to the shape AWS actually sends.

The fields that matter are the ones the handlers read. messageId has to be
present and distinct, because it is what comes back in batchItemFailures, and
the S3 object key is URL-encoded by the notification service, which is the
detail most of these builders exist to reproduce.
"""

from __future__ import annotations

import json
from urllib.parse import quote_plus


def s3_envelope(bucket: str, key: str, etag: str, event_name: str = "ObjectCreated:Put") -> dict:
    """The body S3 puts on the queue.

    The key is URL-encoded the way the notification service encodes it: a
    space becomes ``+``, other reserved characters become percent escapes, and
    the path separators are left alone. The etag is not quoted here, which is
    the opposite of what ListObjectsV2 returns.
    """
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "eu-west-1",
                "eventName": event_name,
                "s3": {
                    "bucket": {"name": bucket, "arn": f"arn:aws:s3:::{bucket}"},
                    "object": {"key": quote_plus(key, safe="/"), "size": 1024, "eTag": etag},
                },
            }
        ]
    }


def sqs_record(body: dict | str, message_id: str = "m-1", receive_count: int = 1) -> dict:
    return {
        "messageId": message_id,
        "receiptHandle": f"rh-{message_id}",
        "body": body if isinstance(body, str) else json.dumps(body),
        "attributes": {
            "ApproximateReceiveCount": str(receive_count),
            "SentTimestamp": "1772000000000",
        },
        "messageAttributes": {},
        "eventSource": "aws:sqs",
        "awsRegion": "eu-west-1",
    }


def sqs_event(*records: dict) -> dict:
    return {"Records": list(records)}


def object_message(bucket: str, key: str, etag: str, message_id: str = "m-1") -> dict:
    return sqs_record(s3_envelope(bucket, key, etag), message_id=message_id)


def s3_test_event(bucket: str, message_id: str = "m-test") -> dict:
    """What S3 sends once, when a notification configuration is saved."""
    return sqs_record(
        {
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Time": "2026-03-01T09:00:00.000Z",
            "Bucket": bucket,
        },
        message_id=message_id,
    )
