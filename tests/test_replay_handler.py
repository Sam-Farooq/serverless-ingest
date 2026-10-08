import json

import boto3
import pytest
from moto import mock_aws

from handlers import replay
from ingest.config import settings

REGION = "eu-west-1"


@pytest.fixture
def queues(monkeypatch):
    with mock_aws():
        sqs = boto3.client("sqs", region_name=REGION)
        main = sqs.create_queue(QueueName="ingest")["QueueUrl"]
        dlq = sqs.create_queue(QueueName="ingest-dlq")["QueueUrl"]
        monkeypatch.setenv("INGEST_MAIN_QUEUE_URL", main)
        monkeypatch.setenv("INGEST_DLQ_URL", dlq)
        settings.cache_clear()
        yield sqs, main, dlq


def park(sqs, dlq: str, body: dict, replay_count: int | None = None) -> None:
    attributes = {}
    if replay_count is not None:
        attributes[replay.REPLAY_COUNT_ATTRIBUTE] = {
            "DataType": "Number",
            "StringValue": str(replay_count),
        }
    sqs.send_message(QueueUrl=dlq, MessageBody=json.dumps(body), MessageAttributes=attributes)


def depth(sqs, url: str) -> int:
    """Visible messages only, which is what ApproximateNumberOfMessages means."""
    attributes = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(attributes["Attributes"]["ApproximateNumberOfMessages"])


def in_flight(sqs, url: str) -> int:
    attributes = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessagesNotVisible"]
    )
    return int(attributes["Attributes"]["ApproximateNumberOfMessagesNotVisible"])


def test_a_parked_message_goes_back_to_the_ingest_queue(queues):
    sqs, main, dlq = queues
    park(sqs, dlq, {"key": "raw/a.ndjson"})

    result = replay.handler({})

    assert result == {"inspected": 1, "replayed": 1, "exhausted": 0}
    assert depth(sqs, main) == 1
    assert depth(sqs, dlq) == 0


def test_the_body_arrives_unchanged_so_the_load_handler_can_read_it(queues):
    sqs, main, dlq = queues
    body = {"Records": [{"eventName": "ObjectCreated:Put"}]}
    park(sqs, dlq, body)

    replay.handler({})

    received = sqs.receive_message(QueueUrl=main, MessageAttributeNames=["All"])["Messages"][0]
    assert json.loads(received["Body"]) == body
    assert received["MessageAttributes"]["replay_count"]["StringValue"] == "1"


def test_the_replay_count_increments_on_each_pass(queues):
    sqs, main, dlq = queues
    park(sqs, dlq, {"n": 1}, replay_count=1)

    replay.handler({})

    received = sqs.receive_message(QueueUrl=main, MessageAttributeNames=["All"])["Messages"][0]
    assert received["MessageAttributes"]["replay_count"]["StringValue"] == "2"


def test_a_message_at_the_replay_ceiling_is_left_where_it_is(queues, monkeypatch):
    sqs, main, dlq = queues
    monkeypatch.setenv("INGEST_MAX_REPLAYS", "3")
    settings.cache_clear()
    park(sqs, dlq, {"n": 1}, replay_count=3)

    result = replay.handler({})

    assert result == {"inspected": 1, "replayed": 0, "exhausted": 1}
    assert depth(sqs, main) == 0


def test_max_messages_bounds_the_drain(queues):
    sqs, main, dlq = queues
    for index in range(7):
        park(sqs, dlq, {"n": index})

    result = replay.handler({"max_messages": 3})

    assert result["replayed"] == 3
    assert depth(sqs, main) == 3


def test_a_dry_run_counts_without_moving_anything(queues):
    sqs, main, dlq = queues
    park(sqs, dlq, {"n": 1})

    result = replay.handler({"dry_run": True})

    assert result["replayed"] == 1
    assert depth(sqs, main) == 0
    # Still in the DLQ, but in flight: a dry run is a receive, so the message
    # is invisible until the visibility timeout lapses.
    assert depth(sqs, dlq) == 0
    assert in_flight(sqs, dlq) == 1


def test_an_empty_dlq_stops_immediately(queues):
    sqs, main, dlq = queues

    assert replay.handler({"max_messages": 500}) == {
        "inspected": 0,
        "replayed": 0,
        "exhausted": 0,
    }


def test_a_missing_queue_url_fails_loudly_rather_than_silently(monkeypatch):
    monkeypatch.setenv("INGEST_DLQ_URL", "")
    monkeypatch.setenv("INGEST_MAIN_QUEUE_URL", "")
    settings.cache_clear()

    with pytest.raises(RuntimeError, match="must both be set"):
        replay.handler({})


def test_the_replay_count_is_read_from_either_message_shape():
    api_shape = {"MessageAttributes": {"replay_count": {"StringValue": "2"}}}
    lambda_shape = {"messageAttributes": {"replay_count": {"stringValue": "4"}}}

    assert replay._replay_count(api_shape) == 2
    assert replay._replay_count(lambda_shape) == 4


def test_an_unparseable_replay_count_is_treated_as_zero():
    broken = {"MessageAttributes": {"replay_count": {"StringValue": "lots"}}}
    assert replay._replay_count(broken) == 0
    assert replay._replay_count({}) == 0
    assert replay._replay_count({"MessageAttributes": {}}) == 0
