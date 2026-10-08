"""Move messages from the dead letter queue back to the ingest queue.

Invoked by hand, with a payload, because an automatic drain is a loop. A
message lands in the DLQ after the ingest queue has already tried it five
times, so re-sending it without knowing why it failed buys five more identical
failures and a round trip.

The order of operations is send, then delete. A crash in between duplicates
the message rather than losing it, and a duplicate is already free: the ledger
claim in ingest.repository makes a second delivery of the same object a no-op.
Deleting first would be the cheaper code and the one that loses data.

Payload:
    {"max_messages": 50, "dry_run": false}
"""

from __future__ import annotations

import logging

from ingest import clients
from ingest.config import settings
from ingest.metrics import MetricBatch

log = logging.getLogger()
log.setLevel(logging.INFO)

# SQS caps a single receive at 10, so anything larger is a loop over polls.
RECEIVE_BATCH = 10
REPLAY_COUNT_ATTRIBUTE = "replay_count"


def _replay_count(message: dict) -> int:
    """Read the counter off a message, whichever shape it arrived in.

    ReceiveMessage answers with MessageAttributes and StringValue. A Lambda SQS
    event carries the same data as messageAttributes and stringValue. This
    handler only ever sees the first, but the casing is a trap worth absorbing
    once rather than debugging twice.
    """
    attributes = message.get("MessageAttributes") or message.get("messageAttributes") or {}
    attribute = attributes.get(REPLAY_COUNT_ATTRIBUTE)
    if not attribute:
        return 0
    raw = attribute.get("StringValue") or attribute.get("stringValue")
    try:
        return int(raw)
    except (TypeError, ValueError):
        log.warning("replay_count %r is not a number, treating it as 0", raw)
        return 0


def handler(event: dict, context: object = None) -> dict:
    config = settings()
    if not config.dlq_url or not config.main_queue_url:
        raise RuntimeError("INGEST_DLQ_URL and INGEST_MAIN_QUEUE_URL must both be set")

    budget = int(event.get("max_messages", 50))
    dry_run = bool(event.get("dry_run", False))
    sqs = clients.client("sqs")
    batch = MetricBatch(dimensions={"Handler": "replay"})

    replayed = 0
    exhausted = 0
    inspected = 0

    while replayed + exhausted < budget:
        response = sqs.receive_message(
            QueueUrl=config.dlq_url,
            MaxNumberOfMessages=min(RECEIVE_BATCH, budget - replayed - exhausted),
            MessageAttributeNames=["All"],
            VisibilityTimeout=60,
            WaitTimeSeconds=0,
        )
        messages = response.get("Messages", [])
        if not messages:
            break

        for message in messages:
            inspected += 1
            attempts = _replay_count(message)

            if attempts >= config.max_replays:
                # Left in the DLQ on purpose. Something about this message is
                # not transient and moving it again only hides that.
                exhausted += 1
                log.warning(
                    "message %s has been replayed %d times, leaving it",
                    message["MessageId"],
                    attempts,
                )
                continue

            if dry_run:
                # The message stays in the DLQ, and stays invisible for the
                # VisibilityTimeout above. A dry run is still a receive.
                replayed += 1
                continue

            sqs.send_message(
                QueueUrl=config.main_queue_url,
                MessageBody=message["Body"],
                MessageAttributes={
                    REPLAY_COUNT_ATTRIBUTE: {
                        "DataType": "Number",
                        "StringValue": str(attempts + 1),
                    }
                },
            )
            sqs.delete_message(QueueUrl=config.dlq_url, ReceiptHandle=message["ReceiptHandle"])
            replayed += 1

    batch.count("messages_inspected", inspected)
    batch.count("messages_replayed", replayed)
    batch.count("messages_exhausted", exhausted)
    batch.flush()

    log.info("inspected %d, replayed %d, exhausted %d", inspected, replayed, exhausted)
    return {"inspected": inspected, "replayed": replayed, "exhausted": exhausted}
