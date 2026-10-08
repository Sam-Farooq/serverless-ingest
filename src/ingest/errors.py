"""The one distinction the handlers actually need.

Everything that goes wrong in an ingest path is either worth retrying or not,
and the two have opposite correct responses. A transient failure has to be
reported back to SQS so the message is redelivered. A permanent failure must
not be, because five redeliveries of a malformed message is five invocations
that fail the same way and a dead letter queue entry that explains nothing.
"""

from __future__ import annotations


class PermanentFailure(Exception):
    """Retrying this will produce the same result. Record it and move on."""
