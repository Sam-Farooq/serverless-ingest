"""The record contract, and the NDJSON parser that applies it line by line.

A line that fails this contract will never pass it. That is the whole reason
parsing returns two lists instead of raising: the caller needs to commit the
good rows and quarantine the bad ones in the same transaction, because the
alternative is a message that retries five times and takes a whole object's
worth of good rows into the dead letter queue with it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

Metric = Literal["temperature_c", "humidity_pct", "pressure_hpa"]

# Physically possible ranges, not business thresholds. A humidity of 140 is a
# broken sensor or a broken producer, and either way it is not a reading.
METRIC_RANGES: dict[str, tuple[float, float]] = {
    "temperature_c": (-90.0, 60.0),
    "humidity_pct": (0.0, 100.0),
    "pressure_hpa": (800.0, 1100.0),
}

MAX_PAYLOAD_KEPT = 2000


class Reading(BaseModel):
    """One sensor reading.

    ``extra="forbid"`` is deliberate. An unexpected field means the producer
    changed and nobody said so, and a silent ingest of a renamed column is
    worse than a visible rejection: the rows go to ``rejected_rows``, the
    rejected count breaches its alarm, and the object still commits.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(min_length=1, max_length=64)
    device_id: str = Field(min_length=1, max_length=64)
    recorded_at: datetime
    metric: Metric
    value: float

    @model_validator(mode="after")
    def _value_in_range(self) -> Reading:
        low, high = METRIC_RANGES[self.metric]
        if not low <= self.value <= high:
            raise ValueError(f"{self.metric} out of range [{low}, {high}]: {self.value}")
        return self


@dataclass(frozen=True)
class Rejection:
    """A line that was read, understood to be wrong, and kept anyway."""

    line_number: int
    reason: str
    payload: str


@dataclass(frozen=True)
class ParseResult:
    readings: list[Reading]
    rejections: list[Rejection]

    @property
    def total_lines(self) -> int:
        return len(self.readings) + len(self.rejections)


def _compact_error(exc: ValidationError) -> str:
    """One line out of a pydantic error, because the column is TEXT not an essay."""
    first = exc.errors()[0]
    location = ".".join(str(part) for part in first["loc"])
    # Pydantic prefixes a custom ValueError. Drop the prefix, keep the text.
    message = first["msg"].removeprefix("Value error, ")
    # A whole-model validator has an empty loc, and "<root>: ..." reads worse
    # than the message on its own.
    return f"{location}: {message}" if location else message


def parse_ndjson(body: bytes) -> ParseResult:
    """Parse newline-delimited JSON into readings and rejections.

    Line numbers are 1-based and count blank lines, so a reason logged against
    line 41 can be found with ``sed -n 41p`` on the object itself. Blank lines
    are skipped rather than rejected: a trailing newline is not an error.

    A duplicate ``event_id`` inside one object is a rejection rather than a
    silent overwrite. The database would skip it either way, but then nothing
    would say the producer is emitting the same id twice.
    """
    readings: list[Reading] = []
    rejections: list[Rejection] = []
    seen: set[str] = set()
    text = body.decode("utf-8", errors="replace")

    for line_number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        payload = line[:MAX_PAYLOAD_KEPT]

        try:
            document = json.loads(line)
        except json.JSONDecodeError as exc:
            rejections.append(Rejection(line_number, f"invalid json: {exc.msg}", payload))
            continue

        if not isinstance(document, dict):
            kind = type(document).__name__
            rejections.append(
                Rejection(line_number, f"expected a json object, got {kind}", payload)
            )
            continue

        try:
            reading = Reading.model_validate(document)
        except ValidationError as exc:
            rejections.append(Rejection(line_number, _compact_error(exc), payload))
            continue

        if reading.event_id in seen:
            reason = f"duplicate event_id in object: {reading.event_id}"
            rejections.append(Rejection(line_number, reason, payload))
            continue

        seen.add(reading.event_id)
        readings.append(reading)

    return ParseResult(readings=readings, rejections=rejections)
