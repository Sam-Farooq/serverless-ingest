"""Metrics as structured log lines, not as a PutMetricData call.

CloudWatch reads metrics out of a log line in embedded metric format, which
means a function can publish without making an API call at all. That matters
here for two reasons. PutMetricData is synchronous, so its latency lands
inside the invocation and its throttling lands on the ingest path. And a
metrics call that fails has to be swallowed, which is one more branch in a
handler that already has enough of them.

What this gives up: the metric is not queryable until the log line is
delivered and parsed, so a dashboard is seconds behind rather than immediate,
and a log group with no subscription produces no metric at all. For an ingest
path measured in batches per minute that is an easy trade. For something
being watched during a release it would not be.

One document per invocation, not one per record. A thousand-row object should
produce one line.
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field

from ingest.config import settings

# CloudWatch rejects a document with more than 100 metric definitions, and a
# handler that wanted that many would have a different problem.
MAX_DEFINITIONS = 100


@dataclass
class MetricBatch:
    """Counters for one invocation, flushed once at the end."""

    dimensions: dict[str, str] = field(default_factory=dict)
    counters: dict[str, float] = field(default_factory=dict)
    units: dict[str, str] = field(default_factory=dict)

    def count(self, name: str, value: float = 1, unit: str = "Count") -> None:
        self.counters[name] = self.counters.get(name, 0) + value
        self.units[name] = unit

    def set(self, name: str, value: float, unit: str = "Count") -> None:
        self.counters[name] = value
        self.units[name] = unit

    def document(self, namespace: str, timestamp_ms: int) -> dict:
        names = list(self.counters)[:MAX_DEFINITIONS]
        body: dict = {
            "_aws": {
                "Timestamp": timestamp_ms,
                "CloudWatchMetrics": [
                    {
                        "Namespace": namespace,
                        "Dimensions": [list(self.dimensions)] if self.dimensions else [[]],
                        "Metrics": [{"Name": name, "Unit": self.units[name]} for name in names],
                    }
                ],
            }
        }
        body.update(self.dimensions)
        for name in names:
            body[name] = self.counters[name]
        return body

    def flush(self, stream=None) -> dict | None:
        """Write the document, or return None when metrics are switched off.

        The return value is the document, so a test can assert on the shape
        without parsing stdout.
        """
        config = settings()
        if not config.metrics_enabled or not self.counters:
            return None

        document = self.document(config.metrics_namespace, int(time.time() * 1000))
        print(json.dumps(document, separators=(",", ":")), file=stream or sys.stdout)
        return document
