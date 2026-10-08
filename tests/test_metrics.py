import io
import json

from ingest.config import settings
from ingest.metrics import MAX_DEFINITIONS, MetricBatch


def test_counters_accumulate_rather_than_overwrite():
    batch = MetricBatch()
    batch.count("rows_loaded", 3)
    batch.count("rows_loaded", 4)

    assert batch.counters["rows_loaded"] == 7


def test_set_replaces_instead_of_adding():
    batch = MetricBatch()
    batch.count("queue_depth", 5)
    batch.set("queue_depth", 2)

    assert batch.counters["queue_depth"] == 2


def test_the_document_carries_the_namespace_dimensions_and_units():
    batch = MetricBatch(dimensions={"Handler": "load"})
    batch.count("rows_loaded", 12)
    batch.set("object_bytes", 2048, unit="Bytes")

    document = batch.document("ServerlessIngest", timestamp_ms=1772000000000)
    metrics = document["_aws"]["CloudWatchMetrics"][0]

    assert metrics["Namespace"] == "ServerlessIngest"
    assert metrics["Dimensions"] == [["Handler"]]
    assert {m["Name"]: m["Unit"] for m in metrics["Metrics"]} == {
        "rows_loaded": "Count",
        "object_bytes": "Bytes",
    }
    assert document["Handler"] == "load"
    assert document["rows_loaded"] == 12
    assert document["object_bytes"] == 2048


def test_a_document_with_no_dimensions_still_declares_an_empty_set():
    batch = MetricBatch()
    batch.count("rows_loaded")

    metrics = batch.document("ns", 0)["_aws"]["CloudWatchMetrics"][0]
    assert metrics["Dimensions"] == [[]]


def test_flush_writes_one_json_line_per_invocation():
    batch = MetricBatch(dimensions={"Handler": "load"})
    batch.count("rows_loaded", 1000)
    stream = io.StringIO()

    returned = batch.flush(stream=stream)

    written = stream.getvalue().strip().splitlines()
    assert len(written) == 1
    assert json.loads(written[0]) == returned


def test_flush_writes_nothing_when_there_is_nothing_to_report():
    stream = io.StringIO()
    assert MetricBatch().flush(stream=stream) is None
    assert stream.getvalue() == ""


def test_flush_is_disabled_by_settings(monkeypatch):
    monkeypatch.setenv("INGEST_METRICS_ENABLED", "false")
    settings.cache_clear()

    batch = MetricBatch()
    batch.count("rows_loaded", 1)
    stream = io.StringIO()

    assert batch.flush(stream=stream) is None
    assert stream.getvalue() == ""


def test_the_definition_count_is_capped():
    batch = MetricBatch()
    for index in range(MAX_DEFINITIONS + 20):
        batch.count(f"metric_{index}")

    metrics = batch.document("ns", 0)["_aws"]["CloudWatchMetrics"][0]["Metrics"]
    assert len(metrics) == MAX_DEFINITIONS
