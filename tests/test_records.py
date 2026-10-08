import json

from ingest.records import Reading, parse_ndjson

GOOD = {
    "event_id": "e-1",
    "device_id": "d-1",
    "recorded_at": "2026-03-01T10:00:00Z",
    "metric": "temperature_c",
    "value": 21.5,
}


def line(**overrides) -> bytes:
    return (json.dumps({**GOOD, **overrides}) + "\n").encode()


def test_valid_line_parses_into_a_reading():
    result = parse_ndjson(line())
    assert result.rejections == []
    assert len(result.readings) == 1
    assert result.readings[0].event_id == "e-1"
    assert result.readings[0].value == 21.5


def test_blank_lines_are_skipped_but_still_counted_for_line_numbers():
    body = b"\n\n" + line(event_id="e-2") + b"\nnot json\n"
    result = parse_ndjson(body)

    assert [r.event_id for r in result.readings] == ["e-2"]
    assert len(result.rejections) == 1
    # The reading is on line 3, so the bad line is 5, not 2.
    assert result.rejections[0].line_number == 5


def test_invalid_json_is_rejected_with_the_offending_text_kept():
    result = parse_ndjson(b'{"event_id": "e-1",}\n')
    assert result.readings == []
    assert result.rejections[0].reason.startswith("invalid json:")
    assert result.rejections[0].payload == '{"event_id": "e-1",}'


def test_a_json_array_line_is_rejected_by_type_not_by_schema():
    result = parse_ndjson(b"[1, 2, 3]\n")
    assert result.rejections[0].reason == "expected a json object, got list"


def test_an_unexpected_field_is_rejected_rather_than_ignored():
    result = parse_ndjson(line(firmware="4.1.2"))
    assert result.readings == []
    assert "firmware" in result.rejections[0].reason


def test_an_unknown_metric_is_rejected():
    result = parse_ndjson(line(metric="voltage_v"))
    assert result.readings == []
    assert result.rejections[0].reason.startswith("metric:")


def test_a_value_outside_the_physical_range_is_rejected():
    result = parse_ndjson(line(metric="humidity_pct", value=140.0))
    assert result.readings == []
    assert result.rejections[0].reason == "humidity_pct out of range [0.0, 100.0]: 140.0"


def test_the_range_boundary_itself_is_accepted():
    result = parse_ndjson(line(metric="humidity_pct", value=100.0))
    assert len(result.readings) == 1
    assert result.readings[0].value == 100.0


def test_a_missing_field_names_the_field_in_the_reason():
    body = json.dumps({k: v for k, v in GOOD.items() if k != "device_id"}).encode()
    result = parse_ndjson(body)
    assert result.rejections[0].reason == "device_id: Field required"


def test_a_repeated_event_id_in_one_object_is_rejected_not_silently_dropped():
    result = parse_ndjson(line() + line(value=22.0))
    assert len(result.readings) == 1
    assert result.readings[0].value == 21.5
    assert result.rejections[0].reason == "duplicate event_id in object: e-1"
    assert result.rejections[0].line_number == 2


def test_good_and_bad_lines_are_both_returned_from_one_object():
    body = line(event_id="a") + b"garbage\n" + line(event_id="b")
    result = parse_ndjson(body)
    assert [r.event_id for r in result.readings] == ["a", "b"]
    assert len(result.rejections) == 1
    assert result.total_lines == 3


def test_undecodable_bytes_do_not_raise():
    result = parse_ndjson(b"\xff\xfe not json\n" + line(event_id="ok"))
    assert [r.event_id for r in result.readings] == ["ok"]
    assert len(result.rejections) == 1


def test_whitespace_is_stripped_from_identifiers():
    reading = Reading.model_validate({**GOOD, "device_id": "  d-9  "})
    assert reading.device_id == "d-9"
