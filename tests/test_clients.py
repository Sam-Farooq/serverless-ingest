import pytest

from ingest import clients
from ingest.config import settings


class FakeConnection:
    def __init__(self) -> None:
        self.closed = False
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True


@pytest.fixture
def connect_calls(monkeypatch):
    """Replace psycopg.connect and count how often the module reaches for it."""
    made: list[FakeConnection] = []

    def fake_connect(*args, **kwargs):
        made.append(FakeConnection())
        return made[-1]

    monkeypatch.setattr(clients.psycopg, "connect", fake_connect)
    return made


def test_a_warm_invocation_reuses_the_connection(connect_calls):
    first = clients.connection()
    second = clients.connection()

    assert first is second
    assert len(connect_calls) == 1


def test_a_connection_closed_under_us_is_replaced(connect_calls):
    first = clients.connection()
    first.closed = True  # RDS idle timeout or a failover

    second = clients.connection()

    assert second is not first
    assert len(connect_calls) == 2


def test_reset_connection_closes_and_forgets(connect_calls):
    first = clients.connection()
    clients.reset_connection()

    assert first.close_calls == 1
    assert clients._connection is None
    assert clients.connection() is not first


def test_reset_connection_survives_a_close_that_raises(connect_calls, caplog):
    first = clients.connection()

    def boom() -> None:
        raise OSError("socket is gone")

    first.close = boom
    clients.reset_connection()  # must not propagate

    assert clients._connection is None
    assert "dropping it regardless" in caplog.text


def test_reset_connection_on_a_cold_container_is_a_no_op():
    clients.reset_connection()
    assert clients._connection is None


def test_the_connection_string_and_timeouts_come_from_settings(monkeypatch):
    captured: dict = {}

    def fake_connect(dsn, **kwargs):
        captured["dsn"] = dsn
        captured.update(kwargs)
        return FakeConnection()

    monkeypatch.setenv("INGEST_DATABASE_URL", "postgresql://db.internal/ingest")
    monkeypatch.setenv("INGEST_STATEMENT_TIMEOUT_MS", "9000")
    settings.cache_clear()
    monkeypatch.setattr(clients.psycopg, "connect", fake_connect)

    clients.connection()

    assert captured["dsn"] == "postgresql://db.internal/ingest"
    assert captured["autocommit"] is False
    assert captured["options"] == "-c statement_timeout=9000"


def test_each_service_client_is_built_once(monkeypatch):
    built: list[str] = []

    def fake_client(service, config=None):
        built.append(service)
        return object()

    monkeypatch.setattr(clients.boto3, "client", fake_client)

    first = clients.client("s3")
    again = clients.client("s3")
    other = clients.client("sqs")

    assert first is again
    assert other is not first
    assert built == ["s3", "sqs"]


def test_the_boto_timeouts_are_shorter_than_the_function_timeout():
    # infra/lambda.tf sets the function timeout to 60 seconds. A read timeout
    # of 10 with 3 attempts is 30 at worst, which leaves room to report the
    # failure instead of being killed mid-batch.
    assert clients._BOTO_CONFIG.read_timeout == 10
    assert clients._BOTO_CONFIG.retries["max_attempts"] == 3
    assert clients._BOTO_CONFIG.read_timeout * clients._BOTO_CONFIG.retries["max_attempts"] < 60
