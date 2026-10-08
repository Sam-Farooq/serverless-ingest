"""Things that are expensive to build, built once per execution environment.

Lambda keeps a container warm between invocations, so anything created at
module scope or cached in a module global is paid for on the cold start and
free afterwards. Two things make that worth doing carefully.

A boto3 client loads and parses a service model the first time it is built.
Doing that inside the handler pays it on every invocation.

A Postgres connection is worse, because it is also wrong if it is reused
blindly. RDS closes idle connections, a failover replaces the endpoint under
you, and a warm container wakes up holding a socket to nothing. So the cached
connection is checked before it is handed out, and a handler that saw a
database error drops it rather than passing the same dead socket to the next
invocation.
"""

from __future__ import annotations

import logging
from typing import Any

import boto3
import psycopg
from botocore.config import Config

from ingest.config import settings

log = logging.getLogger(__name__)

# Three retries with the standard backoff, and short timeouts, because the
# function's own timeout is the real deadline. A default 60 second read timeout
# plus retries outlives the invocation and turns a slow dependency into an
# unexplained timeout rather than a logged error.
_BOTO_CONFIG = Config(
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=3,
    read_timeout=10,
)

_clients: dict[str, Any] = {}
_connection: psycopg.Connection | None = None


def client(service: str) -> Any:
    if service not in _clients:
        _clients[service] = boto3.client(service, config=_BOTO_CONFIG)
    return _clients[service]


def connection() -> psycopg.Connection:
    """The cached connection, or a new one if the cached one is no longer usable."""
    global _connection

    if _connection is not None and _connection.closed:
        log.info("cached connection was closed, reconnecting")
        _connection = None

    if _connection is None:
        config = settings()
        _connection = psycopg.connect(
            config.database_url,
            connect_timeout=config.connect_timeout,
            autocommit=False,
            options=f"-c statement_timeout={config.statement_timeout_ms}",
        )
    return _connection


def reset_connection() -> None:
    """Drop the cached connection after an error, without raising a new one.

    Called when an invocation fails for a reason that might be the connection.
    Closing can itself fail on a socket that is already gone, which must not
    turn a reported batch failure into an unhandled exception.
    """
    global _connection

    if _connection is None:
        return
    try:
        _connection.close()
    except Exception:  # noqa: BLE001 - the connection is being discarded anyway
        log.warning("closing the cached connection failed, dropping it regardless")
    finally:
        _connection = None
