"""Shared fixtures. Nothing here reaches the network.

The AWS credentials are deliberately fake and deliberately set. Without them
botocore looks for a real credential chain, and on a developer machine that
means moto hands back a client pointed at somebody's actual account.
"""

import os

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")


@pytest.fixture(autouse=True)
def _cold_start():
    """Every test starts on a cold container.

    Without this the cached connection from one test is handed to the next,
    which is exactly the bug the cache is supposed to make safe and exactly
    the bug a shared fixture would hide.
    """
    from ingest import clients
    from ingest.config import settings

    clients._clients.clear()
    clients._connection = None
    settings.cache_clear()
    yield
    clients._clients.clear()
    clients._connection = None
    settings.cache_clear()
