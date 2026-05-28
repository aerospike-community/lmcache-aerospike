"""Integration test fixtures (live Aerospike CE required)."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator

import pytest

from tests.integration.helpers import aerospike_hosts, build_connector, close_connector

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION") != "1",
    reason="Set RUN_INTEGRATION=1 (or source .aerospike-ci.env after start_aerospike_ce.sh)",
)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: requires live Aerospike CE (RUN_INTEGRATION=1)",
    )


@pytest.fixture(scope="session")
def aerospike_connection() -> dict[str, object]:
    host, port = aerospike_hosts()[0]
    return {
        "host": host,
        "port": port,
        "namespace": os.environ.get("AEROSPIKE_TEST_NAMESPACE", "lmcache"),
    }


@pytest.fixture
def connector() -> Iterator[tuple]:
    conn, backend, _config, _meta = build_connector()
    try:
        yield conn, backend
    finally:
        asyncio.run(close_connector(conn))


@pytest.fixture(scope="session")
def chunk_id_counter():
    counter = {"n": 0}

    def next_id() -> int:
        counter["n"] += 1
        return counter["n"] * 100_000

    return next_id
