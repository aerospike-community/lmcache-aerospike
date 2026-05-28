"""Integration test fixtures (expanded in IMPLEMENTATION_PLAN S13–S14)."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION") != "1",
    reason="Set RUN_INTEGRATION=1 to run tests against a live Aerospike CE node",
)
