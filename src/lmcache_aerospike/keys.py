"""Logical LMCache keys to Aerospike primary key tuples."""

from __future__ import annotations

from typing import Any


def meta_key(ns: str, set_name: str, ck: Any) -> tuple[str, str, str]:
    """Aerospike key tuple for the meta record."""
    return (ns, set_name, f"{ck.to_string()}|m")


def segment_key(ns: str, set_name: str, ck: Any, index: int) -> tuple[str, str, str]:
    """Aerospike key tuple for segment *index*."""
    return (ns, set_name, f"{ck.to_string()}|s|{index}")


def segment_keys(
    ns: str, set_name: str, ck: Any, nseg: int
) -> list[tuple[str, str, str]]:
    """All segment key tuples for *nseg* segments."""
    return [segment_key(ns, set_name, ck, i) for i in range(nseg)]
