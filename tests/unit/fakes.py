"""Test doubles (no network)."""

from __future__ import annotations

from dataclasses import dataclass, field

import torch

from lmcache.v1.memory_management import MemoryFormat


class FakeKey:
    def __init__(self, value: str = "test-key") -> None:
        self._value = value

    def to_string(self) -> str:
        return self._value


@dataclass
class FakeMemoryObj:
    _data: bytearray
    shapes: list = field(default_factory=lambda: [torch.Size([2, 1, 8, 128])])
    dtypes: list = field(default_factory=lambda: [torch.float16])
    fmt: MemoryFormat = MemoryFormat.KV_2LTD
    ref_count: int = 0

    @property
    def byte_array(self):
        return memoryview(self._data)

    def get_size(self) -> int:
        return len(self._data)

    def get_shapes(self):
        return self.shapes

    def get_dtypes(self):
        return self.dtypes

    def get_memory_format(self):
        return self.fmt

    def ref_count_down(self) -> None:
        self.ref_count += 1

    class _Meta:
        shape = torch.Size([2, 1, 8, 128])
        dtype = torch.float16

    meta = _Meta()


@dataclass
class FakeBatchRecord:
    result: int
    record: tuple | None = None


class FakeBatchRecords:
    def __init__(self, records: list[FakeBatchRecord]) -> None:
        self.batch_records = records


class FakeClient:
    def __init__(self) -> None:
        self.put_calls: list[tuple] = []
        self.get_store: dict[tuple, dict] = {}
        self.batch_read_results: FakeBatchRecords | None = None
        self.batch_write_batches: list = []
        self.connected = True

    def put(self, key, bins, meta=None, policy=None):
        self.put_calls.append((key, dict(bins), meta, policy))
        self.get_store[key] = dict(bins)

    def get(self, key):
        if key not in self.get_store:
            from aerospike import exception as ax

            raise ax.RecordNotFound()
        bins = self.get_store[key]
        return (key, {"gen": 1}, bins)

    def select(self, key, bin_names):
        if key not in self.get_store:
            from aerospike import exception as ax

            raise ax.RecordNotFound()
        bins = {
            k: self.get_store[key][k]
            for k in bin_names
            if k in self.get_store[key]
        }
        return (key, {"gen": 1}, bins)

    def batch_read(self, keys, bins):
        del bins
        if self.batch_read_results is not None:
            return self.batch_read_results
        records = []
        for key in keys:
            if key in self.get_store:
                records.append(
                    FakeBatchRecord(0, (key, {"gen": 1}, self.get_store[key]))
                )
            else:
                records.append(FakeBatchRecord(2, None))
        return FakeBatchRecords(records)

    def batch_write(self, batch):
        self.batch_write_batches.append(batch)

    def remove(self, key):
        if key not in self.get_store:
            from aerospike import exception as ax

            raise ax.RecordNotFound()
        del self.get_store[key]

    def touch(self, key, meta=None, policy=None):
        del meta, policy
        if key not in self.get_store:
            from aerospike import exception as ax

            raise ax.RecordNotFound()

    def operate(self, key, ops, meta=None, policy=None):
        del meta, policy
        if key not in self.get_store:
            from aerospike import exception as ax

            raise ax.RecordNotFound()
        for operation in ops:
            if hasattr(operation, "bin") and hasattr(operation, "value"):
                self.get_store[key][operation.bin] = operation.value

    def is_connected(self):
        return self.connected

    def get_node_names(self):
        return ["node1"] if self.connected else []


class FakeLocalCPUBackend:
    def __init__(self, alloc_size: int = 4096) -> None:
        from unittest.mock import MagicMock

        self.alloc_size = alloc_size
        self.config = MagicMock()
        self.config.extra_config = {}
        self.metadata = MagicMock()

    def allocate(self, shapes, dtypes, fmt):
        return FakeMemoryObj(bytearray(self.alloc_size))
