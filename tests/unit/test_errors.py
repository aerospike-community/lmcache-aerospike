import pytest
from aerospike import exception as ax

from lmcache_aerospike.errors import (
    AEROSPIKE_ERR_FAIL_FORBIDDEN,
    AEROSPIKE_ERR_KEY_BUSY,
    AerospikeBusyError,
    AerospikeConnectionError,
    AerospikeInternalError,
    AerospikeRecordTooBigError,
    AerospikeTTLConfigError,
    AerospikeUnknownError,
    classify,
    map_aerospike_error,
)


class _FakeAx(ax.AerospikeError):
    def __init__(self, code=None, msg="fake"):
        super().__init__(msg)
        self.code = code
        self.msg = msg


@pytest.mark.parametrize(
    "exc,expected",
    [
        (ax.RecordNotFound(), "not_found"),
        (ax.RecordTooBig(), "too_big"),
        (ax.TimeoutError(), "timeout"),
        (ax.ConnectionError(), "connection"),
        (ax.ClientError(), "connection"),
        (_FakeAx(code=AEROSPIKE_ERR_FAIL_FORBIDDEN), "forbidden_ttl"),
        (ax.ForbiddenError(), "forbidden_ttl"),
        (ax.DeviceOverload(), "busy"),
        (ax.BatchQueueFullError(), "busy"),
        (ax.RecordBusy(), "busy"),
        (_FakeAx(code=AEROSPIKE_ERR_KEY_BUSY), "busy"),
        (ax.RecordKeyMismatch(), "key_mismatch"),
        (_FakeAx(code=999), "unknown"),
    ],
)
def test_classify(exc, expected):
    assert classify(exc) == expected


@pytest.mark.parametrize(
    "exc,expected_type",
    [
        (ax.RecordTooBig(), AerospikeRecordTooBigError),
        (ax.ConnectionError(), AerospikeConnectionError),
        (_FakeAx(code=AEROSPIKE_ERR_FAIL_FORBIDDEN), AerospikeTTLConfigError),
        (ax.DeviceOverload(), AerospikeBusyError),
        (ax.RecordKeyMismatch(), AerospikeInternalError),
        (_FakeAx(code=999), AerospikeUnknownError),
    ],
)
def test_map_aerospike_error(exc, expected_type):
    mapped = map_aerospike_error("put", exc)
    assert isinstance(mapped, expected_type)
    assert "put" in str(mapped)
