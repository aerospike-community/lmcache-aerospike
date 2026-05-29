"""AerospikeConfig parsing from LMCache extra_config."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lmcache_aerospike.errors import AerospikeConfigError

_COMMIT_LEVELS = frozenset({"all", "master"})
_REPLICAS = frozenset({"master", "any", "sequence", "prefer_rack"})

_DEFAULT_TARGET_SEGMENT_BYTES = 4 * 1024 * 1024
_DEFAULT_MIN_SEGMENT_BYTES = 64 * 1024
_DEFAULT_TTL_SECONDS = 86400


def _parse_bool(value: Any, fq_key: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "on"):
            return True
        if lowered in ("false", "0", "no", "off"):
            return False
    raise AerospikeConfigError(f"{fq_key}: expected a boolean, got {value!r}")


def _parse_int(value: Any, fq_key: str) -> int:
    if isinstance(value, bool):
        raise AerospikeConfigError(f"{fq_key}: expected an integer, got bool")
    try:
        return int(value)
    except (TypeError, ValueError) as e:
        raise AerospikeConfigError(f"{fq_key}: expected an integer, got {value!r}") from e


def _parse_hosts(value: Any, fq_key: str) -> tuple[tuple[str, int], ...]:
    if not isinstance(value, str) or not value.strip():
        raise AerospikeConfigError(f"{fq_key}: hosts is required")
    hosts: list[tuple[str, int]] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise AerospikeConfigError(
                f"{fq_key}: each host must be host:port, got {part!r}"
            )
        host, port_s = part.rsplit(":", 1)
        host = host.strip()
        if not host:
            raise AerospikeConfigError(f"{fq_key}: empty host in {part!r}")
        try:
            port = int(port_s)
        except ValueError as e:
            raise AerospikeConfigError(
                f"{fq_key}: invalid port in {part!r}"
            ) from e
        if port <= 0 or port > 65535:
            raise AerospikeConfigError(f"{fq_key}: port out of range in {part!r}")
        hosts.append((host, port))
    if not hosts:
        raise AerospikeConfigError(f"{fq_key}: hosts is required")
    return tuple(hosts)


@dataclass(frozen=True, slots=True)
class AerospikeConfig:
    """Frozen configuration parsed from LMCache extra_config."""

    plugin_name: str
    hosts: tuple[tuple[str, int], ...]
    namespace: str = "lmcache"
    set_name: str = "kv_chunks"
    target_segment_bytes: int = _DEFAULT_TARGET_SEGMENT_BYTES
    max_segment_bytes: int | None = None
    min_segment_bytes: int = _DEFAULT_MIN_SEGMENT_BYTES
    single_record_threshold_bytes: int | None = None
    default_ttl_seconds: int = _DEFAULT_TTL_SECONDS
    read_timeout_ms: int = 1000
    write_timeout_ms: int = 2000
    batch_max_in_flight: int = 64
    executor_threads: int = 16
    enable_list: bool = False
    enable_crc32: bool = False
    commit_level: str = "all"
    replica: str = "sequence"
    send_key: bool = False
    username: str = ""
    password: str = ""
    tls_name: str = ""

    def key(self, name: str) -> str:
        """Fully-qualified extra_config key for error messages."""
        return f"remote_storage_plugin.{self.plugin_name}.{name}"

    @classmethod
    def from_extra_config(
        cls,
        extra_config: dict[str, Any] | None,
        plugin_name: str,
        *,
        config_prefix: str | None = None,
    ) -> AerospikeConfig:
        data = extra_config or {}
        prefix = config_prefix or f"remote_storage_plugin.{plugin_name}."
        if not prefix.endswith("."):
            prefix = f"{prefix}."

        def lookup(short_name: str, *aliases: str) -> Any:
            keys = (short_name, *aliases)
            for k in keys:
                fq = f"{prefix}{k}"
                if fq in data:
                    return data[fq]
            return None

        hosts_raw = lookup("hosts")
        if hosts_raw is None:
            raise AerospikeConfigError(f"{prefix}hosts is required")
        hosts = _parse_hosts(hosts_raw, f"{prefix}hosts")

        namespace = lookup("namespace")
        set_raw = lookup("set", "set_name")
        target_raw = lookup("target_segment_bytes")
        max_raw = lookup("max_segment_bytes")
        min_raw = lookup("min_segment_bytes")
        single_raw = lookup("single_record_threshold_bytes")
        ttl_raw = lookup("default_ttl_seconds")
        read_to = lookup("read_timeout_ms")
        write_to = lookup("write_timeout_ms")
        batch_raw = lookup("batch_max_in_flight")
        exec_raw = lookup("executor_threads")
        list_raw = lookup("enable_list")
        crc_raw = lookup("enable_crc32")
        commit_raw = lookup("commit_level")
        replica_raw = lookup("replica")
        send_key_raw = lookup("send_key")
        user_raw = lookup("username")
        pass_raw = lookup("password")
        tls_raw = lookup("tls_name")

        namespace_s = "lmcache" if namespace is None else str(namespace)
        set_name_s = "kv_chunks" if set_raw is None else str(set_raw)

        target_segment_bytes = (
            _DEFAULT_TARGET_SEGMENT_BYTES
            if target_raw is None
            else _parse_int(target_raw, f"{prefix}target_segment_bytes")
        )
        max_segment_bytes = (
            None if max_raw is None else _parse_int(max_raw, f"{prefix}max_segment_bytes")
        )
        min_segment_bytes = (
            _DEFAULT_MIN_SEGMENT_BYTES
            if min_raw is None
            else _parse_int(min_raw, f"{prefix}min_segment_bytes")
        )
        single_record_threshold_bytes = (
            None
            if single_raw is None
            else _parse_int(single_raw, f"{prefix}single_record_threshold_bytes")
        )
        default_ttl_seconds = (
            _DEFAULT_TTL_SECONDS
            if ttl_raw is None
            else _parse_int(ttl_raw, f"{prefix}default_ttl_seconds")
        )
        read_timeout_ms = (
            1000 if read_to is None else _parse_int(read_to, f"{prefix}read_timeout_ms")
        )
        write_timeout_ms = (
            2000 if write_to is None else _parse_int(write_to, f"{prefix}write_timeout_ms")
        )
        batch_max_in_flight = (
            64
            if batch_raw is None
            else _parse_int(batch_raw, f"{prefix}batch_max_in_flight")
        )
        executor_threads = (
            16
            if exec_raw is None
            else _parse_int(exec_raw, f"{prefix}executor_threads")
        )
        enable_list = False if list_raw is None else _parse_bool(list_raw, f"{prefix}enable_list")
        enable_crc32 = False if crc_raw is None else _parse_bool(crc_raw, f"{prefix}enable_crc32")
        commit_level = "all" if commit_raw is None else str(commit_raw).strip().lower()
        replica = (
            "sequence" if replica_raw is None else str(replica_raw).strip().lower()
        )
        send_key = (
            False
            if send_key_raw is None
            else _parse_bool(send_key_raw, f"{prefix}send_key")
        )
        username = "" if user_raw is None else str(user_raw)
        password = "" if pass_raw is None else str(pass_raw)
        tls_name = "" if tls_raw is None else str(tls_raw)

        if commit_level not in _COMMIT_LEVELS:
            raise AerospikeConfigError(
                f"{prefix}commit_level: must be one of {sorted(_COMMIT_LEVELS)}, "
                f"got {commit_level!r}"
            )
        if replica not in _REPLICAS:
            raise AerospikeConfigError(
                f"{prefix}replica: must be one of {sorted(_REPLICAS)}, got {replica!r}"
            )

        for name, val in (
            ("target_segment_bytes", target_segment_bytes),
            ("min_segment_bytes", min_segment_bytes),
            ("read_timeout_ms", read_timeout_ms),
            ("write_timeout_ms", write_timeout_ms),
            ("batch_max_in_flight", batch_max_in_flight),
            ("executor_threads", executor_threads),
        ):
            if val <= 0:
                raise AerospikeConfigError(f"{prefix}{name}: must be positive, got {val}")

        if max_segment_bytes is not None and max_segment_bytes <= 0:
            raise AerospikeConfigError(
                f"{prefix}max_segment_bytes: must be positive, got {max_segment_bytes}"
            )
        if (
            single_record_threshold_bytes is not None
            and single_record_threshold_bytes <= 0
        ):
            raise AerospikeConfigError(
                f"{prefix}single_record_threshold_bytes: must be positive, "
                f"got {single_record_threshold_bytes}"
            )

        return cls(
            plugin_name=plugin_name,
            hosts=hosts,
            namespace=namespace_s,
            set_name=set_name_s,
            target_segment_bytes=target_segment_bytes,
            max_segment_bytes=max_segment_bytes,
            min_segment_bytes=min_segment_bytes,
            single_record_threshold_bytes=single_record_threshold_bytes,
            default_ttl_seconds=default_ttl_seconds,
            read_timeout_ms=read_timeout_ms,
            write_timeout_ms=write_timeout_ms,
            batch_max_in_flight=batch_max_in_flight,
            executor_threads=executor_threads,
            enable_list=enable_list,
            enable_crc32=enable_crc32,
            commit_level=commit_level,
            replica=replica,
            send_key=send_key,
            username=username,
            password=password,
            tls_name=tls_name,
        )

    @classmethod
    def from_storage_plugin_config(
        cls,
        extra_config: dict[str, Any] | None,
        plugin_name: str,
    ) -> AerospikeConfig:
        """Parse ``storage_plugin.<name>.*`` keys from LMCache extra_config."""
        return cls.from_extra_config(
            extra_config,
            plugin_name,
            config_prefix=f"storage_plugin.{plugin_name}",
        )

    @classmethod
    def from_adapter_params(
        cls,
        params: dict[str, Any] | None,
        plugin_name: str = "aerospike",
    ) -> AerospikeConfig:
        """Parse flat L2 ``adapter_params`` dict (hosts, namespace, …)."""
        data = params or {}
        remapped: dict[str, Any] = {
            f"remote_storage_plugin.{plugin_name}.{k}": v for k, v in data.items()
        }
        return cls.from_extra_config(remapped, plugin_name)
