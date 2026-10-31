"""Environment and CLI driven configuration for every primegrid process."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, replace
from typing import TypeVar

from primegrid.common.retry import RetryPolicy


T = TypeVar("T")
_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def _env(name: str, default: T) -> T:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    if isinstance(default, bool):
        lowered = value.strip().lower()
        if lowered in _TRUE:
            return True  # type: ignore[return-value]
        if lowered in _FALSE:
            return False  # type: ignore[return-value]
        raise ValueError(f"{name} must be a boolean, got {value!r}")
    if isinstance(default, int):
        try:
            return int(value)  # type: ignore[return-value]
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer, got {value!r}") from exc
    if isinstance(default, float):
        try:
            return float(value)  # type: ignore[return-value]
        except ValueError as exc:
            raise ValueError(f"{name} must be a number, got {value!r}") from exc
    return value  # type: ignore[return-value]


def _positive(name: str, value: int | float, *, allow_zero: bool = False) -> None:
    if value < 0 or (value == 0 and not allow_zero):
        comparator = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must be {comparator}, got {value!r}")


@dataclass(frozen=True)
class MasterConfig:
    """Master bind, scheduling, retry, and health-check settings."""

    host: str = "0.0.0.0"
    port: int = 50050
    max_workers: int = 64
    heartbeat_interval: float = 2.0
    worker_dead_after: float = 6.0
    health_interval: float = 1.0
    process_deadline: float = 60.0
    rpc_deadline: float = 3.0
    max_retries: int = 3
    process_max_retries: int = 2
    backoff_base: float = 0.5
    backoff_max: float = 10.0
    chunk_attempt_limit: int = 3
    dedup_ttl: float = 3600.0
    default_chunk_size: int = 100_000
    max_range: int = 10_000_000
    max_chunks: int = 100_000
    job_stall_timeout: float = 600.0
    log_level: str = "INFO"
    shutdown_grace: float = 5.0

    def __post_init__(self) -> None:
        _positive("port", self.port, allow_zero=True)
        _positive("max_workers", self.max_workers)
        _positive("heartbeat_interval", self.heartbeat_interval)
        _positive("worker_dead_after", self.worker_dead_after)
        if self.worker_dead_after <= self.heartbeat_interval:
            raise ValueError("WORKER_DEAD_AFTER must be greater than HEARTBEAT_INTERVAL")
        _positive("health_interval", self.health_interval)
        _positive("process_deadline", self.process_deadline)
        _positive("rpc_deadline", self.rpc_deadline)
        _positive("max_retries", self.max_retries)
        _positive("process_max_retries", self.process_max_retries)
        _positive("chunk_attempt_limit", self.chunk_attempt_limit)
        _positive("default_chunk_size", self.default_chunk_size)
        _positive("max_range", self.max_range)
        _positive("max_chunks", self.max_chunks)
        _positive("job_stall_timeout", self.job_stall_timeout)
        if self.max_range < self.default_chunk_size:
            raise ValueError("MAX_RANGE must be at least DEFAULT_CHUNK_SIZE")

    @property
    def retry_policy(self) -> RetryPolicy:
        return RetryPolicy(self.max_retries, self.backoff_base, self.backoff_max)

    @property
    def process_retry_policy(self) -> RetryPolicy:
        return RetryPolicy(self.process_max_retries, self.backoff_base, self.backoff_max)


@dataclass(frozen=True)
class WorkerConfig:
    """Worker identity, registration, heartbeat, and fault injection settings."""

    worker_id: str = ""
    host: str = "0.0.0.0"
    port: int = 50061
    advertise_host: str = "localhost"
    advertise_port: int = 50061
    cores: int = 1
    master_address: str = "localhost:50050"
    heartbeat_interval: float = 2.0
    rpc_deadline: float = 3.0
    max_retries: int = 3
    backoff_base: float = 0.5
    backoff_max: float = 10.0
    max_workers: int = 2
    fault_delay_probability: float = 0.0
    fault_delay_max_ms: int = 0
    fault_error_probability: float = 0.0
    log_level: str = "INFO"
    shutdown_grace: float = 5.0

    def __post_init__(self) -> None:
        if not self.worker_id:
            object.__setattr__(
                self,
                "worker_id",
                f"worker-{socket.gethostname()}-{os.getpid()}",
            )
        _positive("port", self.port, allow_zero=True)
        _positive("advertise_port", self.advertise_port, allow_zero=True)
        _positive("cores", self.cores)
        _positive("heartbeat_interval", self.heartbeat_interval)
        _positive("rpc_deadline", self.rpc_deadline)
        _positive("max_retries", self.max_retries)
        _positive("max_workers", self.max_workers)
        if not 0.0 <= self.fault_delay_probability <= 1.0:
            raise ValueError("FAULT_DELAY_PROBABILITY must be between 0 and 1")
        if not 0.0 <= self.fault_error_probability <= 1.0:
            raise ValueError("FAULT_ERROR_PROBABILITY must be between 0 and 1")
        if self.fault_delay_max_ms < 0:
            raise ValueError("FAULT_DELAY_MAX_MS must be non-negative")

    @property
    def retry_policy(self) -> RetryPolicy:
        return RetryPolicy(self.max_retries, self.backoff_base, self.backoff_max)

    @property
    def advertised_address(self) -> str:
        return f"{self.advertise_host}:{self.advertise_port}"


@dataclass(frozen=True)
class ClientConfig:
    """Client endpoint, deadline, retry, and streaming settings."""

    master_address: str = "localhost:50050"
    rpc_deadline: float = 3.0
    job_timeout: float = 3600.0
    max_retries: int = 3
    backoff_base: float = 0.5
    backoff_max: float = 10.0
    watch_interval_ms: int = 250
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if not self.master_address:
            raise ValueError("master address must not be empty")
        _positive("rpc_deadline", self.rpc_deadline)
        _positive("job_timeout", self.job_timeout)
        _positive("max_retries", self.max_retries)
        _positive("watch_interval_ms", self.watch_interval_ms)

    @property
    def retry_policy(self) -> RetryPolicy:
        return RetryPolicy(self.max_retries, self.backoff_base, self.backoff_max)


def load_master_config(**overrides: object) -> MasterConfig:
    """Load master configuration from environment, then apply CLI overrides."""
    values: dict[str, object] = {
        "host": _env("PRIMEGRID_MASTER_HOST", "0.0.0.0"),
        "port": _env("PRIMEGRID_MASTER_PORT", 50050),
        "max_workers": _env("PRIMEGRID_MASTER_THREADS", 64),
        "heartbeat_interval": _env("PRIMEGRID_HEARTBEAT_INTERVAL", 2.0),
        "worker_dead_after": _env("PRIMEGRID_WORKER_DEAD_AFTER", 6.0),
        "health_interval": _env("PRIMEGRID_HEALTH_INTERVAL", 1.0),
        "process_deadline": _env("PRIMEGRID_PROCESS_DEADLINE", 60.0),
        "rpc_deadline": _env("PRIMEGRID_RPC_DEADLINE", 3.0),
        "max_retries": _env("PRIMEGRID_MAX_RETRIES", 3),
        "process_max_retries": _env("PRIMEGRID_PROCESS_MAX_RETRIES", 2),
        "backoff_base": _env("PRIMEGRID_BACKOFF_BASE", 0.5),
        "backoff_max": _env("PRIMEGRID_BACKOFF_MAX", 10.0),
        "chunk_attempt_limit": _env("PRIMEGRID_CHUNK_ATTEMPT_LIMIT", 3),
        "dedup_ttl": _env("PRIMEGRID_DEDUP_TTL", 3600.0),
        "default_chunk_size": _env("PRIMEGRID_DEFAULT_CHUNK_SIZE", 100_000),
        "max_range": _env("PRIMEGRID_MAX_RANGE", 10_000_000),
        "max_chunks": _env("PRIMEGRID_MAX_CHUNKS", 100_000),
        "job_stall_timeout": _env("PRIMEGRID_JOB_STALL_TIMEOUT", 600.0),
        "log_level": _env("PRIMEGRID_LOG_LEVEL", "INFO"),
        "shutdown_grace": _env("PRIMEGRID_SHUTDOWN_GRACE", 5.0),
    }
    values.update({key: value for key, value in overrides.items() if value is not None})
    return MasterConfig(**values)  # type: ignore[arg-type]


def load_worker_config(**overrides: object) -> WorkerConfig:
    """Load worker configuration from environment, then apply CLI overrides."""
    values: dict[str, object] = {
        "worker_id": _env("PRIMEGRID_WORKER_ID", ""),
        "host": _env("PRIMEGRID_WORKER_HOST", "0.0.0.0"),
        "port": _env("PRIMEGRID_WORKER_PORT", 50061),
        "advertise_host": _env("PRIMEGRID_WORKER_ADVERTISE_HOST", "localhost"),
        "advertise_port": _env("PRIMEGRID_WORKER_ADVERTISE_PORT", 50061),
        "cores": _env("PRIMEGRID_WORKER_CORES", 1),
        "master_address": _env("PRIMEGRID_MASTER_ADDRESS", "localhost:50050"),
        "heartbeat_interval": _env("PRIMEGRID_HEARTBEAT_INTERVAL", 2.0),
        "rpc_deadline": _env("PRIMEGRID_RPC_DEADLINE", 3.0),
        "max_retries": _env("PRIMEGRID_MAX_RETRIES", 3),
        "backoff_base": _env("PRIMEGRID_BACKOFF_BASE", 0.5),
        "backoff_max": _env("PRIMEGRID_BACKOFF_MAX", 10.0),
        "max_workers": _env("PRIMEGRID_WORKER_THREADS", 2),
        "fault_delay_probability": _env("PRIMEGRID_FAULT_DELAY_PROBABILITY", 0.0),
        "fault_delay_max_ms": _env("PRIMEGRID_FAULT_DELAY_MAX_MS", 0),
        "fault_error_probability": _env("PRIMEGRID_FAULT_ERROR_PROBABILITY", 0.0),
        "log_level": _env("PRIMEGRID_LOG_LEVEL", "INFO"),
        "shutdown_grace": _env("PRIMEGRID_SHUTDOWN_GRACE", 5.0),
    }
    values.update({key: value for key, value in overrides.items() if value is not None})
    return WorkerConfig(**values)  # type: ignore[arg-type]


def load_client_config(**overrides: object) -> ClientConfig:
    """Load client configuration from environment, then apply CLI overrides."""
    values: dict[str, object] = {
        "master_address": _env("PRIMEGRID_MASTER_ADDRESS", "localhost:50050"),
        "rpc_deadline": _env("PRIMEGRID_RPC_DEADLINE", 3.0),
        "job_timeout": _env("PRIMEGRID_JOB_TIMEOUT", 3600.0),
        "max_retries": _env("PRIMEGRID_MAX_RETRIES", 3),
        "backoff_base": _env("PRIMEGRID_BACKOFF_BASE", 0.5),
        "backoff_max": _env("PRIMEGRID_BACKOFF_MAX", 10.0),
        "watch_interval_ms": _env("PRIMEGRID_WATCH_INTERVAL_MS", 250),
        "log_level": _env("PRIMEGRID_LOG_LEVEL", "INFO"),
    }
    values.update({key: value for key, value in overrides.items() if value is not None})
    return ClientConfig(**values)  # type: ignore[arg-type]


def with_overrides(config: T, **overrides: object) -> T:
    """Return a dataclass copy with non-None overrides, for CLI composition."""
    return replace(config, **{key: value for key, value in overrides.items() if value is not None})
