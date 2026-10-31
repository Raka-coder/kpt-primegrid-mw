"""Deadline-aware gRPC retry helper with bounded exponential backoff."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

import grpc


T = TypeVar("T")
RETRYABLE_CODES = frozenset(
    {
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNAVAILABLE,
    }
)


@dataclass(frozen=True)
class RetryPolicy:
    """Retry attempt budget and exponential backoff configuration."""

    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 10.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("retry delays must be non-negative")

    def delay(self, attempt: int) -> float:
        """Return a jittered delay for a 1-based failed attempt."""
        if self.base_delay == 0 or self.max_delay == 0:
            return 0.0
        raw = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        upper = min(raw * 1.1, max(raw, self.max_delay))
        return random.uniform(raw * 0.9, upper)


def call_with_retry(
    operation: Callable[..., T],
    request: Any,
    *,
    timeout: float,
    policy: RetryPolicy,
    logger: logging.LoggerAdapter | None = None,
    rpc_name: str = "rpc",
    **context: Any,
) -> T:
    """Call a unary gRPC operation, retrying only transient status codes.

    The same request object is reused on every attempt, preserving idempotency
    keys such as ``request_id``. Every attempt receives the same deadline.
    """
    last_error: grpc.RpcError | None = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return operation(request, timeout=timeout)
        except grpc.RpcError as exc:
            last_error = exc
            code = exc.code()
            if code not in RETRYABLE_CODES or attempt >= policy.max_attempts:
                raise
            delay = policy.delay(attempt)
            if logger is not None:
                logger.warning(
                    "retrying rpc",
                    extra={
                        **context,
                        "rpc": rpc_name,
                        "attempt": attempt,
                        "status": code.name,
                        "delay_s": round(delay, 3),
                    },
                )
            if delay > 0:
                time.sleep(delay)

    if last_error is not None:
        raise last_error
    raise RuntimeError("retry loop exited without a result")
