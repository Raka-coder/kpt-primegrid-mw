"""Retry helper retries only transient gRPC status codes and respects attempts."""

import grpc
import pytest

from primegrid.common.retry import RetryPolicy, call_with_retry


class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


def test_retries_transient_errors_until_success() -> None:
    calls = 0

    def operation(request: str, timeout: float) -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise FakeRpcError(grpc.StatusCode.UNAVAILABLE)
        return f"{request}:{timeout}"

    result = call_with_retry(
        operation,
        "same-request",
        timeout=2.0,
        policy=RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0),
    )

    assert result == "same-request:2.0"
    assert calls == 3


def test_non_retryable_status_is_raised_immediately() -> None:
    calls = 0

    def operation(request: str, timeout: float) -> str:
        nonlocal calls
        calls += 1
        raise FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT)

    with pytest.raises(FakeRpcError):
        call_with_retry(
            operation,
            "request",
            timeout=2.0,
            policy=RetryPolicy(max_attempts=5, base_delay=0.0, max_delay=0.0),
        )

    assert calls == 1


def test_retryable_error_is_raised_after_attempt_budget() -> None:
    calls = 0

    def operation(request: str, timeout: float) -> str:
        nonlocal calls
        calls += 1
        raise FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED)

    with pytest.raises(FakeRpcError):
        call_with_retry(
            operation,
            "request",
            timeout=2.0,
            policy=RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0),
        )

    assert calls == 3


def test_backoff_is_bounded_and_exponential() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay=0.5, max_delay=2.0)

    assert 0.45 <= policy.delay(1) <= 0.55
    assert 0.9 <= policy.delay(2) <= 1.1
    assert 1.8 <= policy.delay(3) <= 2.2
    assert 1.8 <= policy.delay(5) <= 2.2
