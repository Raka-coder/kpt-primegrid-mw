"""Segmented sieve and deliberately CPU-heavy trial-division prime counters."""

from __future__ import annotations

from math import isqrt


def is_prime_trial(value: int) -> bool:
    """Return whether value is prime using odd trial divisors up to sqrt(value)."""
    if value < 2:
        return False
    if value == 2:
        return True
    if value % 2 == 0:
        return False
    divisor = 3
    limit = isqrt(value)
    while divisor <= limit:
        if value % divisor == 0:
            return False
        divisor += 2
    return True


def _base_primes(limit: int) -> list[int]:
    if limit < 2:
        return []
    flags = bytearray(b"\x01") * (limit + 1)
    flags[0:2] = b"\x00\x00"
    for number in range(2, isqrt(limit) + 1):
        if flags[number]:
            first = number * number
            flags[first::number] = b"\x00" * (((limit - first) // number) + 1)
    return [number for number in range(2, limit + 1) if flags[number]]


def sieve_count(start: int, end: int) -> tuple[int, int]:
    """Count primes in the half-open interval [start, end) with a segmented sieve."""
    first = max(start, 2)
    if end <= first:
        return 0, 0
    size = end - first
    segment = bytearray(b"\x01") * size
    for prime in _base_primes(isqrt(end - 1)):
        first_multiple = max(prime * prime, ((first + prime - 1) // prime) * prime)
        if first_multiple >= end:
            continue
        offset = first_multiple - first
        segment[offset::prime] = b"\x00" * (((size - 1 - offset) // prime) + 1)

    count = sum(segment)
    largest = 0
    for index in range(size - 1, -1, -1):
        if segment[index]:
            largest = first + index
            break
    return count, largest


def trial_count(start: int, end: int) -> tuple[int, int]:
    """Count primes in [start, end) using trial division for each candidate."""
    first = max(start, 2)
    if end <= first:
        return 0, 0
    count = 0
    largest = 0
    for value in range(first, end):
        if is_prime_trial(value):
            count += 1
            largest = value
    return count, largest


def count_primes(start: int, end: int, mode: str) -> tuple[int, int]:
    """Dispatch to the requested algorithm for the half-open range [start, end)."""
    normalized = mode.strip().lower()
    if normalized == "sieve":
        return sieve_count(start, end)
    if normalized == "trial":
        return trial_count(start, end)
    raise ValueError(f"Unknown mode: {mode!r}; expected 'sieve' or 'trial'")
