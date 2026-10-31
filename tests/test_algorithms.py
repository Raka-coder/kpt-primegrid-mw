"""Prime algorithms must agree with the PRD reference values."""

import pytest

from primegrid.worker.algorithms import count_primes, is_prime_trial


@pytest.mark.parametrize("mode", ["sieve", "trial"])
@pytest.mark.parametrize("limit,expected", [(10_000, 1229), (100_000, 9592)])
def test_algorithms_match_reference_counts(mode: str, limit: int, expected: int) -> None:
    count, largest = count_primes(1, limit + 1, mode)

    assert count == expected
    assert largest <= limit


@pytest.mark.parametrize("mode", ["sieve", "trial"])
def test_algorithms_handle_unaligned_half_open_ranges(mode: str) -> None:
    count_full, largest_full = count_primes(1, 1000, mode)
    count_left, _ = count_primes(1, 501, mode)
    count_right, _ = count_primes(501, 1000, mode)

    assert count_left + count_right == count_full
    assert largest_full == count_primes(900, 1000, mode)[1]


@pytest.mark.parametrize("mode", ["sieve", "trial"])
def test_algorithms_handle_ranges_below_two(mode: str) -> None:
    assert count_primes(0, 2, mode) == (0, 0)
    assert count_primes(2, 3, mode) == (1, 2)
    assert count_primes(-5, 2, mode) == (0, 0)


def test_trial_prime_edge_cases() -> None:
    assert [n for n in range(30) if is_prime_trial(n)] == [
        2,
        3,
        5,
        7,
        11,
        13,
        17,
        19,
        23,
        29,
    ]


def test_unknown_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="mode"):
        count_primes(1, 10, "bogus")
