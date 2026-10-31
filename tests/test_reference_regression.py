"""Full-range reference regressions from PRD section 8."""

import pytest

from primegrid.worker.algorithms import count_primes


def test_sieve_full_range_reference() -> None:
    count, largest = count_primes(1, 10_000_001, "sieve")

    assert count == 664_579
    assert largest == 9_999_991


@pytest.mark.slow
def test_trial_full_range_reference() -> None:
    count, largest = count_primes(1, 10_000_001, "trial")

    assert count == 664_579
    assert largest == 9_999_991
