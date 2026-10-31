"""Chunking must cover inclusive user ranges exactly once with half-open chunks."""

import pytest

from primegrid.master.job_manager import split_range


def test_chunks_cover_range_without_gaps_or_overlap() -> None:
    chunks = split_range(1, 10_000, 1_000)

    assert chunks[0] == (1, 1_001)
    assert chunks[-1] == (9_001, 10_001)
    flattened = [value for start, end in chunks for value in range(start, end)]
    assert flattened == list(range(1, 10_001))


def test_last_chunk_absorbs_remainder() -> None:
    chunks = split_range(10, 25, 6)

    assert chunks == [(10, 16), (16, 22), (22, 26)]


def test_single_value_and_oversized_chunk() -> None:
    assert split_range(7, 7, 100) == [(7, 8)]
    assert split_range(1, 5, 100) == [(1, 6)]


@pytest.mark.parametrize(
    "start,end,chunk_size",
    [(10, 1, 5), (-1, 10, 5), (1, 10, 0), (1, 10, -2)],
)
def test_invalid_chunk_requests_are_rejected(start: int, end: int, chunk_size: int) -> None:
    with pytest.raises(ValueError):
        split_range(start, end, chunk_size)
