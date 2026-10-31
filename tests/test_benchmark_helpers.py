"""Pure benchmark helpers are deterministic and use the reference sieve."""

import socket

import pytest

from primegrid.client.benchmark import _expected_count, _free_port, _parse_ints


def test_parse_ints_accepts_comma_separated_values() -> None:
    assert _parse_ints("1, 5,20") == [1, 5, 20]


def test_parse_ints_rejects_empty_input() -> None:
    with pytest.raises(argparse_error()):
        _parse_ints(" , ")


def test_free_port_returns_a_bindable_ephemeral_port() -> None:
    port = _free_port("127.0.0.1")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", port))


def test_expected_count_uses_known_references() -> None:
    assert _expected_count(1, 10_000) == 1_229
    assert _expected_count(1, 100_000) == 9_592


def argparse_error():
    import argparse

    return argparse.ArgumentTypeError
