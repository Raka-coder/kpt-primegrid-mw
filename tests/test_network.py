"""IPv4 address helpers used by manual two-laptop deployment."""

import pytest

from primegrid.common.network import detect_local_ip, format_address, parse_address


def test_parse_and_format_address() -> None:
    assert parse_address("192.168.1.10:50050") == ("192.168.1.10", 50050)
    assert format_address("192.168.1.10", 50050) == "192.168.1.10:50050"


@pytest.mark.parametrize(
    "address",
    ["", "missing-port", "host:not-a-port", "host:0", "host:65536"],
)
def test_invalid_addresses_are_rejected(address: str) -> None:
    with pytest.raises(ValueError):
        parse_address(address)


def test_invalid_format_arguments_are_rejected() -> None:
    with pytest.raises(ValueError, match="host"):
        format_address("", 50050)
    with pytest.raises(ValueError, match="port"):
        format_address("host", 0)


def test_detect_local_ip_uses_loopback_for_loopback_peer() -> None:
    assert detect_local_ip("127.0.0.1", 50050) == "127.0.0.1"
