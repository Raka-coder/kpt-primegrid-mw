"""IPv4 address helpers for manual multi-machine deployment."""

from __future__ import annotations

import socket


def parse_address(address: str) -> tuple[str, int]:
    """Parse a ``host:port`` string into its host and validated port."""
    host, separator, port_text = address.strip().rpartition(":")
    if not separator or not host or not port_text:
        raise ValueError(f"address must be host:port, got {address!r}")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"port must be an integer, got {port_text!r}") from exc
    if not 1 <= port <= 65_535:
        raise ValueError(f"port must be between 1 and 65535, got {port}")
    return host, port


def format_address(host: str, port: int) -> str:
    """Build a validated ``host:port`` string."""
    host = host.strip()
    if not host:
        raise ValueError("host must not be empty")
    if not 1 <= port <= 65_535:
        raise ValueError(f"port must be between 1 and 65535, got {port}")
    return f"{host}:{port}"


def detect_local_ip(remote_host: str, remote_port: int) -> str:
    """Return the local IPv4 address selected to reach a remote endpoint.

    A UDP socket does not send a packet, but the operating system still chooses
    the interface and source address that would be used for the route.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((remote_host, remote_port))
            address = sock.getsockname()[0]
    except OSError as exc:
        raise ValueError(
            f"could not determine a local IPv4 address for {remote_host}:{remote_port}; "
            "pass --advertise-host explicitly"
        ) from exc
    if not address or address == "0.0.0.0":
        raise ValueError(
            "could not determine a usable local IPv4 address; pass --advertise-host explicitly"
        )
    return address
