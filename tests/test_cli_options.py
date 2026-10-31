"""CLI parsers expose dynamic master address options."""

from primegrid.client.cli import build_parser as build_client_parser
from primegrid.worker.__main__ import build_parser as build_worker_parser


def test_worker_parser_accepts_master_ip_and_port() -> None:
    args = build_worker_parser().parse_args(
        ["--master-ip", "192.168.1.10", "--master-port", "50050"]
    )

    assert args.master_ip == "192.168.1.10"
    assert args.master_port == 50050
    assert args.master is None


def test_worker_parser_keeps_legacy_master_option() -> None:
    args = build_worker_parser().parse_args(["--master", "192.168.1.10:50050"])

    assert args.master == "192.168.1.10:50050"


def test_client_parser_accepts_master_ip_and_port() -> None:
    args = build_client_parser().parse_args(
        ["--master-ip", "192.168.1.10", "--master-port", "50050", "workers"]
    )

    assert args.master_ip == "192.168.1.10"
    assert args.master_port == 50050
    assert args.command == "workers"
