"""Run the primegrid worker process."""

from __future__ import annotations

import argparse

from primegrid.common.config import load_worker_config
from primegrid.common.logging import configure_logging
from primegrid.worker.server import WorkerServer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="primegrid worker")
    parser.add_argument(
        "--master-ip",
        help="master IP or hostname (env PRIMEGRID_MASTER_IP)",
    )
    parser.add_argument(
        "--master-port",
        type=int,
        help="master port (env PRIMEGRID_MASTER_PORT, default 50050)",
    )
    parser.add_argument(
        "--master",
        help="legacy combined master address host:port; overrides --master-ip",
    )
    parser.add_argument("--host", help="bind host (default 0.0.0.0)")
    parser.add_argument("--port", type=int, help="bind port (default 50061)")
    parser.add_argument("--worker-id", help="unique worker identifier")
    parser.add_argument(
        "--advertise-host",
        help="IP advertised to the master; auto-detected when omitted",
    )
    parser.add_argument(
        "--advertise-port",
        type=int,
        help="port advertised to the master; 0 uses --port",
    )
    parser.add_argument("--cores", type=int, help="reported CPU cores")
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING, ERROR")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_worker_config(
        master_address=args.master,
        master_ip=args.master_ip,
        master_port=args.master_port,
        host=args.host,
        port=args.port,
        worker_id=args.worker_id,
        advertise_host=args.advertise_host,
        advertise_port=args.advertise_port,
        cores=args.cores,
        log_level=args.log_level,
    )
    configure_logging(config.log_level)
    server = WorkerServer(config)
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
