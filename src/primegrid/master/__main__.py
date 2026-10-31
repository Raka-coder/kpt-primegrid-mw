"""Run the primegrid master process."""

from __future__ import annotations

import argparse

from primegrid.common.config import load_master_config
from primegrid.common.logging import configure_logging
from primegrid.master.server import MasterServer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="primegrid master")
    parser.add_argument("--host", help="bind host (env PRIMEGRID_MASTER_HOST)")
    parser.add_argument("--port", type=int, help="bind port (env PRIMEGRID_MASTER_PORT)")
    parser.add_argument("--threads", type=int, help="RPC thread pool size")
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING, ERROR")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_master_config(
        host=args.host,
        port=args.port,
        max_workers=args.threads,
        log_level=args.log_level,
    )
    configure_logging(config.log_level)
    server = MasterServer(config)
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
