"""Operator CLI for submitting, watching, and inspecting primegrid jobs."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid.client.api import MasterClient
from primegrid.common.config import load_client_config
from primegrid.common.logging import configure_logging

_STATUS_NAMES = {
    pb.JOB_STATUS_UNSPECIFIED: "UNSPECIFIED",
    pb.JOB_STATUS_PENDING: "PENDING",
    pb.JOB_STATUS_RUNNING: "RUNNING",
    pb.JOB_STATUS_COMPLETED: "COMPLETED",
    pb.JOB_STATUS_FAILED: "FAILED",
    pb.JOB_STATUS_CANCELLED: "CANCELLED",
}


def _status_name(status: int) -> str:
    return _STATUS_NAMES.get(status, str(status))


def _add_connection_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--master", help="master address host:port")
    parser.add_argument("--rpc-timeout", type=float, help="unary RPC deadline in seconds")
    parser.add_argument("--job-timeout", type=float, help="maximum wait for a job")
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING, ERROR")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="primegrid-client", description=__doc__)
    _add_connection_options(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit = subparsers.add_parser("submit", help="submit a new job")
    submit.add_argument("--start", type=int, required=True)
    submit.add_argument("--end", type=int, required=True)
    submit.add_argument("--chunk-size", type=int, default=0)
    submit.add_argument("--mode", choices=("sieve", "trial"), default="sieve")
    submit.add_argument(
        "--request-id",
        help="stable idempotency key; reuse it when intentionally retrying a submit",
    )

    status = subparsers.add_parser("status", help="show job progress")
    status.add_argument("--job-id", required=True)

    watch = subparsers.add_parser("watch", help="stream job progress")
    watch.add_argument("--job-id", required=True)
    watch.add_argument("--interval-ms", type=int, default=250)

    result = subparsers.add_parser("result", help="fetch the final job result")
    result.add_argument("--job-id", required=True)

    cancel = subparsers.add_parser("cancel", help="cancel a job")
    cancel.add_argument("--job-id", required=True)

    subparsers.add_parser("workers", help="list registered workers")

    bench = subparsers.add_parser("bench", help="run benchmark scenarios")
    bench.add_argument("bench_args", nargs=argparse.REMAINDER)
    return parser


def _client_from_args(args: argparse.Namespace) -> MasterClient:
    config = load_client_config(
        master_address=args.master,
        rpc_deadline=args.rpc_timeout,
        job_timeout=args.job_timeout,
        log_level=args.log_level,
    )
    configure_logging(config.log_level)
    return MasterClient(config)


def _print_progress(progress: pb.JobProgress) -> None:
    print(
        f"job={progress.job_id} status={_status_name(progress.status)} "
        f"done={progress.done_chunks}/{progress.total_chunks} "
        f"progress={progress.progress:.1%} primes={progress.prime_count} "
        f"max_prime={progress.max_prime} elapsed_ms={progress.elapsed_ms}"
    )


def _run(args: argparse.Namespace) -> int:
    if args.command == "bench":
        from primegrid.client.benchmark import main as benchmark_main

        return benchmark_main(args.bench_args)

    client = _client_from_args(args)
    try:
        if args.command == "submit":
            response = client.submit(
                start=args.start,
                end=args.end,
                chunk_size=args.chunk_size,
                mode=args.mode,
                request_id=args.request_id,
            )
            print(
                f"job_id={response.job_id} status={_status_name(response.status)} "
                f"deduplicated={str(response.deduplicated).lower()}"
            )
            return 0

        if args.command == "status":
            _print_progress(client.get_status(args.job_id))
            return 0

        if args.command == "watch":
            for progress in client.watch(args.job_id):
                _print_progress(progress)
            return 0

        if args.command == "result":
            result = client.get_result(args.job_id)
            print(f"job_id={result.job_id} status={_status_name(result.status)}")
            print(
                f"prime_count={result.prime_count} max_prime={result.max_prime} "
                f"chunks={result.done_chunks}/{result.total_chunks} "
                f"elapsed_ms={result.elapsed_ms}"
            )
            if result.error:
                print(f"error={result.error}", file=sys.stderr)
            for worker in result.per_worker_stats:
                print(
                    f"worker={worker.worker_id} chunks={worker.chunks_done} "
                    f"compute_ms={worker.compute_ms}"
                )
            return 0

        if args.command == "cancel":
            response = client.cancel(args.job_id)
            print(
                f"job_id={response.job_id} status={_status_name(response.status)} "
                f"cancelled={str(response.cancelled).lower()}"
            )
            return 0

        if args.command == "workers":
            workers = client.list_workers()
            if not workers:
                print("no workers registered")
            for worker in workers:
                status = "ALIVE" if worker.status == pb.WORKER_STATUS_ALIVE else "DEAD"
                print(
                    f"id={worker.worker_id} address={worker.address} cores={worker.cores} "
                    f"status={status} last_heartbeat_ms={worker.last_heartbeat_ms} "
                    f"chunks={worker.chunks_done} compute_ms={worker.compute_ms}"
                )
            return 0
    except (grpc.RpcError, TimeoutError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()
    return 2


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    return _run(args)


if __name__ == "__main__":
    raise SystemExit(main())
