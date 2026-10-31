"""Benchmark orchestrator for speedup, concurrent clients, and failure demos.

The script launches a real master and real worker processes. Separate processes
are essential: running CPU-bound workers as threads in one process would be
limited by Python's GIL and would invalidate the speedup measurement.
"""

from __future__ import annotations

import argparse
import csv
import os
import platform
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from concurrent import futures
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from primegrid.client.api import MasterClient
from primegrid.common.config import ClientConfig
from primegrid.worker.algorithms import sieve_count

SRC_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = SRC_ROOT.parent
TERMINAL = {
    pb.JOB_STATUS_COMPLETED,
    pb.JOB_STATUS_FAILED,
    pb.JOB_STATUS_CANCELLED,
}


def _free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _child_environment(extra: dict[str, str]) -> dict[str, str]:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{SRC_ROOT}{os.pathsep}{existing}" if existing else str(SRC_ROOT)
    env.update(extra)
    return env


def _mean(values: Sequence[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _stdev(values: Sequence[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def _parse_ints(value: str) -> list[int]:
    items = [item.strip() for item in value.split(",") if item.strip()]
    if not items:
        raise argparse.ArgumentTypeError("expected a comma-separated list of integers")
    return [int(item) for item in items]


@lru_cache(maxsize=16)
def _expected_count(start: int, end: int) -> int:
    count, _ = sieve_count(start, end + 1)
    return count


def _client_config(address: str, job_timeout: float) -> ClientConfig:
    return ClientConfig(
        master_address=address,
        rpc_deadline=10.0,
        job_timeout=job_timeout,
        max_retries=4,
        backoff_base=0.5,
        backoff_max=10.0,
        watch_interval_ms=250,
        log_level="WARNING",
    )


@dataclass
class WorkerProcess:
    worker_id: str
    port: int
    process: subprocess.Popen[bytes]
    log_path: Path


class ProcessCluster:
    """A disposable master plus N worker subprocesses with ephemeral ports."""

    def __init__(
        self,
        worker_count: int,
        *,
        host: str,
        log_dir: Path,
        verbose: bool,
        fault_delay_probability: float = 0.0,
        fault_delay_max_ms: int = 0,
        fault_error_probability: float = 0.0,
    ) -> None:
        self.worker_count = worker_count
        self.host = host
        self.log_dir = log_dir
        self.verbose = verbose
        self.fault_delay_probability = fault_delay_probability
        self.fault_delay_max_ms = fault_delay_max_ms
        self.fault_error_probability = fault_error_probability
        self.master_port = _free_port(host)
        self.master_process: subprocess.Popen[bytes] | None = None
        self.workers: list[WorkerProcess] = []
        self._log_handles: list[Any] = []
        self.log_dir.mkdir(parents=True, exist_ok=True)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.master_port}"

    def __enter__(self) -> ProcessCluster:
        self.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.stop()

    def _open_log(self, name: str):
        if self.verbose:
            return None, self.log_dir / f"{name}.log"
        path = self.log_dir / f"{name}.log"
        handle = open(path, "ab")
        self._log_handles.append(handle)
        return handle, path

    def start(self) -> None:
        log_handle, log_path = self._open_log("master")
        master_env = _child_environment(
            {
                "PRIMEGRID_MASTER_HOST": self.host,
                "PRIMEGRID_MASTER_PORT": str(self.master_port),
                "PRIMEGRID_HEARTBEAT_INTERVAL": "0.5",
                "PRIMEGRID_WORKER_DEAD_AFTER": "2.0",
                "PRIMEGRID_HEALTH_INTERVAL": "0.25",
                "PRIMEGRID_PROCESS_DEADLINE": "120.0",
                "PRIMEGRID_RPC_DEADLINE": "5.0",
                "PRIMEGRID_MAX_RETRIES": "4",
                "PRIMEGRID_PROCESS_MAX_RETRIES": "3",
                "PRIMEGRID_BACKOFF_BASE": "0.25",
                "PRIMEGRID_BACKOFF_MAX": "5.0",
                "PRIMEGRID_CHUNK_ATTEMPT_LIMIT": "3",
                "PRIMEGRID_JOB_STALL_TIMEOUT": "600.0",
                "PRIMEGRID_LOG_LEVEL": "INFO",
                "PRIMEGRID_SHUTDOWN_GRACE": "1.0",
            }
        )
        self.master_process = subprocess.Popen(
            [sys.executable, "-m", "primegrid.master"],
            cwd=REPO_ROOT,
            env=master_env,
            stdout=log_handle,
            stderr=subprocess.STDOUT if log_handle is not None else None,
        )
        self._wait_for_master(log_path)

        for index in range(self.worker_count):
            worker_id = f"bench-worker-{index + 1}"
            worker_port = _free_port(self.host)
            handle, path = self._open_log(worker_id)
            worker_env = _child_environment(
                {
                    "PRIMEGRID_WORKER_ID": worker_id,
                    "PRIMEGRID_WORKER_HOST": self.host,
                    "PRIMEGRID_WORKER_PORT": str(worker_port),
                    "PRIMEGRID_WORKER_ADVERTISE_HOST": self.host,
                    "PRIMEGRID_WORKER_ADVERTISE_PORT": str(worker_port),
                    "PRIMEGRID_WORKER_CORES": "1",
                    "PRIMEGRID_MASTER_ADDRESS": self.address,
                    "PRIMEGRID_HEARTBEAT_INTERVAL": "0.5",
                    "PRIMEGRID_RPC_DEADLINE": "5.0",
                    "PRIMEGRID_MAX_RETRIES": "4",
                    "PRIMEGRID_BACKOFF_BASE": "0.25",
                    "PRIMEGRID_BACKOFF_MAX": "5.0",
                    "PRIMEGRID_FAULT_DELAY_PROBABILITY": str(self.fault_delay_probability),
                    "PRIMEGRID_FAULT_DELAY_MAX_MS": str(self.fault_delay_max_ms),
                    "PRIMEGRID_FAULT_ERROR_PROBABILITY": str(self.fault_error_probability),
                    "PRIMEGRID_LOG_LEVEL": "INFO",
                    "PRIMEGRID_SHUTDOWN_GRACE": "1.0",
                }
            )
            process = subprocess.Popen(
                [sys.executable, "-m", "primegrid.worker"],
                cwd=REPO_ROOT,
                env=worker_env,
                stdout=handle,
                stderr=subprocess.STDOUT if handle is not None else None,
            )
            self.workers.append(WorkerProcess(worker_id, worker_port, process, path))

        self._wait_for_workers()

    def _wait_for_master(self, log_path: Path) -> None:
        channel = grpc.insecure_channel(self.address)
        try:
            grpc.channel_ready_future(channel).result(timeout=15.0)
        except Exception as exc:
            self.stop()
            raise RuntimeError(
                f"master did not start at {self.address}: {exc}\n{_tail(log_path)}"
            ) from exc
        finally:
            channel.close()

    def _wait_for_workers(self) -> None:
        channel = grpc.insecure_channel(self.address)
        stub = rpc.MasterServiceStub(channel)
        deadline = time.monotonic() + 20.0
        try:
            while time.monotonic() < deadline:
                try:
                    workers = stub.ListWorkers(pb.ListWorkersRequest(), timeout=1.0).workers
                except grpc.RpcError:
                    workers = []
                alive = [worker for worker in workers if worker.status == pb.WORKER_STATUS_ALIVE]
                failed = [worker for worker in self.workers if worker.process.poll() is not None]
                if failed:
                    worker = failed[0]
                    raise RuntimeError(f"{worker.worker_id} exited early\n{_tail(worker.log_path)}")
                if len(alive) >= self.worker_count:
                    return
                time.sleep(0.05)
            raise RuntimeError("workers did not register within 20 seconds")
        finally:
            channel.close()

    def stop_worker(self, index: int) -> None:
        worker = self.workers[index]
        if worker.process.poll() is None:
            worker.process.terminate()
            try:
                worker.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker.process.kill()
                worker.process.wait(timeout=5)

    def stop(self) -> None:
        for worker in self.workers:
            if worker.process.poll() is None:
                worker.process.terminate()
        for worker in self.workers:
            try:
                worker.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker.process.kill()
                worker.process.wait(timeout=5)
        if self.master_process is not None and self.master_process.poll() is None:
            self.master_process.terminate()
            try:
                self.master_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.master_process.kill()
                self.master_process.wait(timeout=5)
        for handle in self._log_handles:
            handle.close()
        self._log_handles.clear()


def _tail(path: Path, lines: int = 30) -> str:
    try:
        content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return f"(no log at {path})"
    return "\n".join(content[-lines:])


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _environment_row() -> dict[str, Any]:
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count() or 0,
    }


def _run_one_job(
    client: MasterClient,
    *,
    start: int,
    end: int,
    chunk_size: int,
    mode: str,
    request_id: str,
) -> tuple[float, pb.GetResultResponse]:
    began = time.perf_counter()
    submitted = client.submit(
        start=start,
        end=end,
        chunk_size=chunk_size,
        mode=mode,
        request_id=request_id,
    )
    client.wait_for_terminal(submitted.job_id)
    result = client.get_result(submitted.job_id)
    return time.perf_counter() - began, result


def scenario_a(args: argparse.Namespace) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    expected = _expected_count(args.start, args.end)
    env = _environment_row()
    worker_counts = _parse_ints(args.worker_counts)
    for worker_count in worker_counts:
        print(f"[A] launching {worker_count} worker(s)")
        with ProcessCluster(
            worker_count,
            host=args.host,
            log_dir=Path(args.output_dir) / "logs",
            verbose=args.verbose,
        ) as cluster:
            client = MasterClient(_client_config(cluster.address, args.job_timeout))
            try:
                for warmup in range(args.warmup):
                    print(f"[A] workers={worker_count} warmup {warmup + 1}")
                    elapsed, result = _run_one_job(
                        client,
                        start=args.start,
                        end=args.end,
                        chunk_size=args.chunk_size,
                        mode=args.mode,
                        request_id=f"bench-a-{worker_count}-warmup-{warmup}-{time.time_ns()}",
                    )
                    print(f"    warmup elapsed={elapsed:.3f}s primes={result.prime_count}")
                for run in range(1, args.repeat + 1):
                    elapsed, result = _run_one_job(
                        client,
                        start=args.start,
                        end=args.end,
                        chunk_size=args.chunk_size,
                        mode=args.mode,
                        request_id=f"bench-a-{worker_count}-{run}-{time.time_ns()}",
                    )
                    correct = result.prime_count == expected
                    rows.append(
                        {
                            **env,
                            "scenario": "A",
                            "workers": worker_count,
                            "run": run,
                            "start": args.start,
                            "end": args.end,
                            "chunk_size": args.chunk_size,
                            "mode": args.mode,
                            "elapsed_s": round(elapsed, 6),
                            "prime_count": result.prime_count,
                            "max_prime": result.max_prime,
                            "expected": expected,
                            "correct": correct,
                        }
                    )
                    print(
                        f"[A] workers={worker_count} run={run} "
                        f"elapsed={elapsed:.3f}s correct={correct}"
                    )
            finally:
                client.close()

    baseline = [row["elapsed_s"] for row in rows if row["workers"] == 1]
    baseline_mean = _mean(baseline) if baseline else 0.0
    for row in rows:
        if baseline_mean > 0:
            speedup = baseline_mean / row["elapsed_s"]
            row["speedup"] = round(speedup, 4)
            row["efficiency"] = round(speedup / row["workers"], 4)
        else:
            row["speedup"] = 0.0
            row["efficiency"] = 0.0
    return rows


def _load_client(
    address: str,
    args: argparse.Namespace,
    client_index: int,
    barrier: threading.Barrier,
    results: list[dict[str, Any]],
    lock: threading.Lock,
    job_store: dict[str, str],
) -> None:
    client = MasterClient(_client_config(address, args.job_timeout))
    try:
        barrier.wait(timeout=30)
        began = time.perf_counter()
        submitted = client.submit(
            start=args.b_start,
            end=args.b_end,
            chunk_size=args.b_chunk_size,
            mode="sieve",
            request_id=f"bench-b-job-{client_index}-{time.time_ns()}",
        )
        client.wait_for_terminal(submitted.job_id)
        result = client.get_result(submitted.job_id)
        elapsed = time.perf_counter() - began
        with lock:
            results.append(
                {
                    "elapsed_s": elapsed,
                    "prime_count": result.prime_count,
                    "max_prime": result.max_prime,
                    "job_id": submitted.job_id,
                }
            )
            job_store.setdefault("job_id", submitted.job_id)
    finally:
        client.close()


def _status_load_client(
    address: str,
    job_timeout: float,
    reference_job_id: str,
    request_count: int,
    barrier: threading.Barrier,
    latency_lock: threading.Lock,
    latencies: list[float],
    errors: list[BaseException],
) -> None:
    client = MasterClient(_client_config(address, job_timeout))
    try:
        barrier.wait(timeout=30)
        for _ in range(request_count):
            began = time.perf_counter()
            client.get_status(reference_job_id)
            latency = time.perf_counter() - began
            with latency_lock:
                latencies.append(latency)
    except BaseException as exc:  # pragma: no cover - load diagnostic
        with latency_lock:
            errors.append(exc)
    finally:
        client.close()


def scenario_b(args: argparse.Namespace) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    job_rows: list[dict[str, Any]] = []
    status_rows: list[dict[str, Any]] = []
    expected = _expected_count(args.b_start, args.b_end)
    env = _environment_row()
    client_counts = _parse_ints(args.client_counts)
    with ProcessCluster(
        3,
        host=args.host,
        log_dir=Path(args.output_dir) / "logs",
        verbose=args.verbose,
    ) as cluster:
        last_job_id: str | None = None
        for client_count in client_counts:
            for run in range(1, args.repeat + 1):
                print(f"[B] {client_count} concurrent job clients, run={run}")
                barrier = threading.Barrier(client_count)
                results: list[dict[str, Any]] = []
                lock = threading.Lock()
                job_store: dict[str, str] = {}
                began = time.perf_counter()
                with futures.ThreadPoolExecutor(max_workers=client_count) as executor:
                    pending = [
                        executor.submit(
                            _load_client,
                            cluster.address,
                            args,
                            index,
                            barrier,
                            results,
                            lock,
                            job_store,
                        )
                        for index in range(client_count)
                    ]
                    for future in pending:
                        future.result()
                wall = time.perf_counter() - began
                correct = sum(1 for item in results if item["prime_count"] == expected)
                latencies = [item["elapsed_s"] for item in results]
                last_job_id = job_store.get("job_id", last_job_id)
                job_rows.append(
                    {
                        **env,
                        "scenario": "B-jobs",
                        "workers": 3,
                        "clients": client_count,
                        "run": run,
                        "start": args.b_start,
                        "end": args.b_end,
                        "chunk_size": args.b_chunk_size,
                        "mode": "sieve",
                        "jobs": len(results),
                        "wall_s": round(wall, 6),
                        "latency_mean_s": round(_mean(latencies), 6),
                        "latency_stdev_s": round(_stdev(latencies), 6),
                        "throughput_jobs_s": round(len(results) / wall, 4) if wall else 0.0,
                        "expected": expected,
                        "correct_jobs": correct,
                    }
                )
                print(
                    f"    wall={wall:.3f}s throughput={len(results) / wall:.2f} jobs/s "
                    f"correct={correct}/{len(results)}"
                )

        if last_job_id is None:
            client = MasterClient(_client_config(cluster.address, args.job_timeout))
            try:
                submitted = client.submit(start=1, end=1_000, chunk_size=500, mode="sieve")
                client.wait_for_terminal(submitted.job_id)
                last_job_id = submitted.job_id
            finally:
                client.close()

        for client_count in client_counts:
            for run in range(1, args.repeat + 1):
                print(f"[B] {client_count} status clients, run={run}")
                barrier = threading.Barrier(client_count)
                latency_lock = threading.Lock()
                latencies: list[float] = []
                errors: list[BaseException] = []

                wall_began = time.perf_counter()
                with futures.ThreadPoolExecutor(max_workers=client_count) as executor:
                    pending = [
                        executor.submit(
                            _status_load_client,
                            cluster.address,
                            args.job_timeout,
                            last_job_id,
                            args.status_requests,
                            barrier,
                            latency_lock,
                            latencies,
                            errors,
                        )
                        for _ in range(client_count)
                    ]
                    for future in pending:
                        future.result()
                wall = time.perf_counter() - wall_began
                if errors:
                    raise RuntimeError(f"status load failed: {errors[0]}")
                status_rows.append(
                    {
                        **env,
                        "scenario": "B-status",
                        "workers": 3,
                        "clients": client_count,
                        "run": run,
                        "requests_per_client": args.status_requests,
                        "requests": len(latencies),
                        "wall_s": round(wall, 6),
                        "latency_mean_s": round(_mean(latencies), 6),
                        "latency_stdev_s": round(_stdev(latencies), 6),
                        "throughput_req_s": round(len(latencies) / wall, 4) if wall else 0.0,
                        "errors": len(errors),
                    }
                )
                print(f"    requests={len(latencies)} throughput={len(latencies) / wall:.2f} req/s")
    return job_rows, status_rows


def scenario_c(args: argparse.Namespace) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    env = _environment_row()

    try:
        with ProcessCluster(
            3,
            host=args.host,
            log_dir=Path(args.output_dir) / "logs",
            verbose=args.verbose,
        ) as cluster:
            client = MasterClient(_client_config(cluster.address, args.job_timeout))
            try:
                request_id = f"bench-c-duplicate-{time.time_ns()}"
                first = client.submit(
                    start=1, end=10_000, chunk_size=1_000, mode="sieve", request_id=request_id
                )
                second = client.submit(
                    start=1, end=10_000, chunk_size=1_000, mode="sieve", request_id=request_id
                )
                passed = first.job_id == second.job_id and second.deduplicated
                rows.append(
                    {
                        **env,
                        "scenario": "C",
                        "case": "duplicate_submit",
                        "passed": passed,
                        "detail": f"job_id={first.job_id} deduplicated={second.deduplicated}",
                    }
                )
                print(f"[C] duplicate_submit passed={passed}")
            finally:
                client.close()
    except Exception as exc:
        rows.append(
            {
                **env,
                "scenario": "C",
                "case": "duplicate_submit",
                "passed": False,
                "detail": repr(exc),
            }
        )

    try:
        with ProcessCluster(
            3,
            host=args.host,
            log_dir=Path(args.output_dir) / "logs",
            verbose=args.verbose,
            fault_delay_probability=1.0,
            fault_delay_max_ms=30,
        ) as cluster:
            client = MasterClient(_client_config(cluster.address, args.job_timeout))
            try:
                submitted = client.submit(start=1, end=200_000, chunk_size=2_000, mode="sieve")
                time.sleep(0.15)
                cluster.stop_worker(1)
                progress = client.wait_for_terminal(submitted.job_id)
                result = client.get_result(submitted.job_id)
                expected = _expected_count(1, 200_000)
                passed = (
                    progress.status == pb.JOB_STATUS_COMPLETED and result.prime_count == expected
                )
                rows.append(
                    {
                        **env,
                        "scenario": "C",
                        "case": "worker_loss",
                        "passed": passed,
                        "detail": (
                            f"status={progress.status} primes={result.prime_count} "
                            f"expected={expected}"
                        ),
                    }
                )
                print(f"[C] worker_loss passed={passed}")
            finally:
                client.close()
    except Exception as exc:
        rows.append(
            {
                **env,
                "scenario": "C",
                "case": "worker_loss",
                "passed": False,
                "detail": repr(exc),
            }
        )

    try:
        with ProcessCluster(
            3,
            host=args.host,
            log_dir=Path(args.output_dir) / "logs",
            verbose=args.verbose,
            fault_delay_probability=0.8,
            fault_delay_max_ms=20,
            fault_error_probability=0.1,
        ) as cluster:
            client = MasterClient(_client_config(cluster.address, args.job_timeout))
            try:
                submitted = client.submit(start=1, end=100_000, chunk_size=5_000, mode="sieve")
                progress = client.wait_for_terminal(submitted.job_id)
                result = client.get_result(submitted.job_id)
                expected = _expected_count(1, 100_000)
                passed = (
                    progress.status == pb.JOB_STATUS_COMPLETED and result.prime_count == expected
                )
                rows.append(
                    {
                        **env,
                        "scenario": "C",
                        "case": "fault_injection_retry",
                        "passed": passed,
                        "detail": (
                            f"status={progress.status} primes={result.prime_count} "
                            f"expected={expected}"
                        ),
                    }
                )
                print(f"[C] fault_injection_retry passed={passed}")
            finally:
                client.close()
    except Exception as exc:
        rows.append(
            {
                **env,
                "scenario": "C",
                "case": "fault_injection_retry",
                "passed": False,
                "detail": repr(exc),
            }
        )
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=("A", "B", "C", "all"), default="A")
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--host", default="127.0.0.1", help="benchmark bind host")
    parser.add_argument("--worker-counts", default="1,2,3")
    parser.add_argument("--client-counts", default="1,5,20")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=10_000_000)
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--mode", choices=("sieve", "trial"), default="trial")
    parser.add_argument("--b-start", type=int, default=1)
    parser.add_argument("--b-end", type=int, default=1_000_000)
    parser.add_argument("--b-chunk-size", type=int, default=10_000)
    parser.add_argument("--status-requests", type=int, default=100)
    parser.add_argument("--job-timeout", type=float, default=7200.0)
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    if args.repeat < 1 or args.warmup < 0:
        raise SystemExit("--repeat must be >= 1 and --warmup must be >= 0")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "environment.txt").write_text(
        "\n".join(f"{key}={value}" for key, value in _environment_row().items()) + "\n",
        encoding="utf-8",
    )

    if args.scenario in ("A", "all"):
        rows = scenario_a(args)
        _write_rows(output_dir / "scenario_a.csv", rows)
        print(f"wrote {output_dir / 'scenario_a.csv'}")
    if args.scenario in ("B", "all"):
        job_rows, status_rows = scenario_b(args)
        _write_rows(output_dir / "scenario_b_jobs.csv", job_rows)
        _write_rows(output_dir / "scenario_b_status.csv", status_rows)
        print(f"wrote {output_dir / 'scenario_b_jobs.csv'}")
        print(f"wrote {output_dir / 'scenario_b_status.csv'}")
    if args.scenario in ("C", "all"):
        rows = scenario_c(args)
        _write_rows(output_dir / "scenario_c.csv", rows)
        print(f"wrote {output_dir / 'scenario_c.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
