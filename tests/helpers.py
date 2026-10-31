"""Shared test fixtures for in-process gRPC clusters."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from primegrid.common.config import MasterConfig, WorkerConfig
from primegrid.common.retry import RetryPolicy
from primegrid.master.server import MasterServer
from primegrid.worker.server import WorkerServer


@dataclass
class Cluster:
    master: MasterServer
    workers: list[WorkerServer]

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.master.port}"

    def stub(self) -> rpc.MasterServiceStub:
        return rpc.MasterServiceStub(grpc.insecure_channel(self.address))


def _wait_for_workers(stub: rpc.MasterServiceStub, expected: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            workers = stub.ListWorkers(pb.ListWorkersRequest(), timeout=1.0).workers
        except grpc.RpcError:
            workers = []
        alive = [worker for worker in workers if worker.status == pb.WORKER_STATUS_ALIVE]
        if len(alive) >= expected:
            return
        time.sleep(0.025)
    raise AssertionError(f"{expected} workers did not register within {timeout}s")


@contextmanager
def running_cluster(
    worker_count: int = 3,
    *,
    fault_delay_probability: float = 0.0,
    fault_delay_max_ms: int = 0,
    process_deadline: float = 8.0,
) -> Iterator[Cluster]:
    master_config = MasterConfig(
        host="127.0.0.1",
        port=0,
        max_workers=64,
        heartbeat_interval=0.2,
        worker_dead_after=0.8,
        health_interval=0.05,
        process_deadline=process_deadline,
        rpc_deadline=2.0,
        max_retries=2,
        process_max_retries=1,
        backoff_base=0.0,
        backoff_max=0.0,
        chunk_attempt_limit=3,
        dedup_ttl=60.0,
        default_chunk_size=5_000,
        max_range=10_000_000,
        max_chunks=100_000,
        job_stall_timeout=60.0,
        log_level="WARNING",
        shutdown_grace=1.0,
    )
    master = MasterServer(master_config)
    master.start()
    workers: list[WorkerServer] = []
    try:
        for index in range(worker_count):
            worker_config = WorkerConfig(
                worker_id=f"worker-{index + 1}",
                host="127.0.0.1",
                port=0,
                advertise_host="127.0.0.1",
                advertise_port=0,
                cores=1,
                master_address=f"127.0.0.1:{master.port}",
                heartbeat_interval=0.2,
                rpc_deadline=2.0,
                max_retries=4,
                backoff_base=0.0,
                backoff_max=0.0,
                max_workers=2,
                fault_delay_probability=fault_delay_probability,
                fault_delay_max_ms=fault_delay_max_ms,
                log_level="WARNING",
                shutdown_grace=1.0,
            )
            worker = WorkerServer(worker_config)
            worker.start()
            workers.append(worker)

        _wait_for_workers(master.stub(), worker_count, timeout=5.0)
        yield Cluster(master=master, workers=workers)
    finally:
        for worker in workers:
            worker.stop()
        master.stop()


def wait_for_terminal(
    stub: rpc.MasterServiceStub,
    job_id: str,
    *,
    timeout: float = 20.0,
    poll_interval: float = 0.02,
) -> pb.JobProgress:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        response = stub.GetJobStatus(pb.GetJobStatusRequest(job_id=job_id), timeout=2.0)
        last = response.progress
        if last.status in (
            pb.JOB_STATUS_COMPLETED,
            pb.JOB_STATUS_FAILED,
            pb.JOB_STATUS_CANCELLED,
        ):
            return last
        time.sleep(poll_interval)
    raise AssertionError(f"job {job_id} did not finish; last progress={last}")


def submit(
    stub: rpc.MasterServiceStub,
    request_id: str,
    start: int,
    end: int,
    chunk_size: int,
    mode: pb.AlgorithmMode,
) -> pb.SubmitJobResponse:
    request = pb.SubmitJobRequest(
        request_id=request_id,
        start=start,
        end=end,
        chunk_size=chunk_size,
        mode=mode,
    )
    return stub.SubmitJob(request, timeout=3.0)


TEST_RETRY = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0)
