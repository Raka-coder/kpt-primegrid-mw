"""End-to-end gRPC tests for three workers, streaming, load, and failure."""

from __future__ import annotations

import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from tests.helpers import running_cluster, submit, wait_for_terminal


def test_three_workers_complete_reference_job_and_stream_progress() -> None:
    with running_cluster(
        worker_count=3,
        fault_delay_probability=1.0,
        fault_delay_max_ms=8,
    ) as cluster:
        stub = cluster.stub()
        request_id = f"itest-{uuid.uuid4()}"
        submitted = submit(
            stub,
            request_id,
            1,
            100_000,
            5_000,
            pb.ALGORITHM_MODE_SIEVE,
        )

        updates = list(
            stub.WatchJob(
                pb.WatchJobRequest(job_id=submitted.job_id, update_interval_ms=20),
                timeout=20.0,
            )
        )
        assert len(updates) >= 2
        assert updates[-1].progress.status == pb.JOB_STATUS_COMPLETED
        assert updates[-1].progress.done_chunks == updates[-1].progress.total_chunks

        result = stub.GetResult(pb.GetResultRequest(job_id=submitted.job_id), timeout=2.0)
        assert result.prime_count == 9_592
        assert result.max_prime == 99_991
        assert len(result.per_worker_stats) >= 2

        duplicate = submit(
            stub,
            request_id,
            1,
            100_000,
            5_000,
            pb.ALGORITHM_MODE_SIEVE,
        )
        assert duplicate.job_id == submitted.job_id
        assert duplicate.deduplicated is True

        workers = stub.ListWorkers(pb.ListWorkersRequest(), timeout=2.0).workers
        assert len(workers) == 3
        assert all(worker.status == pb.WORKER_STATUS_ALIVE for worker in workers)


def test_twenty_concurrent_clients_get_correct_results() -> None:
    with running_cluster(worker_count=3) as cluster:
        reference = 2_262  # pi(20_000)

        def run_client(index: int) -> tuple[str, int]:
            stub = rpc.MasterServiceStub(grpc.insecure_channel(cluster.address))
            response = submit(
                stub,
                f"load-{index}-{uuid.uuid4()}",
                1,
                20_000,
                1_000,
                pb.ALGORITHM_MODE_SIEVE,
            )
            wait_for_terminal(stub, response.job_id, timeout=30.0)
            result = stub.GetResult(pb.GetResultRequest(job_id=response.job_id), timeout=2.0)
            return response.job_id, result.prime_count

        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(run_client, range(20)))

        assert len({job_id for job_id, _ in results}) == 20
        assert all(count == reference for _, count in results)


def test_worker_loss_mid_job_keeps_result_correct() -> None:
    with running_cluster(
        worker_count=3,
        fault_delay_probability=1.0,
        fault_delay_max_ms=30,
        process_deadline=5.0,
    ) as cluster:
        stub = cluster.stub()
        response = submit(
            stub,
            f"failure-{uuid.uuid4()}",
            1,
            200_000,
            2_000,
            pb.ALGORITHM_MODE_SIEVE,
        )

        time.sleep(0.12)
        cluster.workers[1].stop()

        progress = wait_for_terminal(stub, response.job_id, timeout=30.0)
        assert progress.status == pb.JOB_STATUS_COMPLETED
        assert progress.prime_count == 17_984
        assert progress.max_prime == 199_999

        workers = stub.ListWorkers(pb.ListWorkersRequest(), timeout=2.0).workers
        dead = [worker for worker in workers if worker.status == pb.WORKER_STATUS_DEAD]
        assert len(dead) == 1
