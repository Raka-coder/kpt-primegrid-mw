"""gRPC status-code mapping for invalid, missing, and cancelled requests."""

from __future__ import annotations

import grpc
import pytest

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from tests.helpers import running_cluster


def _code(callable_) -> grpc.StatusCode:
    with pytest.raises(grpc.RpcError) as caught:
        callable_()
    return caught.value.code()


def test_master_validation_and_not_found_codes() -> None:
    with running_cluster(worker_count=3) as cluster:
        stub = cluster.stub()

        assert (
            _code(
                lambda: stub.SubmitJob(
                    pb.SubmitJobRequest(
                        request_id="bad-mode",
                        start=1,
                        end=10,
                        mode=pb.ALGORITHM_MODE_UNSPECIFIED,
                    ),
                    timeout=2.0,
                )
            )
            == grpc.StatusCode.INVALID_ARGUMENT
        )
        assert (
            _code(
                lambda: stub.SubmitJob(
                    pb.SubmitJobRequest(
                        request_id="bad-range",
                        start=10,
                        end=1,
                        mode=pb.ALGORITHM_MODE_SIEVE,
                    ),
                    timeout=2.0,
                )
            )
            == grpc.StatusCode.INVALID_ARGUMENT
        )
        assert (
            _code(lambda: stub.GetJobStatus(pb.GetJobStatusRequest(job_id="missing"), timeout=2.0))
            == grpc.StatusCode.NOT_FOUND
        )
        assert (
            _code(lambda: stub.GetResult(pb.GetResultRequest(job_id="missing"), timeout=2.0))
            == grpc.StatusCode.NOT_FOUND
        )


def test_result_precondition_and_cancel_idempotency() -> None:
    with running_cluster(worker_count=3) as cluster:
        stub = cluster.stub()
        submitted = stub.SubmitJob(
            pb.SubmitJobRequest(
                request_id="cancel-me",
                start=1,
                end=1_000_000,
                chunk_size=10_000,
                mode=pb.ALGORITHM_MODE_TRIAL,
            ),
            timeout=2.0,
        )
        assert (
            _code(lambda: stub.GetResult(pb.GetResultRequest(job_id=submitted.job_id), timeout=2.0))
            == grpc.StatusCode.FAILED_PRECONDITION
        )

        first = stub.CancelJob(pb.CancelJobRequest(job_id=submitted.job_id), timeout=2.0)
        second = stub.CancelJob(pb.CancelJobRequest(job_id=submitted.job_id), timeout=2.0)
        assert first.status == pb.JOB_STATUS_CANCELLED
        assert first.cancelled is True
        assert second.status == pb.JOB_STATUS_CANCELLED
        assert second.cancelled is True


def test_worker_ping_and_input_validation() -> None:
    with running_cluster(worker_count=1) as cluster:
        worker = cluster.workers[0]
        stub = rpc.WorkerServiceStub(grpc.insecure_channel(f"127.0.0.1:{worker.port}"))
        ping = stub.Ping(pb.PingRequest(worker_id="worker-1"), timeout=2.0)
        assert ping.healthy is True
        assert ping.worker_id == "worker-1"

        assert (
            _code(
                lambda: stub.ProcessChunk(
                    pb.ProcessChunkRequest(
                        job_id="j",
                        chunk_id=0,
                        start=10,
                        end=1,
                        mode=pb.ALGORITHM_MODE_SIEVE,
                    ),
                    timeout=2.0,
                )
            )
            == grpc.StatusCode.INVALID_ARGUMENT
        )
