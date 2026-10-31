"""Client API reuses one request_id across retries and waits for terminals."""

from __future__ import annotations

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid.client.api import MasterClient
from primegrid.common.config import ClientConfig
from tests.helpers import running_cluster


class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return "transient"


class FlakySubmitStub:
    def __init__(self) -> None:
        self.requests: list[tuple[str, float]] = []

    def SubmitJob(self, request: pb.SubmitJobRequest, timeout: float) -> pb.SubmitJobResponse:
        self.requests.append((request.request_id, timeout))
        if len(self.requests) == 1:
            raise FakeRpcError(grpc.StatusCode.UNAVAILABLE)
        return pb.SubmitJobResponse(
            job_id="job-1",
            deduplicated=False,
            status=pb.JOB_STATUS_PENDING,
        )


def test_submit_reuses_the_same_request_id_on_retry() -> None:
    config = ClientConfig(
        master_address="unused:1",
        rpc_deadline=2.0,
        max_retries=2,
        backoff_base=0.0,
        backoff_max=0.0,
    )
    client = MasterClient(config)
    fake = FlakySubmitStub()
    client.stub = fake  # type: ignore[assignment]
    try:
        response = client.submit(
            start=1,
            end=10,
            chunk_size=10,
            mode="sieve",
            request_id="stable-request",
        )
    finally:
        client.close()

    assert response.job_id == "job-1"
    assert fake.requests == [("stable-request", 2.0), ("stable-request", 2.0)]


def test_client_waits_and_fetches_result() -> None:
    with running_cluster(worker_count=3) as cluster:
        config = ClientConfig(
            master_address=cluster.address,
            rpc_deadline=2.0,
            job_timeout=20.0,
            max_retries=3,
            backoff_base=0.0,
            backoff_max=0.0,
            watch_interval_ms=20,
        )
        with MasterClient(config) as client:
            submitted = client.submit(
                start=1,
                end=50_000,
                chunk_size=5_000,
                mode="sieve",
                request_id="client-api-job",
            )
            duplicate = client.submit(
                start=1,
                end=50_000,
                chunk_size=5_000,
                mode="sieve",
                request_id="client-api-job",
            )
            assert duplicate.job_id == submitted.job_id
            assert duplicate.deduplicated is True

            progress = client.wait_for_terminal(submitted.job_id)
            result = client.get_result(submitted.job_id)
            workers = client.list_workers()

        assert progress.status == pb.JOB_STATUS_COMPLETED
        assert result.prime_count == 5_133
        assert result.max_prime == 49_999
        assert len(workers) == 3
