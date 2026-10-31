"""Reusable client API with deadlines, retries, and streamed progress."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from primegrid.common.config import ClientConfig
from primegrid.common.logging import get_logger
from primegrid.common.retry import RETRYABLE_CODES, call_with_retry

_TERMINAL = (
    pb.JOB_STATUS_COMPLETED,
    pb.JOB_STATUS_FAILED,
    pb.JOB_STATUS_CANCELLED,
)
_MODE_BY_NAME = {
    "sieve": pb.ALGORITHM_MODE_SIEVE,
    "trial": pb.ALGORITHM_MODE_TRIAL,
}


def _mode_value(mode: str | int) -> int:
    if isinstance(mode, str):
        try:
            return _MODE_BY_NAME[mode.strip().lower()]
        except KeyError as exc:
            raise ValueError(f"unknown mode: {mode!r}") from exc
    if mode in (pb.ALGORITHM_MODE_SIEVE, pb.ALGORITHM_MODE_TRIAL):
        return mode
    raise ValueError(f"unknown mode value: {mode!r}")


class MasterClient:
    """Synchronous client wrapper that reuses request IDs on retries."""

    def __init__(self, config: ClientConfig) -> None:
        self.config = config
        self.logger = get_logger("client")
        self._channel = grpc.insecure_channel(config.master_address)
        self.stub = rpc.MasterServiceStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> MasterClient:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def submit(
        self,
        *,
        start: int,
        end: int,
        chunk_size: int = 0,
        mode: str | int = "sieve",
        request_id: str | None = None,
    ) -> pb.SubmitJobResponse:
        """Submit a job. One request_id is generated per intent and reused by retries."""
        stable_request_id = request_id or str(uuid.uuid4())
        request = pb.SubmitJobRequest(
            request_id=stable_request_id,
            start=start,
            end=end,
            chunk_size=chunk_size,
            mode=_mode_value(mode),
        )
        return call_with_retry(
            self.stub.SubmitJob,
            request,
            timeout=self.config.rpc_deadline,
            policy=self.config.retry_policy,
            logger=self.logger,
            rpc_name="SubmitJob",
        )

    def get_status(self, job_id: str) -> pb.JobProgress:
        request = pb.GetJobStatusRequest(job_id=job_id)
        response = call_with_retry(
            self.stub.GetJobStatus,
            request,
            timeout=self.config.rpc_deadline,
            policy=self.config.retry_policy,
            logger=self.logger,
            rpc_name="GetJobStatus",
            job_id=job_id,
        )
        return response.progress

    def watch(self, job_id: str) -> Iterator[pb.JobProgress]:
        """Yield progress updates, reconnecting after transient stream failures."""
        deadline = time.monotonic() + self.config.job_timeout
        attempt = 0
        request = pb.WatchJobRequest(
            job_id=job_id,
            update_interval_ms=self.config.watch_interval_ms,
        )
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"job {job_id} did not finish before the client timeout")
            attempt += 1
            try:
                for response in self.stub.WatchJob(request, timeout=remaining):
                    progress = response.progress
                    yield progress
                    if progress.status in _TERMINAL:
                        return
            except grpc.RpcError as exc:
                code = exc.code()
                if code not in RETRYABLE_CODES:
                    raise
                self.logger.warning(
                    "watch stream interrupted; reconnecting",
                    extra={
                        "job_id": job_id,
                        "status": code.name if code else "UNKNOWN",
                        "attempt": attempt,
                    },
                )
            else:
                self.logger.warning(
                    "watch stream ended without a terminal update",
                    extra={"job_id": job_id, "attempt": attempt},
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"job {job_id} did not finish before the client timeout")
            time.sleep(min(self.config.retry_policy.delay(attempt), remaining))

    def wait_for_terminal(self, job_id: str) -> pb.JobProgress:
        last: pb.JobProgress | None = None
        for progress in self.watch(job_id):
            last = progress
            if progress.status in _TERMINAL:
                return progress
        if last is not None:
            return last
        raise TimeoutError(f"job {job_id} produced no progress updates")

    def get_result(self, job_id: str) -> pb.GetResultResponse:
        request = pb.GetResultRequest(job_id=job_id)
        return call_with_retry(
            self.stub.GetResult,
            request,
            timeout=self.config.rpc_deadline,
            policy=self.config.retry_policy,
            logger=self.logger,
            rpc_name="GetResult",
            job_id=job_id,
        )

    def cancel(self, job_id: str) -> pb.CancelJobResponse:
        request = pb.CancelJobRequest(job_id=job_id)
        return call_with_retry(
            self.stub.CancelJob,
            request,
            timeout=self.config.rpc_deadline,
            policy=self.config.retry_policy,
            logger=self.logger,
            rpc_name="CancelJob",
            job_id=job_id,
        )

    def list_workers(self) -> list[pb.WorkerInfo]:
        request = pb.ListWorkersRequest()
        response = call_with_retry(
            self.stub.ListWorkers,
            request,
            timeout=self.config.rpc_deadline,
            policy=self.config.retry_policy,
            logger=self.logger,
            rpc_name="ListWorkers",
        )
        return list(response.workers)
