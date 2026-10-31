"""Worker-side gRPC servicer with single-chunk compute serialization."""

from __future__ import annotations

import random
import threading
import time

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from primegrid.common.config import WorkerConfig
from primegrid.common.logging import get_logger
from primegrid.worker.algorithms import count_primes

_MODE_NAMES = {
    pb.ALGORITHM_MODE_SIEVE: "sieve",
    pb.ALGORITHM_MODE_TRIAL: "trial",
}


class WorkerServicer(rpc.WorkerServiceServicer):
    """ProcessChunk and Ping implementation for one worker process."""

    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.logger = get_logger("worker", worker_id=config.worker_id)
        self._compute_lock = threading.Lock()

    @property
    def active_chunks(self) -> int:
        return 1 if self._compute_lock.locked() else 0

    def ProcessChunk(
        self,
        request: pb.ProcessChunkRequest,
        context: grpc.ServicerContext,
    ) -> pb.ProcessChunkResponse:
        if request.chunk_id < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "chunk_id must be non-negative")
        if request.start < 0 or request.end <= request.start:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "chunk range must be non-empty and non-negative",
            )
        mode = _MODE_NAMES.get(request.mode)
        if mode is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unknown algorithm mode")

        if (
            self.config.fault_error_probability > 0
            and random.random() < self.config.fault_error_probability
        ):
            self.logger.warning(
                "injected worker failure",
                extra={
                    "job_id": request.job_id,
                    "chunk_id": request.chunk_id,
                    "worker_id": self.config.worker_id,
                },
            )
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "injected transient worker failure",
            )

        with self._compute_lock:
            if (
                self.config.fault_delay_probability > 0
                and self.config.fault_delay_max_ms > 0
                and random.random() < self.config.fault_delay_probability
            ):
                delay_ms = random.randint(0, self.config.fault_delay_max_ms)
                time.sleep(delay_ms / 1000.0)

            started = time.monotonic()
            try:
                prime_count, max_prime = count_primes(request.start, request.end, mode)
            except ValueError as exc:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            except Exception as exc:  # pragma: no cover - defensive boundary
                self.logger.exception(
                    "chunk computation failed",
                    extra={
                        "job_id": request.job_id,
                        "chunk_id": request.chunk_id,
                        "worker_id": self.config.worker_id,
                    },
                )
                context.abort(grpc.StatusCode.INTERNAL, f"compute failed: {exc}")
            duration_ms = max(0, int((time.monotonic() - started) * 1000))

        self.logger.info(
            "chunk computed",
            extra={
                "job_id": request.job_id,
                "chunk_id": request.chunk_id,
                "worker_id": self.config.worker_id,
                "duration_ms": duration_ms,
            },
        )
        return pb.ProcessChunkResponse(
            job_id=request.job_id,
            chunk_id=request.chunk_id,
            prime_count=prime_count,
            max_prime=max_prime,
            compute_ms=duration_ms,
            success=True,
        )

    def Ping(
        self,
        request: pb.PingRequest,
        context: grpc.ServicerContext,
    ) -> pb.PingResponse:
        return pb.PingResponse(healthy=True, worker_id=self.config.worker_id)
