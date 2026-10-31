"""Master gRPC servicer: validation, status, streaming, and worker RPCs."""

from __future__ import annotations

from collections.abc import Iterator

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from primegrid.common.config import MasterConfig
from primegrid.common.logging import get_logger
from primegrid.master.dispatcher import DispatcherManager
from primegrid.master.job_manager import Job, JobManager
from primegrid.master.worker_registry import WorkerRegistry

_MODE_NAMES = {
    pb.ALGORITHM_MODE_SIEVE: "sieve",
    pb.ALGORITHM_MODE_TRIAL: "trial",
}
_MIN_WATCH_INTERVAL_MS = 50
_MAX_WATCH_INTERVAL_MS = 5_000
_TERMINAL = (
    pb.JOB_STATUS_COMPLETED,
    pb.JOB_STATUS_FAILED,
    pb.JOB_STATUS_CANCELLED,
)


class MasterServicer(rpc.MasterServiceServicer):
    """Implements all eight MasterService RPCs."""

    def __init__(
        self,
        *,
        config: MasterConfig,
        manager: JobManager,
        registry: WorkerRegistry,
        dispatchers: DispatcherManager,
    ) -> None:
        self.config = config
        self.manager = manager
        self.registry = registry
        self.dispatchers = dispatchers
        self.logger = get_logger("master")

    def SubmitJob(
        self,
        request: pb.SubmitJobRequest,
        context: grpc.ServicerContext,
    ) -> pb.SubmitJobResponse:
        mode = _MODE_NAMES.get(request.mode)
        if mode is None:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "mode must be ALGORITHM_MODE_SIEVE or ALGORITHM_MODE_TRIAL",
            )
        try:
            job, deduplicated = self.manager.submit(
                request.request_id,
                request.start,
                request.end,
                int(request.chunk_size),
                mode,
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except Exception as exc:  # pragma: no cover - defensive boundary
            self.logger.exception("SubmitJob failed")
            context.abort(grpc.StatusCode.INTERNAL, f"SubmitJob failed: {exc}")

        snapshot, _ = job.snapshot()
        return pb.SubmitJobResponse(
            job_id=job.job_id,
            deduplicated=deduplicated,
            status=snapshot.status,
        )

    def GetJobStatus(
        self,
        request: pb.GetJobStatusRequest,
        context: grpc.ServicerContext,
    ) -> pb.GetJobStatusResponse:
        job = self._require_job(request.job_id, context)
        progress, _ = job.snapshot()
        return pb.GetJobStatusResponse(progress=progress)

    def WatchJob(
        self,
        request: pb.WatchJobRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[pb.WatchJobResponse]:
        job = self._require_job(request.job_id, context)
        interval_ms = request.update_interval_ms or self.config.default_watch_interval_ms
        interval_ms = max(_MIN_WATCH_INTERVAL_MS, min(_MAX_WATCH_INTERVAL_MS, interval_ms))
        interval_seconds = interval_ms / 1000.0

        progress, version = job.snapshot()
        yield pb.WatchJobResponse(progress=progress)
        while progress.status not in _TERMINAL:
            if not context.is_active():
                return
            new_version = job.wait_for_change(version, timeout=interval_seconds)
            if not context.is_active():
                return
            if new_version == version:
                continue
            progress, version = job.snapshot()
            yield pb.WatchJobResponse(progress=progress)

    def GetResult(
        self,
        request: pb.GetResultRequest,
        context: grpc.ServicerContext,
    ) -> pb.GetResultResponse:
        job = self._require_job(request.job_id, context)
        progress, _ = job.snapshot()
        if progress.status not in _TERMINAL:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "job has not reached a terminal state",
            )
        return job.result()

    def CancelJob(
        self,
        request: pb.CancelJobRequest,
        context: grpc.ServicerContext,
    ) -> pb.CancelJobResponse:
        job = self._require_job(request.job_id, context)
        job.cancel()
        progress, _ = job.snapshot()
        return pb.CancelJobResponse(
            job_id=job.job_id,
            cancelled=progress.status == pb.JOB_STATUS_CANCELLED,
            status=progress.status,
        )

    def ListWorkers(
        self,
        request: pb.ListWorkersRequest,
        context: grpc.ServicerContext,
    ) -> pb.ListWorkersResponse:
        return pb.ListWorkersResponse(workers=self.registry.list_workers())

    def RegisterWorker(
        self,
        request: pb.RegisterWorkerRequest,
        context: grpc.ServicerContext,
    ) -> pb.RegisterWorkerResponse:
        try:
            registration = self.registry.register(
                request.worker_id,
                request.address,
                int(request.cores),
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

        self.dispatchers.ensure(request.worker_id, registration.record.address)
        self.logger.info(
            "worker registered",
            extra={
                "worker_id": request.worker_id,
                "worker_created": registration.created,
                "revived": registration.revived,
                "address": request.address,
            },
        )
        return pb.RegisterWorkerResponse(
            accepted=True,
            heartbeat_interval_ms=int(self.config.heartbeat_interval * 1000),
        )

    def Heartbeat(
        self,
        request: pb.HeartbeatRequest,
        context: grpc.ServicerContext,
    ) -> pb.HeartbeatResponse:
        result = self.registry.heartbeat(request.worker_id)
        if not result.found:
            context.abort(grpc.StatusCode.NOT_FOUND, "worker is not registered")
        if result.revived:
            record = self.registry.get(request.worker_id)
            if record is not None:
                self.dispatchers.ensure(request.worker_id, record.address)
                self.logger.info(
                    "worker revived",
                    extra={"worker_id": request.worker_id},
                )
        return pb.HeartbeatResponse(acknowledged=True, should_stop=False)

    def _require_job(self, job_id: str, context: grpc.ServicerContext) -> Job:
        job = self.manager.get(job_id)
        if job is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"job {job_id} not found")
        return job
