"""Worker registration and heartbeat client thread."""

from __future__ import annotations

import threading

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from primegrid.common.config import WorkerConfig
from primegrid.common.logging import get_logger
from primegrid.common.retry import call_with_retry


class WorkerRegistrar:
    """Registers with the master, then heartbeats until stopped."""

    def __init__(
        self,
        config: WorkerConfig,
        advertised_address: str,
        active_chunks,
    ) -> None:
        self.config = config
        self.advertised_address = advertised_address
        self._active_chunks = active_chunks
        self.logger = get_logger("worker", worker_id=config.worker_id)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._channel = grpc.insecure_channel(config.master_address)
        self._stub = rpc.MasterServiceStub(self._channel)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name=f"registrar-{self.config.worker_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._channel.close()

    def _register(self) -> bool:
        request = pb.RegisterWorkerRequest(
            worker_id=self.config.worker_id,
            address=self.advertised_address,
            cores=self.config.cores,
        )
        try:
            response = call_with_retry(
                self._stub.RegisterWorker,
                request,
                timeout=self.config.rpc_deadline,
                policy=self.config.retry_policy,
                logger=self.logger,
                rpc_name="RegisterWorker",
                worker_id=self.config.worker_id,
            )
        except grpc.RpcError as exc:
            self.logger.error(
                "worker registration failed",
                extra={
                    "worker_id": self.config.worker_id,
                    "status": exc.code().name if exc.code() else "UNKNOWN",
                },
            )
            return False
        if response.accepted:
            self.logger.info(
                "worker registered",
                extra={"worker_id": self.config.worker_id},
            )
            return True
        self.logger.error(
            "master rejected worker registration",
            extra={"worker_id": self.config.worker_id},
        )
        return False

    def _run(self) -> None:
        while not self._stop.is_set() and not self._register():
            self._stop.wait(max(0.5, self.config.backoff_base))

        while not self._stop.is_set():
            request = pb.HeartbeatRequest(
                worker_id=self.config.worker_id,
                active_chunks=self._active_chunks(),
            )
            try:
                response = call_with_retry(
                    self._stub.Heartbeat,
                    request,
                    timeout=self.config.rpc_deadline,
                    policy=self.config.retry_policy,
                    logger=self.logger,
                    rpc_name="Heartbeat",
                    worker_id=self.config.worker_id,
                )
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.NOT_FOUND:
                    self.logger.warning(
                        "master forgot worker; re-registering",
                        extra={"worker_id": self.config.worker_id},
                    )
                    while not self._stop.is_set() and not self._register():
                        self._stop.wait(max(0.5, self.config.backoff_base))
                    continue
                self.logger.warning(
                    "heartbeat failed",
                    extra={
                        "worker_id": self.config.worker_id,
                        "status": exc.code().name if exc.code() else "UNKNOWN",
                    },
                )
            else:
                if response.should_stop:
                    self._stop.set()
                    return
            self._stop.wait(self.config.heartbeat_interval)
