"""Worker gRPC server lifecycle."""

from __future__ import annotations

import threading
from concurrent import futures

import grpc

from primegrid import primegrid_pb2_grpc as rpc
from primegrid.common.config import WorkerConfig
from primegrid.common.logging import get_logger
from primegrid.worker.registrar import WorkerRegistrar
from primegrid.worker.servicer import WorkerServicer


class WorkerServer:
    """A running worker server plus its registration thread."""

    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.logger = get_logger("worker", worker_id=config.worker_id)
        self.servicer = WorkerServicer(config)
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=config.max_workers),
            maximum_concurrent_rpcs=config.max_workers,
        )
        rpc.add_WorkerServiceServicer_to_server(self.servicer, self._server)
        self.port = self._server.add_insecure_port(f"{config.host}:{config.port}")
        if self.port == 0:
            raise RuntimeError(f"could not bind worker server on {config.host}:{config.port}")
        advertise_port = config.advertise_port or self.port
        advertised_address = f"{config.advertise_host}:{advertise_port}"
        self.registrar = WorkerRegistrar(
            config,
            advertised_address,
            lambda: self.servicer.active_chunks,
        )
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._server.start()
            self._started = True
        self.registrar.start()
        self.logger.info(
            "worker server started",
            extra={
                "worker_id": self.config.worker_id,
                "address": f"{self.config.host}:{self.port}",
                "advertised": self.registrar.advertised_address,
            },
        )

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._started = False
        self.registrar.stop()
        self._server.stop(self.config.shutdown_grace).wait()
        self.logger.info("worker server stopped", extra={"worker_id": self.config.worker_id})

    def wait_for_termination(self) -> None:
        self._server.wait_for_termination()
