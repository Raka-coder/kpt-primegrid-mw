"""Master gRPC server lifecycle and background coordination threads."""

from __future__ import annotations

import threading
from concurrent import futures

import grpc

from primegrid import primegrid_pb2_grpc as rpc
from primegrid.common.config import MasterConfig
from primegrid.common.logging import get_logger
from primegrid.master.dispatcher import DispatcherManager
from primegrid.master.job_manager import JobManager
from primegrid.master.maintenance import MaintenanceLoop
from primegrid.master.servicer import MasterServicer
from primegrid.master.worker_registry import WorkerRegistry


class MasterServer:
    """Owns the job manager, worker registry, dispatchers, and gRPC server."""

    def __init__(self, config: MasterConfig) -> None:
        self.config = config
        self.logger = get_logger("master")
        self.manager = JobManager(config)
        self.registry = WorkerRegistry(config)
        self.dispatchers = DispatcherManager(
            config=config,
            manager=self.manager,
            registry=self.registry,
        )
        self.maintenance = MaintenanceLoop(
            config=config,
            manager=self.manager,
            registry=self.registry,
            dispatchers=self.dispatchers,
        )
        self.servicer = MasterServicer(
            config=config,
            manager=self.manager,
            registry=self.registry,
            dispatchers=self.dispatchers,
        )
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=config.max_workers),
            maximum_concurrent_rpcs=config.max_workers,
        )
        rpc.add_MasterServiceServicer_to_server(self.servicer, self._server)
        self.port = self._server.add_insecure_port(f"{config.host}:{config.port}")
        if self.port == 0:
            raise RuntimeError(f"could not bind master server on {config.host}:{config.port}")
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._server.start()
            self.maintenance.start()
            self._started = True
        self.logger.info(
            "master server started",
            extra={"address": f"{self.config.host}:{self.port}"},
        )

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._started = False
        self.maintenance.stop()
        self.dispatchers.stop_all()
        self._server.stop(self.config.shutdown_grace).wait()
        self.logger.info("master server stopped")

    def wait_for_termination(self) -> None:
        self._server.wait_for_termination()

    def stub(self) -> rpc.MasterServiceStub:
        """Create a client stub for this running master (useful in tests)."""
        return rpc.MasterServiceStub(grpc.insecure_channel(f"127.0.0.1:{self.port}"))
