"""Background worker-liveness, dedup TTL, and stalled-job maintenance."""

from __future__ import annotations

import threading
import time

from primegrid.common.config import MasterConfig
from primegrid.common.logging import get_logger
from primegrid.master.dispatcher import DispatcherManager
from primegrid.master.job_manager import JobManager
from primegrid.master.worker_registry import WorkerRegistry


class MaintenanceLoop:
    """One periodic loop that never holds a registry or job lock during I/O."""

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
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._no_worker_since: float | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="master-maintenance",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        while not self._stop.wait(self.config.health_interval):
            try:
                self._tick()
            except Exception:  # pragma: no cover - maintenance must survive
                self.logger.exception("maintenance tick failed")

    def _tick(self) -> None:
        stale = self.registry.mark_stale(self.config.worker_dead_after)
        for worker_id in stale:
            self.dispatchers.handle_dead(worker_id, "heartbeat timeout")

        removed = self.manager.cleanup_dedup()
        if removed:
            self.logger.debug("expired request_id entries", extra={"count": removed})

        if self.registry.has_alive_workers():
            self._no_worker_since = None
            return
        if self._no_worker_since is None:
            self._no_worker_since = time.monotonic()
        failed = self.manager.fail_jobs_without_workers(self._no_worker_since)
        for job_id in failed:
            self.logger.error(
                "job failed without live workers",
                extra={"job_id": job_id},
            )
