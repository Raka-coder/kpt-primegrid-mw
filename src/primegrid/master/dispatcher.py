"""Dynamic one-thread-per-worker chunk dispatcher."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import grpc

from primegrid import primegrid_pb2 as pb
from primegrid import primegrid_pb2_grpc as rpc
from primegrid.common.config import MasterConfig
from primegrid.common.logging import get_logger
from primegrid.common.retry import RETRYABLE_CODES, call_with_retry
from primegrid.master.job_manager import Assignment, JobManager
from primegrid.master.worker_registry import WorkerRegistry


class Dispatcher:
    """Continuously pulls chunks and sends them to exactly one worker."""

    def __init__(
        self,
        *,
        config: MasterConfig,
        manager: JobManager,
        registry: WorkerRegistry,
        worker_id: str,
        address: str,
        on_worker_failure: Callable[[str, str], None],
    ) -> None:
        self.config = config
        self.manager = manager
        self.registry = registry
        self.worker_id = worker_id
        self.address = address
        self.on_worker_failure = on_worker_failure
        self.logger = get_logger("master", worker_id=worker_id)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name=f"dispatcher-{self.worker_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, join: bool = True) -> None:
        self._stop.set()
        if join and self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)

    @property
    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        channel = grpc.insecure_channel(self.address)
        stub = rpc.WorkerServiceStub(channel)
        try:
            while not self._stop.is_set():
                if not self.registry.is_alive(self.worker_id):
                    break
                task = self.manager.next_task(timeout=0.2)
                if task is None:
                    continue
                if not self.registry.is_alive(self.worker_id):
                    self.manager.enqueue_task(task.job_id, task.chunk_id)
                    break

                assignment = self.manager.assign(task, self.worker_id)
                if assignment is None:
                    continue
                self._process_assignment(stub, assignment)
        finally:
            channel.close()

    def _process_assignment(
        self,
        stub: rpc.WorkerServiceStub,
        assignment: Assignment,
    ) -> None:
        request = pb.ProcessChunkRequest(
            job_id=assignment.job_id,
            chunk_id=assignment.chunk_id,
            start=assignment.start,
            end=assignment.end,
            mode=(
                pb.ALGORITHM_MODE_SIEVE if assignment.mode == "sieve" else pb.ALGORITHM_MODE_TRIAL
            ),
        )
        self.logger.info(
            "chunk assigned",
            extra={
                "job_id": assignment.job_id,
                "chunk_id": assignment.chunk_id,
                "worker_id": self.worker_id,
                "attempt": assignment.attempts,
            },
        )
        started = time.monotonic()
        try:
            response = call_with_retry(
                stub.ProcessChunk,
                request,
                timeout=self.config.process_deadline,
                policy=self.config.process_retry_policy,
                logger=self.logger,
                rpc_name="ProcessChunk",
                job_id=assignment.job_id,
                chunk_id=assignment.chunk_id,
                worker_id=self.worker_id,
            )
        except grpc.RpcError as exc:
            code = exc.code()
            reason = f"{code.name if code else 'UNKNOWN'}: {exc.details()}"
            self._release_failure(assignment, reason)
            if code in RETRYABLE_CODES:
                self.on_worker_failure(self.worker_id, reason)
            return
        except Exception as exc:  # pragma: no cover - defensive boundary
            self.logger.exception(
                "unexpected dispatch error",
                extra={
                    "job_id": assignment.job_id,
                    "chunk_id": assignment.chunk_id,
                    "worker_id": self.worker_id,
                },
            )
            self._release_failure(assignment, f"unexpected error: {exc}")
            return

        if not response.success:
            self._release_failure(
                assignment,
                response.error or "worker reported an unsuccessful chunk",
            )
            return
        if response.job_id != assignment.job_id or response.chunk_id != assignment.chunk_id:
            self._release_failure(assignment, "worker response identifiers mismatch")
            return

        accepted = self.manager.accept_result(
            assignment,
            response.prime_count,
            response.max_prime,
            response.compute_ms,
        )
        duration_ms = max(0, int((time.monotonic() - started) * 1000))
        if accepted:
            self.registry.record_chunk(self.worker_id, response.compute_ms)
            self.logger.info(
                "chunk completed",
                extra={
                    "job_id": assignment.job_id,
                    "chunk_id": assignment.chunk_id,
                    "worker_id": self.worker_id,
                    "duration_ms": duration_ms,
                },
            )
        else:
            self.logger.info(
                "duplicate or stale chunk result ignored",
                extra={
                    "job_id": assignment.job_id,
                    "chunk_id": assignment.chunk_id,
                    "worker_id": self.worker_id,
                    "duration_ms": duration_ms,
                },
            )

    def _release_failure(self, assignment: Assignment, reason: str) -> None:
        decision = self.manager.release_failure(assignment, reason)
        self.logger.warning(
            "chunk attempt failed",
            extra={
                "job_id": assignment.job_id,
                "chunk_id": assignment.chunk_id,
                "worker_id": self.worker_id,
                "decision": decision,
                "reason": reason,
            },
        )
        if decision == "requeue":
            self.manager.enqueue_task(assignment.job_id, assignment.chunk_id)


class DispatcherManager:
    """Creates, replaces, and stops dispatchers as worker liveness changes."""

    def __init__(
        self,
        *,
        config: MasterConfig,
        manager: JobManager,
        registry: WorkerRegistry,
    ) -> None:
        self.config = config
        self.manager = manager
        self.registry = registry
        self.logger = get_logger("master")
        self._lock = threading.Lock()
        self._dispatchers: dict[str, Dispatcher] = {}

    def ensure(self, worker_id: str, address: str) -> None:
        replaced: Dispatcher | None = None
        with self._lock:
            existing = self._dispatchers.get(worker_id)
            if existing is not None and existing.is_alive and existing.address == address:
                return
            if existing is not None:
                replaced = self._dispatchers.pop(worker_id, None)
            dispatcher = Dispatcher(
                config=self.config,
                manager=self.manager,
                registry=self.registry,
                worker_id=worker_id,
                address=address,
                on_worker_failure=self.handle_dead,
            )
            self._dispatchers[worker_id] = dispatcher
        if replaced is not None:
            replaced.stop(join=False)
        dispatcher.start()

    def handle_dead(self, worker_id: str, reason: str) -> None:
        self.registry.mark_dead(worker_id)
        requeued = self.manager.requeue_worker(worker_id)
        self.logger.warning(
            "worker marked dead",
            extra={
                "worker_id": worker_id,
                "requeued_chunks": requeued,
                "reason": reason,
            },
        )
        self.stop_worker(worker_id)

    def stop_worker(self, worker_id: str) -> None:
        with self._lock:
            dispatcher = self._dispatchers.pop(worker_id, None)
        if dispatcher is not None:
            dispatcher.stop(join=dispatcher._thread is not threading.current_thread())

    def stop_all(self) -> None:
        with self._lock:
            dispatchers = list(self._dispatchers.values())
            self._dispatchers.clear()
        for dispatcher in dispatchers:
            dispatcher.stop()
