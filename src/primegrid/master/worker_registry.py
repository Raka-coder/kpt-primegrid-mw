"""Thread-safe worker liveness registry, isolated from job locks."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, replace

from primegrid import primegrid_pb2 as pb
from primegrid.common.config import MasterConfig


@dataclass
class WorkerRecord:
    worker_id: str
    address: str
    cores: int
    status: pb.WorkerStatus
    last_heartbeat: float
    chunks_done: int = 0
    compute_ms: int = 0


@dataclass(frozen=True)
class Registration:
    record: WorkerRecord
    created: bool
    revived: bool


@dataclass(frozen=True)
class HeartbeatResult:
    found: bool
    revived: bool


class WorkerRegistry:
    """Registry protected by one short-lived lock; never held during RPC."""

    def __init__(self, config: MasterConfig) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._workers: dict[str, WorkerRecord] = {}

    def register(self, worker_id: str, address: str, cores: int) -> Registration:
        if not worker_id.strip():
            raise ValueError("worker_id must not be empty")
        if not address.strip():
            raise ValueError("worker address must not be empty")
        if cores <= 0:
            raise ValueError("cores must be positive")

        now = time.monotonic()
        with self._lock:
            existing = self._workers.get(worker_id)
            if existing is None:
                record = WorkerRecord(
                    worker_id=worker_id,
                    address=address,
                    cores=cores,
                    status=pb.WORKER_STATUS_ALIVE,
                    last_heartbeat=now,
                )
                self._workers[worker_id] = record
                return Registration(replace(record), created=True, revived=False)

            revived = existing.status is pb.WORKER_STATUS_DEAD
            existing.address = address
            existing.cores = cores
            existing.status = pb.WORKER_STATUS_ALIVE
            existing.last_heartbeat = now
            return Registration(replace(existing), created=False, revived=revived)

    def heartbeat(self, worker_id: str) -> HeartbeatResult:
        now = time.monotonic()
        with self._lock:
            record = self._workers.get(worker_id)
            if record is None:
                return HeartbeatResult(found=False, revived=False)
            revived = record.status is pb.WORKER_STATUS_DEAD
            record.status = pb.WORKER_STATUS_ALIVE
            record.last_heartbeat = now
            return HeartbeatResult(found=True, revived=revived)

    def mark_dead(self, worker_id: str) -> bool:
        with self._lock:
            record = self._workers.get(worker_id)
            if record is None or record.status is pb.WORKER_STATUS_DEAD:
                return False
            record.status = pb.WORKER_STATUS_DEAD
            return True

    def mark_stale(self, dead_after: float) -> list[str]:
        now = time.monotonic()
        dead: list[str] = []
        with self._lock:
            for record in self._workers.values():
                if (
                    record.status is pb.WORKER_STATUS_ALIVE
                    and now - record.last_heartbeat > dead_after
                ):
                    record.status = pb.WORKER_STATUS_DEAD
                    dead.append(record.worker_id)
        return sorted(dead)

    def get(self, worker_id: str) -> WorkerRecord | None:
        with self._lock:
            record = self._workers.get(worker_id)
            return replace(record) if record is not None else None

    def address_of(self, worker_id: str) -> str | None:
        with self._lock:
            record = self._workers.get(worker_id)
            if record is None or record.status is not pb.WORKER_STATUS_ALIVE:
                return None
            return record.address

    def is_alive(self, worker_id: str) -> bool:
        with self._lock:
            record = self._workers.get(worker_id)
            return record is not None and record.status is pb.WORKER_STATUS_ALIVE

    def has_alive_workers(self) -> bool:
        with self._lock:
            return any(
                record.status is pb.WORKER_STATUS_ALIVE
                for record in self._workers.values()
            )

    def record_chunk(self, worker_id: str, compute_ms: int) -> None:
        with self._lock:
            record = self._workers.get(worker_id)
            if record is not None:
                record.chunks_done += 1
                record.compute_ms += max(0, compute_ms)

    def list_workers(self) -> list[pb.WorkerInfo]:
        now = time.monotonic()
        with self._lock:
            records = sorted(self._workers.values(), key=lambda item: item.worker_id)
            return [
                pb.WorkerInfo(
                    worker_id=record.worker_id,
                    address=record.address,
                    cores=record.cores,
                    status=record.status,
                    last_heartbeat_ms=max(
                        0, int((now - record.last_heartbeat) * 1000)
                    ),
                    chunks_done=record.chunks_done,
                    compute_ms=record.compute_ms,
                )
                for record in records
            ]

    def _force_heartbeat_age(self, worker_id: str, age_seconds: float) -> None:
        with self._lock:
            record = self._workers[worker_id]
            record.last_heartbeat = time.monotonic() - age_seconds
