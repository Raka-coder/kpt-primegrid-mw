"""Thread-safe job registry, chunk queue, idempotency, and aggregation."""

from __future__ import annotations

import queue
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from primegrid import primegrid_pb2 as pb
from primegrid.common.config import MasterConfig
from primegrid.common.logging import get_logger


class ChunkState(str, Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    DONE = "DONE"


@dataclass
class Chunk:
    chunk_id: int
    start: int
    end: int
    state: ChunkState = ChunkState.PENDING
    attempts: int = 0
    assigned_to: str | None = None
    lease: int = 0


@dataclass
class WorkerContribution:
    chunks_done: int = 0
    compute_ms: int = 0


@dataclass(frozen=True)
class ChunkTask:
    job_id: str
    chunk_id: int


@dataclass(frozen=True)
class Assignment:
    job_id: str
    chunk_id: int
    start: int
    end: int
    mode: str
    worker_id: str
    lease: int
    attempts: int


@dataclass(frozen=True)
class DedupEntry:
    job_id: str
    created_at: float


def split_range(start: int, end: int, chunk_size: int) -> list[tuple[int, int]]:
    """Split inclusive [start, end] into half-open chunks with no gaps."""
    if start < 0:
        raise ValueError("start must be non-negative")
    if end < start:
        raise ValueError("end must be greater than or equal to start")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    chunks: list[tuple[int, int]] = []
    cursor = start
    exclusive_end = end + 1
    while cursor < exclusive_end:
        stop = min(cursor + chunk_size, exclusive_end)
        chunks.append((cursor, stop))
        cursor = stop
    return chunks


class Job:
    """One job's state. All mutations are guarded by that job's own lock."""

    def __init__(
        self,
        *,
        job_id: str,
        request_id: str,
        start: int,
        end: int,
        mode: str,
        chunk_size: int,
        ranges: list[tuple[int, int]],
        chunk_attempt_limit: int,
    ) -> None:
        self.job_id = job_id
        self.request_id = request_id
        self.start = start
        self.end = end
        self.mode = mode
        self.chunk_size = chunk_size
        self.chunks = [Chunk(chunk_id=index, start=a, end=b) for index, (a, b) in enumerate(ranges)]
        self._chunk_attempt_limit = chunk_attempt_limit
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._version = 0
        self.status = pb.JOB_STATUS_PENDING
        self.prime_count = 0
        self.max_prime = 0
        self.done_set: set[int] = set()
        self.per_worker_stats: dict[str, WorkerContribution] = {}
        self.created_at = time.monotonic()
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.error = ""

    @property
    def total_chunks(self) -> int:
        return len(self.chunks)

    @property
    def done_chunks(self) -> int:
        with self._lock:
            return len(self.done_set)

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    @property
    def condition(self) -> threading.Condition:
        return self._condition

    @property
    def lock(self) -> threading.Lock:
        return self._lock

    def is_terminal(self) -> bool:
        with self._lock:
            return self.status in (
                pb.JOB_STATUS_COMPLETED,
                pb.JOB_STATUS_FAILED,
                pb.JOB_STATUS_CANCELLED,
            )

    def _bump_locked(self) -> None:
        self._version += 1
        self._condition.notify_all()

    def assign(self, chunk_id: int, worker_id: str) -> Assignment | None:
        """Lease a pending chunk to a worker."""
        with self._lock:
            if self.status in (
                pb.JOB_STATUS_FAILED,
                pb.JOB_STATUS_CANCELLED,
                pb.JOB_STATUS_COMPLETED,
            ):
                return None
            if chunk_id < 0 or chunk_id >= len(self.chunks):
                return None
            chunk = self.chunks[chunk_id]
            if chunk.state is not ChunkState.PENDING:
                return None

            chunk.state = ChunkState.ASSIGNED
            chunk.assigned_to = worker_id
            chunk.attempts += 1
            chunk.lease += 1
            if self.status is pb.JOB_STATUS_PENDING:
                self.status = pb.JOB_STATUS_RUNNING
                self.started_at = time.monotonic()
            assignment = Assignment(
                job_id=self.job_id,
                chunk_id=chunk.chunk_id,
                start=chunk.start,
                end=chunk.end,
                mode=self.mode,
                worker_id=worker_id,
                lease=chunk.lease,
                attempts=chunk.attempts,
            )
            self._bump_locked()
            return assignment

    def accept_result(
        self,
        assignment: Assignment,
        prime_count: int,
        max_prime: int,
        compute_ms: int,
    ) -> bool:
        """Aggregate a chunk result exactly once, even if retries deliver duplicates."""
        with self._lock:
            if self.status not in (pb.JOB_STATUS_PENDING, pb.JOB_STATUS_RUNNING):
                return False
            if assignment.chunk_id < 0 or assignment.chunk_id >= len(self.chunks):
                return False
            chunk = self.chunks[assignment.chunk_id]
            if chunk.state is ChunkState.DONE or chunk.chunk_id in self.done_set:
                return False

            chunk.state = ChunkState.DONE
            chunk.assigned_to = assignment.worker_id
            self.done_set.add(chunk.chunk_id)
            self.prime_count += max(0, prime_count)
            self.max_prime = max(self.max_prime, max_prime)
            contribution = self.per_worker_stats.setdefault(
                assignment.worker_id, WorkerContribution()
            )
            contribution.chunks_done += 1
            contribution.compute_ms += max(0, compute_ms)

            if len(self.done_set) == len(self.chunks):
                self.status = pb.JOB_STATUS_COMPLETED
                self.finished_at = time.monotonic()
            self._bump_locked()
            return True

    def release_failure(self, assignment: Assignment, reason: str) -> str:
        """Release a failed lease. Returns requeue, failed, or ignored."""
        with self._lock:
            if self.status not in (pb.JOB_STATUS_PENDING, pb.JOB_STATUS_RUNNING):
                return "ignored"
            if assignment.chunk_id < 0 or assignment.chunk_id >= len(self.chunks):
                return "ignored"
            chunk = self.chunks[assignment.chunk_id]
            if chunk.state is ChunkState.DONE:
                return "ignored"
            if (
                chunk.state is not ChunkState.ASSIGNED
                or chunk.assigned_to != assignment.worker_id
                or chunk.lease != assignment.lease
            ):
                return "ignored"

            if chunk.attempts >= self._chunk_attempt_limit:
                self.status = pb.JOB_STATUS_FAILED
                self.error = (
                    f"chunk {chunk.chunk_id} failed after {chunk.attempts} attempts: {reason}"
                )
                self.finished_at = time.monotonic()
                self._bump_locked()
                return "failed"

            chunk.state = ChunkState.PENDING
            chunk.assigned_to = None
            self._bump_locked()
            return "requeue"

    def requeue_worker(self, worker_id: str) -> list[int]:
        """Return every active chunk leased to a dead worker to the queue."""
        released: list[int] = []
        with self._lock:
            if self.status not in (pb.JOB_STATUS_PENDING, pb.JOB_STATUS_RUNNING):
                return released
            for chunk in self.chunks:
                if chunk.state is ChunkState.ASSIGNED and chunk.assigned_to == worker_id:
                    chunk.state = ChunkState.PENDING
                    chunk.assigned_to = None
                    released.append(chunk.chunk_id)
            if released:
                self._bump_locked()
        return released

    def cancel(self) -> bool:
        with self._lock:
            if self.status in (
                pb.JOB_STATUS_COMPLETED,
                pb.JOB_STATUS_FAILED,
                pb.JOB_STATUS_CANCELLED,
            ):
                return False
            self.status = pb.JOB_STATUS_CANCELLED
            self.finished_at = time.monotonic()
            self._bump_locked()
            return True

    def fail(self, error: str) -> bool:
        with self._lock:
            if self.status in (
                pb.JOB_STATUS_COMPLETED,
                pb.JOB_STATUS_FAILED,
                pb.JOB_STATUS_CANCELLED,
            ):
                return False
            self.status = pb.JOB_STATUS_FAILED
            self.error = error
            self.finished_at = time.monotonic()
            self._bump_locked()
            return True

    def _elapsed_ms_locked(self, now: float) -> int:
        if self.started_at is None:
            return 0
        stop = self.finished_at if self.finished_at is not None else now
        return max(0, int((stop - self.started_at) * 1000))

    def snapshot(self) -> tuple[pb.JobProgress, int]:
        """Return an immutable progress snapshot and the version it represents."""
        with self._lock:
            total = len(self.chunks)
            done = len(self.done_set)
            progress = pb.JobProgress(
                job_id=self.job_id,
                status=self.status,
                total_chunks=total,
                done_chunks=done,
                prime_count=self.prime_count,
                max_prime=self.max_prime,
                progress=(done / total) if total else 1.0,
                elapsed_ms=self._elapsed_ms_locked(time.monotonic()),
                error=self.error,
            )
            return progress, self._version

    def result(self) -> pb.GetResultResponse:
        with self._lock:
            stats = [
                pb.WorkerStat(
                    worker_id=worker_id,
                    chunks_done=contribution.chunks_done,
                    compute_ms=contribution.compute_ms,
                )
                for worker_id, contribution in sorted(self.per_worker_stats.items())
            ]
            return pb.GetResultResponse(
                job_id=self.job_id,
                status=self.status,
                total_chunks=len(self.chunks),
                done_chunks=len(self.done_set),
                prime_count=self.prime_count,
                max_prime=self.max_prime,
                elapsed_ms=self._elapsed_ms_locked(time.monotonic()),
                per_worker_stats=stats,
                error=self.error,
            )

    def wait_for_change(self, version: int, timeout: float) -> int:
        with self._condition:
            if self._version == version:
                self._condition.wait(timeout)
            return self._version


class JobManager:
    """Owns the job registry, deduplication table, and shared chunk queue."""

    def __init__(self, config: MasterConfig) -> None:
        self.config = config
        self.logger = get_logger("master")
        self._jobs_lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self._dedup_lock = threading.Lock()
        self._dedup: dict[str, DedupEntry] = {}
        self._queue: queue.Queue[ChunkTask] = queue.Queue()

    @property
    def jobs(self) -> dict[str, Job]:
        with self._jobs_lock:
            return dict(self._jobs)

    def submit(
        self,
        request_id: str,
        start: int,
        end: int,
        chunk_size: int,
        mode: str,
    ) -> tuple[Job, bool]:
        """Create a job or return the existing job for this request_id."""
        request_key = request_id.strip()
        if not request_key:
            raise ValueError("request_id must not be empty")
        normalized_mode = mode.strip().lower()
        if normalized_mode not in ("sieve", "trial"):
            raise ValueError(f"invalid mode: {mode!r}")
        if end - start + 1 > self.config.max_range:
            raise ValueError(f"range exceeds configured maximum of {self.config.max_range}")
        effective_chunk_size = chunk_size or self.config.default_chunk_size
        ranges = split_range(start, end, effective_chunk_size)
        if len(ranges) > self.config.max_chunks:
            raise ValueError(
                f"request creates {len(ranges)} chunks; maximum is {self.config.max_chunks}"
            )

        now = time.monotonic()
        with self._dedup_lock:
            existing = self._dedup.get(request_key)
            if existing is not None:
                if now - existing.created_at <= self.config.dedup_ttl:
                    with self._jobs_lock:
                        job = self._jobs.get(existing.job_id)
                    if job is not None:
                        self.logger.info(
                            "duplicate submit detected",
                            extra={"job_id": job.job_id},
                        )
                        return job, True
                del self._dedup[request_key]

            job = Job(
                job_id=str(uuid.uuid4()),
                request_id=request_key,
                start=start,
                end=end,
                mode=normalized_mode,
                chunk_size=effective_chunk_size,
                ranges=ranges,
                chunk_attempt_limit=self.config.chunk_attempt_limit,
            )
            with self._jobs_lock:
                self._jobs[job.job_id] = job
            self._dedup[request_key] = DedupEntry(job.job_id, now)
            tasks = [ChunkTask(job.job_id, chunk.chunk_id) for chunk in job.chunks]

        for task in tasks:
            self._queue.put_nowait(task)
        self.logger.info(
            "job created",
            extra={
                "job_id": job.job_id,
                "start": start,
                "end": end,
                "mode": normalized_mode,
                "total_chunks": len(tasks),
            },
        )
        return job, False

    def get(self, job_id: str) -> Job | None:
        with self._jobs_lock:
            return self._jobs.get(job_id)

    def next_task(self, timeout: float = 0.5) -> ChunkTask | None:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def enqueue_task(self, job_id: str, chunk_id: int) -> None:
        self._queue.put_nowait(ChunkTask(job_id, chunk_id))

    def assign(self, task: ChunkTask, worker_id: str) -> Assignment | None:
        job = self.get(task.job_id)
        if job is None:
            return None
        return job.assign(task.chunk_id, worker_id)

    def accept_result(
        self,
        assignment: Assignment,
        prime_count: int,
        max_prime: int,
        compute_ms: int,
    ) -> bool:
        job = self.get(assignment.job_id)
        if job is None:
            return False
        return job.accept_result(assignment, prime_count, max_prime, compute_ms)

    def release_failure(self, assignment: Assignment, reason: str) -> str:
        job = self.get(assignment.job_id)
        if job is None:
            return "ignored"
        return job.release_failure(assignment, reason)

    def requeue_worker(self, worker_id: str) -> int:
        """Requeue all chunks leased by a worker, returning the number moved."""
        tasks: list[ChunkTask] = []
        with self._jobs_lock:
            jobs = list(self._jobs.values())
        for job in jobs:
            for chunk_id in job.requeue_worker(worker_id):
                tasks.append(ChunkTask(job.job_id, chunk_id))
        for task in tasks:
            self._queue.put_nowait(task)
        return len(tasks)

    def cancel(self, job_id: str) -> tuple[bool, Job | None]:
        job = self.get(job_id)
        if job is None:
            return False, None
        return job.cancel(), job

    def cleanup_dedup(self, now: float | None = None) -> int:
        current = time.monotonic() if now is None else now
        removed = 0
        with self._dedup_lock:
            for request_id, entry in list(self._dedup.items()):
                if current - entry.created_at > self.config.dedup_ttl:
                    del self._dedup[request_id]
                    removed += 1
        return removed

    def fail_jobs_without_workers(self, no_worker_since: float | None) -> list[str]:
        """Fail long-pending jobs when no worker has been alive for too long."""
        if no_worker_since is None:
            return []
        if time.monotonic() - no_worker_since < self.config.job_stall_timeout:
            return []
        failed: list[str] = []
        for job in self.jobs_from_snapshot():
            if job.fail("no live workers available before the job stall timeout"):
                failed.append(job.job_id)
        return failed

    def jobs_from_snapshot(self) -> Iterable[Job]:
        with self._jobs_lock:
            return list(self._jobs.values())
