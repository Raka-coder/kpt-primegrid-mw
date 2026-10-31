"""Job manager tests cover idempotency, locking, requeue, and aggregation."""

from __future__ import annotations

import queue
import threading

from primegrid import primegrid_pb2 as pb
from primegrid.common.config import MasterConfig
from primegrid.master.job_manager import JobManager


def make_manager(**overrides: object) -> JobManager:
    values = {
        "host": "127.0.0.1",
        "port": 0,
        "worker_dead_after": 3.0,
        "heartbeat_interval": 1.0,
        "default_chunk_size": 10,
        "max_range": 1_000_000,
    }
    values.update(overrides)
    return JobManager(MasterConfig(**values))  # type: ignore[arg-type]


def test_submit_is_deduplicated_by_request_id() -> None:
    manager = make_manager()

    first, first_duplicate = manager.submit("req-1", 1, 100, 10, "sieve")
    second, second_duplicate = manager.submit("req-1", 1, 100, 10, "sieve")

    assert first_duplicate is False
    assert second_duplicate is True
    assert first.job_id == second.job_id
    assert len(manager.jobs) == 1


def test_concurrent_duplicate_submits_create_one_job() -> None:
    manager = make_manager()
    barrier = threading.Barrier(12)
    results: queue.Queue[str] = queue.Queue()

    def submit() -> None:
        barrier.wait()
        job, _ = manager.submit("same-request", 1, 100, 10, "sieve")
        results.put(job.job_id)

    threads = [threading.Thread(target=submit) for _ in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    job_ids = {results.get_nowait() for _ in range(12)}
    assert len(job_ids) == 1
    assert len(manager.jobs) == 1


def test_aggregation_accepts_each_chunk_once() -> None:
    manager = make_manager()
    job, _ = manager.submit("req", 1, 10, 10, "sieve")
    assert job.status == pb.JOB_STATUS_PENDING

    task = manager.next_task(timeout=0.1)
    assert task is not None
    assignment = manager.assign(task, "worker-1")
    assert assignment is not None
    assert job.status == pb.JOB_STATUS_RUNNING

    accepted = manager.accept_result(assignment, prime_count=4, max_prime=7, compute_ms=12)
    duplicate = manager.accept_result(assignment, prime_count=999, max_prime=999, compute_ms=1)

    assert accepted is True
    assert duplicate is False
    assert job.prime_count == 4
    assert job.max_prime == 7
    assert job.status == pb.JOB_STATUS_COMPLETED
    assert job.per_worker_stats["worker-1"].chunks_done == 1
    assert job.per_worker_stats["worker-1"].compute_ms == 12


def test_failed_chunk_is_requeued_then_fails_after_attempt_limit() -> None:
    manager = make_manager(chunk_attempt_limit=2)
    job, _ = manager.submit("req", 1, 10, 10, "sieve")

    for attempt in range(2):
        task = manager.next_task(timeout=0.1)
        assert task is not None
        assignment = manager.assign(task, "worker-1")
        assert assignment is not None
        decision = manager.release_failure(assignment, "synthetic failure")
        if attempt == 0:
            assert decision == "requeue"
            manager.enqueue_task(assignment.job_id, assignment.chunk_id)
        else:
            assert decision == "failed"

    assert job.status == pb.JOB_STATUS_FAILED
    assert "synthetic failure" in job.error


def test_requeued_chunk_cannot_be_completed_by_stale_lease() -> None:
    manager = make_manager()
    job, _ = manager.submit("req", 1, 10, 10, "sieve")
    task = manager.next_task(timeout=0.1)
    assert task is not None

    old_assignment = manager.assign(task, "worker-1")
    assert old_assignment is not None
    assert manager.requeue_worker("worker-1") == 1

    replacement_task = manager.next_task(timeout=0.1)
    assert replacement_task is not None
    new_assignment = manager.assign(replacement_task, "worker-2")
    assert new_assignment is not None

    assert manager.release_failure(old_assignment, "stale failure") == "ignored"
    assert manager.accept_result(new_assignment, prime_count=4, max_prime=7, compute_ms=5)
    assert job.status == pb.JOB_STATUS_COMPLETED
    assert job.prime_count == 4


def test_cancel_is_idempotent_and_blocks_late_results() -> None:
    manager = make_manager()
    job, _ = manager.submit("req", 1, 10, 10, "sieve")
    task = manager.next_task(timeout=0.1)
    assert task is not None
    assignment = manager.assign(task, "worker-1")

    assert job.cancel() is True
    assert job.cancel() is False
    assert job.status == pb.JOB_STATUS_CANCELLED
    assert assignment is not None
    assert manager.accept_result(assignment, 4, 7, 1) is False


def test_invalid_mode_and_range_are_rejected() -> None:
    manager = make_manager()

    for mode, start, end in [("bogus", 1, 10), ("sieve", 10, 1)]:
        try:
            manager.submit("req", start, end, 10, mode)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid submit was accepted")
