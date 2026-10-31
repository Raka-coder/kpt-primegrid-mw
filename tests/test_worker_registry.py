"""Worker registry tracks liveness and supports upsert/revival."""

from primegrid import primegrid_pb2 as pb
from primegrid.common.config import MasterConfig
from primegrid.master.worker_registry import WorkerRegistry


def make_registry() -> WorkerRegistry:
    return WorkerRegistry(MasterConfig(heartbeat_interval=1.0, worker_dead_after=2.0))


def test_register_upserts_by_worker_id() -> None:
    registry = make_registry()

    first = registry.register("w1", "host-a:50061", 2)
    second = registry.register("w1", "host-b:60061", 4)

    assert first.created is True
    assert second.created is False
    workers = registry.list_workers()
    assert len(workers) == 1
    assert workers[0].address == "host-b:60061"
    assert workers[0].cores == 4
    assert workers[0].status == pb.WORKER_STATUS_ALIVE


def test_stale_worker_is_marked_dead_and_revived_by_heartbeat() -> None:
    registry = make_registry()
    registry.register("w1", "host:1", 1)
    registry._force_heartbeat_age("w1", 10.0)

    dead = registry.mark_stale(2.0)
    assert dead == ["w1"]
    assert registry.list_workers()[0].status == pb.WORKER_STATUS_DEAD

    result = registry.heartbeat("w1")
    assert result.found is True
    assert result.revived is True
    assert registry.list_workers()[0].status == pb.WORKER_STATUS_ALIVE


def test_unknown_heartbeat_is_not_found() -> None:
    result = make_registry().heartbeat("missing")

    assert result.found is False
    assert result.revived is False


def test_worker_chunk_stats_accumulate() -> None:
    registry = make_registry()
    registry.register("w1", "host:1", 1)

    registry.record_chunk("w1", 15)
    registry.record_chunk("w1", 20)

    worker = registry.list_workers()[0]
    assert worker.chunks_done == 2
    assert worker.compute_ms == 35
