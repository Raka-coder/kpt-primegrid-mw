"""Configuration is environment-driven and fails loudly on invalid values."""

import pytest

from primegrid.common.config import (
    ClientConfig,
    MasterConfig,
    WorkerConfig,
    load_client_config,
    load_master_config,
    load_worker_config,
)


def test_master_config_uses_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRIMEGRID_MASTER_HOST", "127.0.0.9")
    monkeypatch.setenv("PRIMEGRID_MASTER_PORT", "51000")
    monkeypatch.setenv("PRIMEGRID_DEFAULT_CHUNK_SIZE", "2500")
    monkeypatch.setenv("PRIMEGRID_PROCESS_DEADLINE", "12.5")

    config = load_master_config()

    assert isinstance(config, MasterConfig)
    assert config.host == "127.0.0.9"
    assert config.port == 51000
    assert config.default_chunk_size == 2500
    assert config.process_deadline == 12.5


def test_worker_config_builds_master_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRIMEGRID_MASTER_ADDRESS", "10.20.30.40:6000")
    monkeypatch.delenv("PRIMEGRID_WORKER_ID", raising=False)

    config = load_worker_config()

    assert isinstance(config, WorkerConfig)
    assert config.master_address == "10.20.30.40:6000"
    assert config.worker_id.startswith("worker-")
    assert config.heartbeat_interval > 0


def test_cli_overrides_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRIMEGRID_MASTER_ADDRESS", "env-host:50050")

    config = load_client_config(master_address="cli-host:7777")

    assert isinstance(config, ClientConfig)
    assert config.master_address == "cli-host:7777"


def test_invalid_integer_environment_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRIMEGRID_MASTER_PORT", "not-a-port")

    with pytest.raises(ValueError, match="PRIMEGRID_MASTER_PORT"):
        load_master_config()


def test_worker_dead_threshold_must_exceed_heartbeat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRIMEGRID_HEARTBEAT_INTERVAL", "5")
    monkeypatch.setenv("PRIMEGRID_WORKER_DEAD_AFTER", "5")

    with pytest.raises(ValueError, match="WORKER_DEAD_AFTER"):
        load_master_config()
