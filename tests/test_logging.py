"""Structured logs must expose the fields required for demo evidence."""

import io
import logging

from primegrid.common.logging import configure_logging, get_logger


def test_log_line_contains_structured_context() -> None:
    stream = io.StringIO()
    configure_logging(level="INFO", stream=stream, force=True)
    logger = get_logger("master", job_id="job-1")

    logger.info(
        "chunk completed",
        extra={"chunk_id": 4, "worker_id": "w-1", "duration_ms": 12.5},
    )
    output = stream.getvalue()

    assert "component=master" in output
    assert "job_id=job-1" in output
    assert "chunk_id=4" in output
    assert "worker_id=w-1" in output
    assert "duration_ms=12.5" in output
    assert "message=chunk completed" in output
    assert "\n" in output
    logging.getLogger().handlers.clear()
