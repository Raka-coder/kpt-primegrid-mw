"""One-line structured logging shared by master, workers, and clients."""

from __future__ import annotations

import logging
import sys
from typing import Any, TextIO

_CONTEXT_FIELDS = ("job_id", "chunk_id", "worker_id", "duration_ms")
_FORMAT = (
    "%(asctime)s level=%(levelname)s component=%(component)s "
    "job_id=%(job_id)s chunk_id=%(chunk_id)s worker_id=%(worker_id)s "
    "duration_ms=%(duration_ms)s message=%(message)s"
)


class _StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "component"):
            record.component = "app"
        for field in _CONTEXT_FIELDS:
            if not hasattr(record, field):
                setattr(record, field, "-")
        return super().format(record)


class ContextAdapter(logging.LoggerAdapter):
    """LoggerAdapter that binds component and per-call context fields."""

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        merged = dict(self.extra or {})
        merged.update(kwargs.get("extra") or {})
        kwargs["extra"] = merged
        return msg, kwargs


def configure_logging(
    level: str = "INFO",
    *,
    stream: TextIO | None = None,
    force: bool = False,
) -> None:
    """Configure the root logger once with a compact key=value format."""
    root = logging.getLogger()
    if force:
        for handler in list(root.handlers):
            root.removeHandler(handler)
    already_configured = any(
        getattr(handler, "_primegrid_structured", False) for handler in root.handlers
    )
    if not already_configured:
        handler = logging.StreamHandler(stream or sys.stderr)
        handler._primegrid_structured = True  # type: ignore[attr-defined]
        handler.setFormatter(_StructuredFormatter(_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S%z"))
        root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def get_logger(component: str, **context: Any) -> ContextAdapter:
    """Return a logger bound to a component name and optional context."""
    logger = logging.getLogger(f"primegrid.{component}")
    return ContextAdapter(logger, {"component": component, **context})
