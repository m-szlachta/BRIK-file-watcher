from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

# Fields passed via ``extra=`` that are rendered in log lines.
CONTEXT_FIELDS = ("content_hash", "job_name", "path", "status", "attempt", "reason", "same_as", "phase", "count")


def _context(record: logging.LogRecord) -> dict[str, object]:
    return {key: getattr(record, key) for key in CONTEXT_FIELDS if getattr(record, key, None) is not None}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **{key: str(value) for key, value in _context(record).items()},
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    def formatMessage(self, record: logging.LogRecord) -> str:
        context = " ".join(f"{key}={value}" for key, value in _context(record).items())
        line = super().formatMessage(record)
        return f"{line} {context}" if context else line


def configure_logging(level: str = "INFO", fmt: str = "text") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # The Kubernetes client logs every request at DEBUG through urllib3.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
