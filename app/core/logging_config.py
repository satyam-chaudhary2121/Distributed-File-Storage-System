"""Logging setup shared by the coordinator and storage nodes."""

from __future__ import annotations

import logging
import sys

_FORMAT = "%(asctime)s %(levelname)-7s [%(component)s] %(name)s: %(message)s"


class _ComponentFilter(logging.Filter):
    def __init__(self, component: str) -> None:
        super().__init__()
        self.component = component

    def filter(self, record: logging.LogRecord) -> bool:
        record.component = self.component
        return True


def setup_logging(level: str = "INFO", component: str = "coordinator") -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_FORMAT))
    handler.addFilter(_ComponentFilter(component))
    root.addHandler(handler)
    root.setLevel(level.upper())
    # Uvicorn access logs are noisy during tests and demos.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
