from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["run_sync"]


def __getattr__(name: str) -> Any:
    if name == "run_sync":
        return import_module("sync_data_system.run_sync")
    raise AttributeError(f"module 'sync_data_system' has no attribute {name!r}")
