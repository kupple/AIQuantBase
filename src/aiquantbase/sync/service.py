from __future__ import annotations

from pathlib import Path

from ..sync_integration import SyncIntegration


class SyncService(SyncIntegration):
    pass


def build_sync_service(sync_project_root: str | Path | None = None) -> SyncService:
    return SyncService(sync_project_root=sync_project_root)
