from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import load_yaml
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(slots=True)
class LlmConfig:
    provider_name: str
    base_url: str
    api_key: str
    model_name: str
    temperature: float = 0.1
    max_tokens: int = 4096
    enabled: bool = True
    verify_ssl: bool = True


@dataclass(slots=True)
class DatasourceConfig:
    id: str
    name: str
    db_type: str
    host: str
    port: int
    database: str
    username: str
    password: str
    secure: bool = False
    extra_params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DiscoveryConfig:
    allow_databases: list[str] = field(default_factory=list)
    allow_tables: list[str] = field(default_factory=list)
    trading_calendar_table: str = ""


@dataclass(slots=True)
class RuntimeStateConfig:
    database: str = "alphablocks"


@dataclass(slots=True)
class SyncAmazingDataConfig:
    username: str = ""
    password: str = ""
    host: str = ""
    port: int = 0
    local_path: str = ""


@dataclass(slots=True)
class SyncBaoStockConfig:
    user_id: str = "anonymous"
    password: str = "123456"


@dataclass(slots=True)
class SyncConfig:
    amazingdata: SyncAmazingDataConfig = field(default_factory=SyncAmazingDataConfig)
    baostock: SyncBaoStockConfig = field(default_factory=SyncBaoStockConfig)


@dataclass(slots=True)
class RuntimeConfig:
    llm: LlmConfig
    datasource: DatasourceConfig
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    runtime_state: RuntimeStateConfig = field(default_factory=RuntimeStateConfig)
    sync: SyncConfig = field(default_factory=SyncConfig)


DEFAULT_RUNTIME_CONFIG_PATH = PROJECT_ROOT / "config" / "runtime.local.yaml"
DEFAULT_RUNTIME_EXAMPLE_PATH = PROJECT_ROOT / "config" / "runtime.example.yaml"


def load_runtime_config(path: str | Path = DEFAULT_RUNTIME_CONFIG_PATH) -> RuntimeConfig:
    resolved_path = Path(path)
    if not resolved_path.is_absolute():
        resolved_path = PROJECT_ROOT / resolved_path
    if not resolved_path.exists() and resolved_path.name == "runtime.local.yaml" and DEFAULT_RUNTIME_EXAMPLE_PATH.exists():
        resolved_path = DEFAULT_RUNTIME_EXAMPLE_PATH

    data = load_yaml(resolved_path)
    if "llm" not in data or "datasource" not in data:
        raise ValueError("Runtime config must contain 'llm' and 'datasource'")
    datasource_payload = {
        "id": "primary",
        "name": "Primary Data Source",
        **(data.get("datasource", {}) or {}),
    }
    return RuntimeConfig(
        llm=LlmConfig(**data["llm"]),
        datasource=DatasourceConfig(**datasource_payload),
        discovery=DiscoveryConfig(**data.get("discovery", {})),
        runtime_state=RuntimeStateConfig(**(data.get("runtime_state", {}) or {})),
        sync=SyncConfig(
            amazingdata=SyncAmazingDataConfig(**(data.get("sync", {}).get("amazingdata", {}) or {})),
            baostock=SyncBaoStockConfig(**(data.get("sync", {}).get("baostock", {}) or {})),
        ),
    )
