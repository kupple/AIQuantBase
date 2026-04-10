from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import load_yaml


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


@dataclass(slots=True)
class RuntimeConfig:
    llm: LlmConfig
    datasource: DatasourceConfig
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)


DEFAULT_RUNTIME_CONFIG_PATH = Path("config/runtime.local.yaml")
DEFAULT_RUNTIME_EXAMPLE_PATH = Path("config/runtime.example.yaml")


def load_runtime_config(path: str | Path = DEFAULT_RUNTIME_CONFIG_PATH) -> RuntimeConfig:
    data = load_yaml(path)
    if "llm" not in data or "datasource" not in data:
        raise ValueError("Runtime config must contain 'llm' and 'datasource'")
    return RuntimeConfig(
        llm=LlmConfig(**data["llm"]),
        datasource=DatasourceConfig(**data["datasource"]),
        discovery=DiscoveryConfig(**data.get("discovery", {})),
    )
