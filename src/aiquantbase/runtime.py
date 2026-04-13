from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_field_catalog, load_nodes_and_edges
from .executor import ClickHouseExecutor
from .planner import GraphRegistry, QueryPlanner
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, load_runtime_config
from .sql import SqlRenderer

DEFAULT_GRAPH_PATH = Path('config/graph.yaml')
DEFAULT_FIELDS_PATH = Path('config/fields.yaml')


@dataclass(slots=True)
class RuntimeResult:
    code: int
    message: str
    sql: str
    df: pd.DataFrame


class GraphRuntime:
    def __init__(
        self,
        graph_path: str | Path = DEFAULT_GRAPH_PATH,
        fields_path: str | Path = DEFAULT_FIELDS_PATH,
        runtime_path: str | Path = DEFAULT_RUNTIME_CONFIG_PATH,
    ) -> None:
        self.graph_path = Path(graph_path)
        self.fields_path = Path(fields_path)
        self.runtime_path = Path(runtime_path)

        self.runtime_config = load_runtime_config(self.runtime_path)
        self.nodes, self.edges = load_nodes_and_edges(self.graph_path)
        self.field_catalog = load_field_catalog(self.fields_path)
        self.registry = GraphRegistry(self.nodes, self.edges, field_catalog=self.field_catalog)
        self.planner = QueryPlanner(self.registry)
        self.renderer = SqlRenderer(self.registry)
        self.executor = ClickHouseExecutor(self.runtime_config.datasource)

    @classmethod
    def from_defaults(cls) -> 'GraphRuntime':
        return cls(
            graph_path=DEFAULT_GRAPH_PATH,
            fields_path=DEFAULT_FIELDS_PATH,
            runtime_path=DEFAULT_RUNTIME_CONFIG_PATH,
        )

    def render_intent(self, intent: dict[str, Any]) -> str:
        plan = self.planner.plan(_intent_from_dict(intent))
        return self.renderer.render(plan)

    def get_metadata_catalog(self) -> dict[str, Any]:
        return {
            'nodes': [
                {
                    'name': node.name,
                    'table': node.table,
                    'entity_keys': list(node.entity_keys),
                    'time_key': node.time_key,
                    'grain': node.grain,
                    'description': node.description,
                    'description_zh': node.description_zh,
                    'node_role': node.node_role,
                    'is_ai_entry': node.is_ai_entry,
                }
                for node in self.nodes
            ],
            'edges': [
                {
                    'name': edge.name,
                    'from': edge.from_node,
                    'to': edge.to_node,
                    'relation_type': edge.relation_type,
                    'description': edge.description,
                    'description_zh': edge.description_zh,
                    'time_binding': {
                        'mode': edge.time_binding.mode,
                        'base_time_field': edge.time_binding.base_time_field,
                        'source_time_field': edge.time_binding.source_time_field,
                        'source_start_field': edge.time_binding.source_start_field,
                        'source_end_field': edge.time_binding.source_end_field,
                        'base_time_cast': edge.time_binding.base_time_cast,
                        'source_time_cast': edge.time_binding.source_time_cast,
                    }
                    if edge.time_binding
                    else None,
                }
                for edge in self.edges
            ],
            'fields': [
                {
                    'standard_field': field.standard_field,
                    'source_node': field.source_node,
                    'source_field': field.source_field,
                    'field_role': field.field_role,
                    'resolver_type': field.resolver_type,
                    'applies_to_grain': field.applies_to_grain,
                    'path_domain': field.path_domain,
                    'path_group': field.path_group,
                    'via_node': field.via_node,
                    'time_semantics': field.time_semantics,
                    'lookahead_category': field.lookahead_category,
                    'description_zh': field.description_zh,
                    'depends_on': list(field.depends_on),
                    'formula': field.formula,
                }
                for field in self.field_catalog
            ],
        }

    def get_real_nodes(self) -> list[dict[str, Any]]:
        """返回当前图谱中所有 *_real 节点的信息。

        这个方法是给外部模块快速查看“当前有哪些正式业务节点”用的，
        会返回节点名、表名、粒度、中文说明、节点角色以及是否作为 AI 主入口开放。
        """
        return [
            {
                'name': node.name,
                'table': node.table,
                'entity_keys': list(node.entity_keys),
                'time_key': node.time_key,
                'grain': node.grain,
                'description': node.description,
                'description_zh': node.description_zh,
                'node_role': node.node_role,
                'is_ai_entry': node.is_ai_entry,
            }
            for node in self.nodes
            if node.name.endswith('_real')
        ]

    def execute_intent(self, intent: dict[str, Any]) -> dict[str, Any]:
        try:
            plan = self.planner.plan(_intent_from_dict(intent))
            sql = self.renderer.render(plan)
            result = self.executor.execute_sql(sql)
            df = pd.DataFrame(result.data)
            return {
                'code': 0,
                'message': 'success',
                'sql': result.sql,
                'df': df,
            }
        except Exception as exc:  # pragma: no cover - defensive path
            return {
                'code': 1,
                'message': str(exc),
                'sql': '',
                'df': pd.DataFrame(),
            }


def _intent_from_dict(intent: dict[str, Any]):
    from .config import load_query_intent, dump_yaml
    from tempfile import NamedTemporaryFile

    with NamedTemporaryFile('w+', suffix='.yaml', encoding='utf-8', delete=True) as handle:
        handle.write(dump_yaml(intent))
        handle.flush()
        return load_query_intent(handle.name)
