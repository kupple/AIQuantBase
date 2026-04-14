from __future__ import annotations

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


RAW_FIELD_DESCRIPTION_ZH = {
    'code': '代码',
    'trade_time': '交易时间',
    'trade_date': '交易日期',
    'open': '开盘价',
    'high': '最高价',
    'low': '最低价',
    'close': '收盘价',
    'volume': '成交量',
    'amount': '成交额',
    'factor_value': '因子值',
    'market_code': '市场代码',
    'index_code': '指数代码',
    'index_name': '指数名称',
    'weight': '权重',
}


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
            return self.execute_sql(sql)
        except Exception as exc:  # pragma: no cover - defensive path
            return {
                'code': 1,
                'message': str(exc),
                'sql': '',
                'df': pd.DataFrame(),
            }

    def execute_sql(self, sql: str) -> dict[str, Any]:
        """直接执行原生 SQL，并返回标准结果结构。"""
        try:
            result = self.executor.execute_sql(sql)
            return self._build_result(
                code=0,
                message='success',
                sql=result.sql,
                df=pd.DataFrame(result.data),
            )
        except Exception as exc:  # pragma: no cover - defensive path
            return self._build_result(
                code=1,
                message=str(exc),
                sql=sql,
                df=pd.DataFrame(),
            )

    def get_real_fields_json(self) -> dict[str, Any]:
        """按 *_real 节点分组返回最小字段清单 JSON。

        这个方法只返回节点名、表名、中文说明，以及字段的英文名和中文名，
        便于外部模块先做字段选择；真正执行查询仍然通过 execute_intent 完成。
        """
        fields_by_node: dict[str, list[dict[str, Any]]] = {}
        for field in self.field_catalog:
            if not field.source_node or not field.source_node.endswith('_real'):
                continue
            fields_by_node.setdefault(field.source_node, []).append(
                {
                    'field_name': field.standard_field,
                    'field_label_zh': field.description_zh,
                }
            )

        items = []
        for node in self.nodes:
            if not node.name.endswith('_real'):
                continue
            node_fields = list(fields_by_node.get(node.name, []))
            existing_standard = {item['field_name'] for item in node_fields}
            for raw_name in node.fields:
                if raw_name in existing_standard:
                    continue
                node_fields.append(
                    {
                        'field_name': raw_name,
                        'field_label_zh': RAW_FIELD_DESCRIPTION_ZH.get(raw_name, raw_name),
                    }
                )
            items.append(
                {
                    'name': node.name,
                    'table': node.table,
                    'grain': node.grain,
                    'description_zh': node.description_zh,
                    'is_ai_entry': node.is_ai_entry,
                    'fields': sorted(node_fields, key=lambda item: item['field_name']),
                }
            )

        return {
            'code': 0,
            'message': 'success',
            'items': items,
        }

    def _build_result(self, code: int, message: str, sql: str, df: pd.DataFrame) -> dict[str, Any]:
        return {
            'code': code,
            'message': message,
            'sql': sql,
            'df': df,
        }


def _intent_from_dict(intent: dict[str, Any]):
    from .config import load_query_intent, dump_yaml
    from tempfile import NamedTemporaryFile

    with NamedTemporaryFile('w+', suffix='.yaml', encoding='utf-8', delete=True) as handle:
        handle.write(dump_yaml(intent))
        handle.flush()
        return load_query_intent(handle.name)
