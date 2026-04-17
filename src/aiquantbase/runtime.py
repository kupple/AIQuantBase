from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from datetime import datetime

from .config import load_field_catalog, load_nodes_and_edges
from .executor import ClickHouseExecutor
from .planner import GraphRegistry, QueryPlanner
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, load_runtime_config
from .sql import SqlRenderer

DEFAULT_GRAPH_PATH = Path('config/graph.yaml')
DEFAULT_FIELDS_PATH = Path('config/fields.yaml')
MAX_MINUTE_QUERY_HOURS = 8
MAX_DAILY_UNIVERSE_DAYS = 31
SUCCESS_CODE = 0
EXECUTION_FAILED_CODE = 1000
ISSUE_ERROR_CODES = {
    'unknown_symbol_type': 1101,
    'unsupported_universe': 1102,
    'missing_symbols': 1103,
    'symbols_required_for_minute': 1104,
    'minute_time_range_too_large': 1105,
    'daily_universe_time_range_too_large': 1106,
    'mixed_asset_types': 1107,
    'unsupported_freq': 1108,
    'unsupported_field': 1109,
    'unsupported_asset_type': 1110,
    'missing_query_node': 1111,
}


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

LEGACY_ASSET_NODE_MAP = {
    'fund': {'1d': 'fund_share_real'},
    'macro': {'1d': 'treasury_yield_real'},
    'kzz': {'1d': 'kzz_conv_real'},
}

IDENTITY_FIELDS = {
    'stock_daily_real': ['code', 'trade_time'],
    'stock_minute_real': ['code', 'trade_time'],
    'etf_daily_real': ['code', 'trade_time'],
    'etf_minute_real': ['code', 'trade_time'],
    'index_daily_real': ['code', 'trade_time'],
    'index_minute_real': ['code', 'trade_time'],
    'etf_pcf_real': ['etf_code', 'etf_trading_day'],
    'fund_share_real': ['market_code', 'fund_share_ann_date'],
    'treasury_yield_real': ['treasury_term', 'treasury_trade_date'],
    'kzz_conv_real': ['market_code', 'kzz_conv_rule_ann_date'],
}

ASSET_FIELD_ALLOWLIST = {
    ('etf', '1d'): {
        'code', 'trade_time', 'open', 'high', 'low', 'close', 'volume', 'amount',
        'security_type', 'security_name', 'list_date', 'delist_date',
        'close_adj', 'open_adj', 'high_adj', 'low_adj',
        'etf_code', 'etf_trading_day', 'etf_nav', 'etf_nav_per_cu', 'etf_cash_component',
        'etf_cash_component_ratio', 'etf_limit_spread', 'etf_net_limit_spread',
        'etf_creation_flag', 'etf_redemption_flag', 'etf_symbol',
    },
    ('etf', '1m'): {
        'code', 'trade_time', 'open', 'high', 'low', 'close', 'volume', 'amount',
        'minute_open', 'minute_high', 'minute_low', 'minute_close', 'minute_volume', 'minute_amount',
        'security_type', 'security_name',
    },
    ('index', '1d'): {
        'code', 'trade_time', 'open', 'high', 'low', 'close', 'volume', 'amount',
        'security_type', 'security_name', 'list_date', 'delist_date',
    },
    ('index', '1m'): {
        'code', 'trade_time', 'open', 'high', 'low', 'close', 'volume', 'amount',
        'minute_open', 'minute_high', 'minute_low', 'minute_close', 'minute_volume', 'minute_amount',
        'security_type', 'security_name',
    },
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
        self.asset_node_map = self._build_asset_node_map()
        self.application_query_node_map = self._build_application_query_node_map()

    @classmethod
    def from_defaults(cls) -> 'GraphRuntime':
        return cls(
            graph_path=DEFAULT_GRAPH_PATH,
            fields_path=DEFAULT_FIELDS_PATH,
            runtime_path=DEFAULT_RUNTIME_CONFIG_PATH,
        )

    def render_intent(self, intent: dict[str, Any]) -> str:
        plan = self.planner.plan(_intent_from_dict(self._expand_logical_intent(intent)))
        return self.renderer.render(plan)

    def resolve_symbols(self, symbols: list[str]) -> dict[str, Any]:
        items = []
        issues = []
        for symbol in symbols:
            asset_type = self._infer_asset_type(symbol)
            if asset_type is None:
                issues.append(
                    self._issue(
                        code='unknown_symbol_type',
                        message=f'cannot infer asset type for symbol={symbol}',
                        symbol=symbol,
                    )
                )
                continue
            node = self.asset_node_map.get(asset_type, {}).get('1d')
            market = symbol.split('.')[-1] if '.' in symbol else None
            items.append(
                {
                    'symbol': symbol,
                    'asset_type': asset_type,
                    'market': market,
                    'node': self.application_query_node_map.get(asset_type, {}).get('1d'),
                    'graph_node': self._physical_node_for(node) if node else None,
                }
            )
        return {
            'ok': not issues,
            'items': items,
            'issues': issues,
        }

    def get_supported_fields(
        self,
        *,
        asset_type: str | None = None,
        freq: str = '1d',
        node: str | None = None,
        field_role: str | None = None,
        derived_only: bool | None = None,
    ) -> dict[str, Any]:
        assets = [asset_type] if asset_type else sorted(self.application_query_node_map.keys())
        rows = []
        for asset in assets:
            node_names = self._candidate_nodes_for_asset_type(asset, freq)
            if not node_names:
                continue
            for node_name in node_names:
                if node and node_name != node:
                    continue
                for field in self._fields_for_node(node_name):
                    is_derived = field.get('kind') == 'derived_field'
                    if field_role and field.get('kind') != field_role:
                        continue
                    if derived_only is True and not is_derived:
                        continue
                    if derived_only is False and is_derived:
                        continue
                    rows.append(
                        {
                            'name': field['field_name'],
                            'description_zh': field['field_label_zh'],
                            'asset_types': [asset],
                            'derived': is_derived,
                            'field_role': field.get('kind'),
                            'notes': list(field.get('notes') or []),
                            'node': node_name,
                        }
                    )
        dedup: dict[str, dict[str, Any]] = {}
        for row in rows:
            current = dedup.get(row['name'])
            if current:
                merged = sorted(set(current['asset_types'] + row['asset_types']))
                current['asset_types'] = merged
            else:
                dedup[row['name']] = row
        return {
            'ok': True,
            'asset_type': asset_type,
            'freq': freq,
            'node': node,
            'field_role': field_role,
            'derived_only': derived_only,
            'fields': sorted(dedup.values(), key=lambda item: item['name']),
        }

    def validate_query_request(self, request: dict[str, Any]) -> dict[str, Any]:
        symbols = list(request.get('symbols') or [])
        fields = list(request.get('fields') or [])
        freq = request.get('freq', '1d')
        requested_asset_type = request.get('asset_type', 'auto')
        issues = []

        if request.get('universe'):
            issues.append(
                self._issue(
                    code='unsupported_universe',
                    message='universe is not supported in current version',
                    path='universe',
                )
            )

        if not symbols:
            issues.append(
                self._issue(
                    code='missing_symbols',
                    message='symbols is required',
                    path='symbols',
                )
            )

        start = request.get('start')
        end = request.get('end')
        if freq == '1m' and not symbols:
            issues.append(
                self._issue(
                    code='symbols_required_for_minute',
                    message='minute queries must include symbols',
                    path='symbols',
                )
            )
        if freq == '1m' and start and end:
            hours = self._time_range_hours(start, end)
            if hours is not None and hours > MAX_MINUTE_QUERY_HOURS:
                issues.append(
                    self._issue(
                        code='minute_time_range_too_large',
                        message=f'minute query time range exceeds {MAX_MINUTE_QUERY_HOURS} hours',
                        path='time_range',
                    )
                )
        if freq == '1d' and not symbols and start and end:
            days = self._time_range_days(start, end)
            if days is not None and days > MAX_DAILY_UNIVERSE_DAYS:
                issues.append(
                    self._issue(
                        code='daily_universe_time_range_too_large',
                        message=f'daily query without symbols exceeds {MAX_DAILY_UNIVERSE_DAYS} days',
                        path='time_range',
                    )
                )

        resolved_asset_type = requested_asset_type
        resolved_node = None
        if requested_asset_type == 'auto' and symbols:
            resolved_symbols = self.resolve_symbols(symbols)
            issues.extend(
                {
                    'path': f'symbols.{item["symbol"]}',
                    'code': issue['code'],
                    'message': issue['message'],
                }
                for issue in resolved_symbols['issues']
                for item in [{'symbol': issue['symbol']}]
            )
            asset_types = sorted({item['asset_type'] for item in resolved_symbols['items']})
            if len(asset_types) == 1:
                resolved_asset_type = asset_types[0]
            elif len(asset_types) > 1:
                issues.append(
                    self._issue(
                        code='mixed_asset_types',
                        message=f'mixed asset types detected: {asset_types}',
                        path='symbols',
                    )
                )
        if resolved_asset_type and not self.application_query_node_map.get(resolved_asset_type, {}).get(freq):
            issues.append(
                self._issue(
                    code='unsupported_asset_type',
                    message=f'asset_type={resolved_asset_type} is not supported for freq={freq}',
                    path='symbols.0' if symbols else 'asset_type',
                )
            )

        resolved_best_node = self.resolve_best_node(
            symbols=symbols,
            fields=fields,
            freq=freq,
            asset_type=resolved_asset_type,
        )
        resolved_node = resolved_best_node.get('node')
        if not resolved_node and not any(issue['code'] == 'unsupported_asset_type' for issue in issues):
            issues.append(
                self._issue(
                    code='unsupported_freq',
                    message=f'freq={freq} is not supported for asset_type={resolved_asset_type}',
                    path='freq',
                )
            )

        supported_names = {item['name'] for item in self.get_supported_fields(asset_type=resolved_asset_type, freq=freq)['fields']}
        for field in fields:
            if field not in supported_names or not self._is_field_allowed_for_asset(field, resolved_asset_type, freq):
                issues.append(
                    self._issue(
                        code='unsupported_field',
                        message=f'{field} is not supported for asset_type={resolved_asset_type}, freq={freq}',
                        path=f'fields.{field}',
                    )
                )

        return {
            'ok': not issues,
            'issues': issues,
            'resolved': {
                'asset_type': resolved_asset_type,
                'node': resolved_node,
            },
        }

    def resolve_best_node(
        self,
        *,
        symbols: list[str] | None,
        fields: list[str],
        freq: str = '1d',
        asset_type: str = 'auto',
    ) -> dict[str, Any]:
        resolved_asset_type = asset_type
        issues = []
        if asset_type == 'auto' and symbols:
            resolved_symbols = self.resolve_symbols(list(symbols))
            asset_types = sorted({item['asset_type'] for item in resolved_symbols['items']})
            if len(asset_types) == 1:
                resolved_asset_type = asset_types[0]
            elif len(asset_types) > 1:
                issues.append(
                    self._issue(
                        code='mixed_asset_types',
                        message=f'mixed asset types detected: {asset_types}',
                    )
                )
                resolved_asset_type = None
            else:
                resolved_asset_type = None

        candidates = self._candidate_nodes_for_asset_type(resolved_asset_type, freq)
        if resolved_asset_type and not candidates:
            issues.append(
                self._issue(
                    code='missing_query_node',
                    message=f'asset_type={resolved_asset_type} has no supported {freq} query node',
                )
            )
        scored = []
        field_sets = {node_name: {item['field_name'] for item in self._fields_for_node(node_name)} for node_name in candidates}
        for node_name in candidates:
            supported_fields = sorted(set(fields) & field_sets[node_name])
            unsupported_fields = sorted(set(fields) - field_sets[node_name])
            scored.append(
                {
                    'node': node_name,
                    'supported_fields': supported_fields,
                    'unsupported_fields': unsupported_fields,
                    'score': len(supported_fields),
                }
            )
        scored.sort(key=lambda item: (-item['score'], item['node']))
        best = next((item for item in scored if not item['unsupported_fields']), None)
        fallback = scored[0] if scored else None
        return {
            'ok': best is not None and not issues,
            'node': best['node'] if best else (fallback['node'] if fallback else None),
            'asset_type': resolved_asset_type,
            'freq': freq,
            'candidates': scored,
            'issues': issues,
        }

    def query_daily(
        self,
        *,
        symbols: list[str] | None = None,
        universe: str | None = None,
        fields: list[str],
        start: str,
        end: str,
        asset_type: str = 'auto',
        freq: str = '1d',
    ) -> dict[str, Any]:
        request = {
            'symbols': symbols or [],
            'universe': universe,
            'fields': fields,
            'start': start,
            'end': end,
            'freq': freq,
            'asset_type': asset_type,
        }
        validation = self.validate_query_request(request)
        if not validation['ok']:
            return {
                'ok': False,
                'df': pd.DataFrame(),
                'issues': validation['issues'],
                'meta': {
                    'asset_type': validation['resolved']['asset_type'],
                    'node': validation['resolved']['node'],
                    'fields': fields,
                    'row_count': 0,
                    'symbol_count': len(symbols or []),
                    'empty': True,
                },
                'debug': {
                    'intent': None,
                    'sql': '',
                },
            }

        node = validation['resolved']['node']
        identity_fields = self._identity_fields_for_node(node)
        select_fields = list(dict.fromkeys(identity_fields + fields))
        symbol_field = identity_fields[0] if identity_fields else 'code'
        time_field = identity_fields[1] if len(identity_fields) > 1 else 'trade_time'
        intent = {
            'from': node,
            'select': select_fields,
            'where': {
                'mode': 'and',
                'items': (
                    [{'field': symbol_field, 'op': 'in' if len(symbols or []) > 1 else '=', 'value': (symbols if len(symbols or []) > 1 else (symbols or [None])[0])}]
                    if symbols
                    else []
                ),
            },
            'time_range': {
                'field': time_field,
                'start': start,
                'end': end,
            },
            'page': 1,
            'page_size': 500,
            'safety': {
                'lookahead_safe': False,
                'strict_mode': True,
            },
        }
        result = self.execute_intent(intent)
        return {
            'ok': result['code'] == 0,
            'df': result['df'],
            'issues': [],
            'meta': {
                'asset_type': validation['resolved']['asset_type'],
                'node': node,
                'fields': fields,
                'row_count': int(len(result['df'])),
                'symbol_count': len(symbols or []),
                'empty': bool(result['df'].empty),
            },
            'debug': {
                'intent': intent,
                'sql': result['sql'],
            },
        }

    def query_minute(
        self,
        *,
        symbols: list[str] | None = None,
        universe: str | None = None,
        fields: list[str],
        start: str,
        end: str,
        asset_type: str = 'auto',
        freq: str = '1m',
    ) -> dict[str, Any]:
        request = {
            'symbols': symbols or [],
            'universe': universe,
            'fields': fields,
            'start': start,
            'end': end,
            'freq': freq,
            'asset_type': asset_type,
        }
        validation = self.validate_query_request(request)
        if not validation['ok']:
            return {
                'ok': False,
                'df': pd.DataFrame(),
                'issues': validation['issues'],
                'meta': {
                    'asset_type': validation['resolved']['asset_type'],
                    'node': validation['resolved']['node'],
                    'fields': fields,
                    'row_count': 0,
                    'symbol_count': len(symbols or []),
                    'empty': True,
                },
                'debug': {
                    'intent': None,
                    'sql': '',
                },
            }

        node = validation['resolved']['node']
        identity_fields = self._identity_fields_for_node(node)
        select_fields = list(dict.fromkeys(identity_fields + fields))
        symbol_field = identity_fields[0] if identity_fields else 'code'
        time_field = identity_fields[1] if len(identity_fields) > 1 else 'trade_time'
        intent = {
            'from': node,
            'select': select_fields,
            'where': {
                'mode': 'and',
                'items': (
                    [{'field': symbol_field, 'op': 'in' if len(symbols or []) > 1 else '=', 'value': (symbols if len(symbols or []) > 1 else (symbols or [None])[0])}]
                    if symbols
                    else []
                ),
            },
            'time_range': {
                'field': time_field,
                'start': start,
                'end': end,
            },
            'page': 1,
            'page_size': 1000,
            'safety': {
                'lookahead_safe': False,
                'strict_mode': True,
            },
        }
        result = self.execute_intent(intent)
        return {
            'ok': result['code'] == 0,
            'df': result['df'],
            'issues': [],
            'meta': {
                'asset_type': validation['resolved']['asset_type'],
                'node': node,
                'fields': fields,
                'row_count': int(len(result['df'])),
                'symbol_count': len(symbols or []),
                'empty': bool(result['df'].empty),
            },
            'debug': {
                'intent': intent,
                'sql': result['sql'],
            },
        }

    def query_minute(
        self,
        *,
        symbols: list[str] | None = None,
        universe: str | None = None,
        fields: list[str],
        start: str,
        end: str,
        asset_type: str = 'auto',
        freq: str = '1m',
    ) -> dict[str, Any]:
        request = {
            'symbols': symbols or [],
            'universe': universe,
            'fields': fields,
            'start': start,
            'end': end,
            'freq': freq,
            'asset_type': asset_type,
        }
        validation = self.validate_query_request(request)
        if not validation['ok']:
            return {
                'ok': False,
                'df': pd.DataFrame(),
                'issues': validation['issues'],
                'meta': {
                    'asset_type': validation['resolved']['asset_type'],
                    'node': validation['resolved']['node'],
                    'fields': fields,
                    'row_count': 0,
                    'symbol_count': len(symbols or []),
                    'empty': True,
                },
                'debug': {
                    'intent': None,
                    'sql': '',
                },
            }

        node = validation['resolved']['node']
        identity_fields = self._identity_fields_for_node(node)
        select_fields = list(dict.fromkeys(identity_fields + fields))
        symbol_field = identity_fields[0] if identity_fields else 'code'
        time_field = identity_fields[1] if len(identity_fields) > 1 else 'trade_time'
        intent = {
            'from': node,
            'select': select_fields,
            'where': {
                'mode': 'and',
                'items': (
                    [{'field': symbol_field, 'op': 'in' if len(symbols or []) > 1 else '=', 'value': (symbols if len(symbols or []) > 1 else (symbols or [None])[0])}]
                    if symbols
                    else []
                ),
            },
            'time_range': {
                'field': time_field,
                'start': start,
                'end': end,
            },
            'page': 1,
            'page_size': 1000,
            'safety': {
                'lookahead_safe': False,
                'strict_mode': True,
            },
        }
        result = self.execute_intent(intent)
        return {
            'ok': result['code'] == 0,
            'df': result['df'],
            'issues': [],
            'meta': {
                'asset_type': validation['resolved']['asset_type'],
                'node': node,
                'fields': fields,
                'row_count': int(len(result['df'])),
                'symbol_count': len(symbols or []),
                'empty': bool(result['df'].empty),
            },
            'debug': {
                'intent': intent,
                'sql': result['sql'],
            },
        }

    def build_intent_from_requirement(self, data_requirement: dict[str, Any]) -> dict[str, Any]:
        fields = list(data_requirement.get('fields') or [])
        scope = dict(data_requirement.get('scope') or {})
        symbols = list(scope.get('symbols') or [])
        freq = scope.get('freq', '1d')
        start = scope.get('start')
        end = scope.get('end')
        asset_type = data_requirement.get('asset_type', 'auto')

        validation = self.validate_query_request(
            {
                'symbols': symbols,
                'universe': scope.get('universe'),
                'fields': fields,
                'start': start,
                'end': end,
                'freq': freq,
                'asset_type': asset_type,
            }
        )
        if not validation['ok']:
            return {
                'ok': False,
                'issues': validation['issues'],
                'resolved': validation['resolved'],
                'intent': None,
            }

        node = validation['resolved']['node']
        identity_fields = self._identity_fields_for_node(node)
        select_fields = list(dict.fromkeys(identity_fields + fields))
        symbol_field = identity_fields[0] if identity_fields else 'code'
        time_field = identity_fields[1] if len(identity_fields) > 1 else 'trade_time'
        where_items = []
        if symbols:
            where_items.append(
                {
                    'field': symbol_field,
                    'op': 'in' if len(symbols) > 1 else '=',
                    'value': symbols if len(symbols) > 1 else symbols[0],
                }
            )
        intent = {
            'from': node,
            'select': select_fields,
            'where': {
                'mode': 'and',
                'items': where_items,
            },
            'time_range': {
                'field': time_field,
                'start': start,
                'end': end,
            },
            'order_by': [
                {'field': time_field, 'direction': 'asc'},
                {'field': symbol_field, 'direction': 'asc'},
            ],
            'page': 1,
            'page_size': 500,
            'safety': {
                'lookahead_safe': False,
                'strict_mode': True,
            },
        }
        return {
            'ok': True,
            'issues': [],
            'resolved': validation['resolved'],
            'intent': intent,
        }

    def execute_requirement(self, data_requirement: dict[str, Any]) -> dict[str, Any]:
        built = self.build_intent_from_requirement(data_requirement)
        if not built['ok']:
            return {
                'ok': False,
                'df': pd.DataFrame(),
                'issues': built['issues'],
                'resolved': built['resolved'],
                'debug': {
                    'intent': None,
                    'sql': '',
                },
            }

        result = self.execute_intent(built['intent'])
        return {
            'ok': result['code'] == 0,
            'df': result['df'],
            'issues': [],
            'resolved': built['resolved'],
            'debug': {
                'intent': built['intent'],
                'sql': result['sql'],
            },
        }

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
                    'status': node.status,
                    'physical_node': node.physical_node,
                    'asset_type': node.asset_type,
                    'query_freq': node.query_freq,
                    'base_filters': list(node.base_filters),
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
                    'notes': list(field.notes),
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
                'status': node.status,
                'physical_node': node.physical_node,
                'asset_type': node.asset_type,
                'query_freq': node.query_freq,
                'base_filters': list(node.base_filters),
            }
            for node in self.nodes
            if node.name.endswith('_real')
        ]

    def get_protocol_summary(self) -> dict[str, Any]:
        """返回当前对外协议层的精简摘要。

        这个接口刻意不返回完整字段明细，只返回：
        1. 当前启用的主入口节点
        2. 每个入口节点的粒度/资产类型/字段数量
        3. 若干示例字段

        上层模块可以先用这个方法快速理解当前协议规模，
        再按需调用 get_real_fields_json() 拉取详细字段清单。
        """
        enabled_real_nodes = [
            node for node in self.nodes
            if node.name.endswith('_real') and node.status == 'enabled'
        ]
        items: list[dict[str, Any]] = []
        total_fields = 0
        for node in enabled_real_nodes:
            field_items = self._fields_for_node(node.name)
            total_fields += len(field_items)
            identity_fields = self._identity_fields_for_node(node.name)
            items.append(
                {
                    'name': node.name,
                    'description_zh': node.description_zh,
                    'table': node.table,
                    'grain': node.grain,
                    'asset_type': node.asset_type,
                    'query_freq': node.query_freq,
                    'physical_node': node.physical_node,
                    'identity_fields': identity_fields,
                    'field_count': len(field_items),
                    'sample_fields': self._protocol_sample_fields_for_node(node.name, identity_fields),
                    'base_filters': list(node.base_filters),
                }
            )

        return {
            'code': 0,
            'message': 'success',
            'summary': {
                'enabled_real_nodes': len(enabled_real_nodes),
                'disabled_real_nodes': len(
                    [node for node in self.nodes if node.name.endswith('_real') and node.status != 'enabled']
                ),
                'total_fields_across_enabled_nodes': total_fields,
                'edge_count': len(self.edges),
            },
            'items': items,
        }

    def get_disabled_node_cleanup_report(self) -> dict[str, Any]:
        """分析当前 disabled 节点是否可以安全从图谱中移除。"""
        enabled_real_nodes = [
            node for node in self.nodes
            if node.name.endswith('_real') and node.status == 'enabled'
        ]
        baseline_fields = {
            node.name: self._field_name_set_for_node(node.name)
            for node in enabled_real_nodes
        }

        items: list[dict[str, Any]] = []
        for node in self.nodes:
            if not node.name.endswith('_real') or node.status != 'disabled':
                continue
            items.append(self._analyze_disabled_node_cleanup(node.name, baseline_fields))

        safe_items = [item for item in items if item['can_delete']]
        blocked_items = [item for item in items if not item['can_delete']]
        return {
            'code': 0,
            'message': 'success',
            'summary': {
                'disabled_real_nodes': len(items),
                'safe_to_delete': len(safe_items),
                'blocked': len(blocked_items),
            },
            'safe_nodes': [item['name'] for item in safe_items],
            'blocked_nodes': [item['name'] for item in blocked_items],
            'items': items,
        }

    def execute_intent(self, intent: dict[str, Any]) -> dict[str, Any]:
        try:
            plan = self.planner.plan(_intent_from_dict(self._expand_logical_intent(intent)))
            sql = self.renderer.render(plan)
            return self.execute_sql(sql)
        except Exception as exc:  # pragma: no cover - defensive path
            return {
                'code': EXECUTION_FAILED_CODE,
                'message': str(exc),
                'sql': '',
                'df': pd.DataFrame(),
            }

    def execute_sql(self, sql: str) -> dict[str, Any]:
        """直接执行原生 SQL，并返回标准结果结构。"""
        try:
            result = self.executor.execute_sql(sql)
            return self._build_result(
                code=SUCCESS_CODE,
                message='success',
                sql=result.sql,
                df=pd.DataFrame(result.data),
            )
        except Exception as exc:  # pragma: no cover - defensive path
            return self._build_result(
                code=EXECUTION_FAILED_CODE,
                message=str(exc),
                sql=sql,
                df=pd.DataFrame(),
            )

    def get_real_fields_json(self) -> dict[str, Any]:
        """按 *_real 节点分组返回最小字段清单 JSON。

        这个方法只返回节点名、表名、中文说明，以及字段的英文名和中文名，
        便于外部模块先做字段选择；真正执行查询仍然通过 execute_intent 完成。
        """
        items = []
        for node in self.nodes:
            if not node.name.endswith('_real'):
                continue
            items.append(
                {
                    'name': node.name,
                    'table': node.table,
                    'grain': node.grain,
                    'description_zh': node.description_zh,
                    'status': node.status,
                    'physical_node': node.physical_node,
                    'asset_type': node.asset_type,
                    'query_freq': node.query_freq,
                    'base_filters': list(node.base_filters),
                    'fields': [
                        {
                            'field_name': item['field_name'],
                            'field_label_zh': item['field_label_zh'],
                            'notes': list(item.get('notes') or []),
                        }
                        for item in self._fields_for_node(node.name)
                    ],
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

    def _analyze_disabled_node_cleanup(
        self,
        node_name: str,
        baseline_fields: dict[str, set[str]],
    ) -> dict[str, Any]:
        target = self.registry.nodes[node_name]
        trimmed_nodes = [node for node in self.nodes if node.name != node_name]
        trimmed_edges = [
            edge for edge in self.edges
            if edge.from_node != node_name and edge.to_node != node_name
        ]
        trimmed_runtime = self._clone_with_graph(trimmed_nodes, trimmed_edges)

        affected_enabled_nodes: list[dict[str, Any]] = []
        for enabled_node_name, current_fields in baseline_fields.items():
            try:
                next_fields = trimmed_runtime._field_name_set_for_node(enabled_node_name)
            except Exception as exc:  # pragma: no cover - defensive path
                affected_enabled_nodes.append(
                    {
                        'node': enabled_node_name,
                        'missing_count': len(current_fields),
                        'missing_fields_sample': sorted(current_fields)[:20],
                        'error': str(exc),
                    }
                )
                continue
            missing_fields = sorted(current_fields - next_fields)
            if missing_fields:
                affected_enabled_nodes.append(
                    {
                        'node': enabled_node_name,
                        'missing_count': len(missing_fields),
                        'missing_fields_sample': missing_fields[:20],
                    }
                )

        source_node_fields = sorted(
            {
                field.standard_field
                for field in self.field_catalog
                if field.source_node == node_name
            }
        )
        via_node_fields = sorted(
            {
                field.standard_field
                for field in self.field_catalog
                if field.via_node == node_name
            }
        )
        legacy_source_table_fields = sorted(
            {
                field.standard_field
                for field in self.field_catalog
                if field.binding_mode == 'source_table'
                and field.source_table == target.table
                and not self.registry.has_direct_source_table_binding(field)
            }
        )
        direct_source_table_fields = sorted(
            {
                field.standard_field
                for field in self.field_catalog
                if field.binding_mode == 'source_table'
                and field.source_table == target.table
                and self.registry.has_direct_source_table_binding(field)
            }
        )
        physical_node_dependents = sorted(
            {
                node.name
                for node in self.nodes
                if node.name != node_name and node.physical_node == node_name
            }
        )
        edge_names = sorted(
            {
                edge.name
                for edge in self.edges
                if edge.from_node == node_name or edge.to_node == node_name
            }
        )

        return {
            'name': target.name,
            'table': target.table,
            'node_role': target.node_role,
            'can_delete': not affected_enabled_nodes and not physical_node_dependents,
            'affected_enabled_nodes': affected_enabled_nodes,
            'physical_node_dependents': physical_node_dependents,
            'legacy_field_refs': {
                'source_node_fields': source_node_fields,
                'via_node_fields': via_node_fields,
                'legacy_source_table_fields': legacy_source_table_fields,
                'direct_source_table_fields': direct_source_table_fields,
            },
            'edge_names': edge_names,
        }

    def _field_name_set_for_node(self, node_name: str) -> set[str]:
        return {item['field_name'] for item in self._fields_for_node(node_name)}

    def _protocol_sample_fields_for_node(self, node_name: str, identity_fields: list[str]) -> list[str]:
        node = self.registry.nodes.get(node_name)
        if not node:
            return identity_fields[:12]
        sample = []
        seen = set()
        for field_name in identity_fields + list(node.fields):
            if field_name in seen:
                continue
            seen.add(field_name)
            sample.append(field_name)
            if len(sample) >= 12:
                break
        return sample

    def _clone_with_graph(self, nodes, edges) -> 'GraphRuntime':
        clone = object.__new__(GraphRuntime)
        clone.graph_path = self.graph_path
        clone.fields_path = self.fields_path
        clone.runtime_path = self.runtime_path
        clone.runtime_config = self.runtime_config
        clone.nodes = list(nodes)
        clone.edges = list(edges)
        clone.field_catalog = self.field_catalog
        clone.registry = GraphRegistry(clone.nodes, clone.edges, field_catalog=clone.field_catalog)
        clone.planner = QueryPlanner(clone.registry)
        clone.renderer = SqlRenderer(clone.registry)
        clone.executor = self.executor
        clone.asset_node_map = clone._build_asset_node_map()
        clone.application_query_node_map = clone._build_application_query_node_map()
        return clone

    def _freq_from_node(self, node) -> str | None:
        if node.query_freq:
            return node.query_freq
        if node.grain == 'daily':
            return '1d'
        if node.grain == 'minute':
            return '1m'
        return None

    def _build_asset_node_map(self) -> dict[str, dict[str, str]]:
        mapping = {asset: dict(freq_map) for asset, freq_map in LEGACY_ASSET_NODE_MAP.items()}
        for node in self.nodes:
            if not node.asset_type:
                continue
            freq = self._freq_from_node(node)
            if not freq:
                continue
            mapping.setdefault(node.asset_type, {})[freq] = node.name
        return mapping

    def _build_application_query_node_map(self) -> dict[str, dict[str, str]]:
        mapping: dict[str, dict[str, str]] = {}
        for node in self.nodes:
            if node.status != 'enabled' or not node.asset_type:
                continue
            freq = self._freq_from_node(node)
            if not freq:
                continue
            mapping.setdefault(node.asset_type, {})[freq] = node.name
        for asset_type, freq_map in LEGACY_ASSET_NODE_MAP.items():
            mapping.setdefault(asset_type, {}).update(
                {freq: node_name for freq, node_name in freq_map.items() if freq not in mapping[asset_type]}
            )
        return mapping

    def _logical_node_spec(self, node_name: str | None) -> dict[str, Any] | None:
        if not node_name:
            return None
        node = self.registry.nodes.get(node_name)
        if not node:
            return None
        physical_node = node.physical_node or node.name
        asset_type = node.asset_type
        freq = self._freq_from_node(node)
        base_filters = list(node.base_filters)
        if physical_node == node.name and not base_filters and not asset_type and not freq:
            return None
        return {
            'physical_node': physical_node,
            'asset_type': asset_type,
            'freq': freq,
            'base_filters': base_filters,
        }

    def _physical_node_for(self, node_name: str | None) -> str | None:
        spec = self._logical_node_spec(node_name)
        if spec:
            return str(spec['physical_node'])
        return node_name

    def _asset_freq_for_node(self, node_name: str | None) -> tuple[str | None, str | None]:
        spec = self._logical_node_spec(node_name)
        if spec:
            return spec.get('asset_type'), spec.get('freq')
        for asset_type, freq_map in self.application_query_node_map.items():
            for freq, candidate in freq_map.items():
                if candidate == node_name:
                    return asset_type, freq
        return None, None

    def _identity_fields_for_node(self, node_name: str | None) -> list[str]:
        node = self.registry.nodes.get(node_name or '')
        if node:
            identity_fields = list(node.entity_keys)
            if node.time_key and node.time_key not in identity_fields:
                identity_fields.append(node.time_key)
            if identity_fields:
                return identity_fields
        fields = IDENTITY_FIELDS.get(node_name or '')
        if fields:
            return list(fields)
        physical_node = self._physical_node_for(node_name)
        physical = self.registry.nodes.get(physical_node or '')
        if physical:
            identity_fields = list(physical.entity_keys)
            if physical.time_key and physical.time_key not in identity_fields:
                identity_fields.append(physical.time_key)
            if identity_fields:
                return identity_fields
        return list(IDENTITY_FIELDS.get(physical_node or '', []))

    def _is_node_field_allowed(self, field_name: str, node_name: str) -> bool:
        asset_type, freq = self._asset_freq_for_node(node_name)
        if not asset_type or not freq:
            return True
        return self._is_field_allowed_for_asset(field_name, asset_type, freq)

    def _expand_logical_intent(self, intent: dict[str, Any]) -> dict[str, Any]:
        expanded = dict(intent)
        logical_node = expanded.get('from')
        physical_node = self._physical_node_for(logical_node)
        if physical_node:
            expanded['from'] = physical_node

        spec = self._logical_node_spec(logical_node)
        base_filters = list(spec.get('base_filters', [])) if spec else []
        if not base_filters:
            return expanded

        where = expanded.get('where')
        if isinstance(where, dict):
            items = list(where.get('items', []))
            expanded['where'] = {
                'mode': where.get('mode', 'and'),
                'items': base_filters + items,
            }
            return expanded
        if isinstance(where, list):
            expanded['where'] = {
                'mode': 'and',
                'items': base_filters + list(where),
            }
            return expanded

        expanded['where'] = {
            'mode': 'and',
            'items': base_filters,
        }
        return expanded

    def _issue(self, *, code: str, message: str, **extra: Any) -> dict[str, Any]:
        payload = {
            'code': code,
            'error_code': ISSUE_ERROR_CODES.get(code, 1999),
            'message': message,
        }
        payload.update(extra)
        return payload

    def _candidate_nodes_for_asset_type(self, asset_type: str | None, freq: str) -> list[str]:
        node = self.application_query_node_map.get(asset_type or '', {}).get(freq)
        return [node] if node else []

    def _resolve_best_node(self, *, asset_type: str | None, fields: list[str], freq: str) -> str | None:
        candidates = self._candidate_nodes_for_asset_type(asset_type, freq)
        if not candidates:
            return None
        field_sets = {node_name: {item['field_name'] for item in self._fields_for_node(node_name)} for node_name in candidates}
        valid = [node_name for node_name in candidates if set(fields).issubset(field_sets[node_name])]
        if not valid:
            return None
        return valid[0]

    def _is_field_allowed_for_asset(self, field_name: str, asset_type: str | None, freq: str) -> bool:
        allowlist = ASSET_FIELD_ALLOWLIST.get((asset_type, freq))
        if allowlist is None:
            return True
        return field_name in allowlist

    def _fields_for_node(self, node_name: str) -> list[dict[str, Any]]:
        fields_by_node: list[dict[str, Any]] = []
        effective_node_name = self._physical_node_for(node_name) or node_name
        node = next((item for item in self.nodes if item.name == effective_node_name), None)
        if node:
            for raw_name in node.fields:
                fields_by_node.append(
                    {
                        'field_name': raw_name,
                        'field_label_zh': RAW_FIELD_DESCRIPTION_ZH.get(raw_name, raw_name),
                        'kind': 'raw_node_field',
                        'notes': [],
                    }
                )
        for field in self.field_catalog:
            try:
                if field.base_node and field.base_node != effective_node_name:
                    continue
                if field.resolver_type == 'derived':
                    for dep in field.depends_on:
                        self.registry.resolve_field_node(dep, effective_node_name)
                elif (
                    field.binding_mode == 'source_table'
                    and field.source_table
                    and (
                        field.source_table == (node.table if node else None)
                        or self.registry.has_direct_source_table_binding(field)
                    )
                ):
                    pass
                else:
                    source_node_name = None
                    if field.source_node or field.source_table:
                        source_node_name = self.registry.resolve_source_node_for_entry(
                            field,
                            effective_node_name,
                            selected_nodes={effective_node_name},
                        )
                    if not source_node_name:
                        continue
                    self.registry.find_path(effective_node_name, source_node_name)
                fields_by_node.append(
                    {
                        'field_name': field.standard_field,
                        'field_label_zh': field.description_zh,
                        'kind': field.field_role,
                        'notes': list(field.notes),
                    }
                )
            except Exception:
                continue

        dedup: dict[str, dict[str, Any]] = {}
        for item in fields_by_node:
            if not self._is_node_field_allowed(item['field_name'], node_name):
                continue
            dedup.setdefault(item['field_name'], item)
        return sorted(dedup.values(), key=lambda item: item['field_name'])

    def _infer_asset_type(self, symbol: str) -> str | None:
        code, _, market = symbol.partition('.')
        market = market.upper()
        if market in {'SZ', 'SH'}:
            if code.startswith(('15', '16', '18', '50', '51', '52', '56', '58')):
                return 'etf'
            if (market == 'SH' and code.startswith(('000', '880'))) or (market == 'SZ' and code.startswith(('399', '980'))):
                return 'index'
            if code.startswith(('11', '12', '123', '127', '128', '110', '113', '118')):
                return 'kzz'
            if code.startswith(('000', '001', '002', '003', '300', '301', '600', '601', '603', '605', '688', '689')):
                return 'stock'
        if market == 'BJ':
            return 'stock'
        return None

    def _time_range_hours(self, start: str, end: str) -> float | None:
        try:
            start_dt = datetime.fromisoformat(start.replace('Z', ''))
            end_dt = datetime.fromisoformat(end.replace('Z', ''))
            return max((end_dt - start_dt).total_seconds() / 3600, 0)
        except Exception:
            return None

    def _time_range_days(self, start: str, end: str) -> int | None:
        try:
            start_dt = datetime.fromisoformat(start.replace('Z', ''))
            end_dt = datetime.fromisoformat(end.replace('Z', ''))
            return max((end_dt.date() - start_dt.date()).days + 1, 0)
        except Exception:
            return None


def _intent_from_dict(intent: dict[str, Any]):
    from .config import load_query_intent, dump_yaml
    from tempfile import NamedTemporaryFile

    with NamedTemporaryFile('w+', suffix='.yaml', encoding='utf-8', delete=True) as handle:
        handle.write(dump_yaml(intent))
        handle.flush()
        return load_query_intent(handle.name)
