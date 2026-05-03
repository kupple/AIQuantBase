from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from datetime import datetime
from time import perf_counter

from .config import load_field_catalog, load_nodes_and_edges
from .executor import ClickHouseExecutor
from .membership import (
    DEFAULT_MEMBERSHIP_PATH,
    filter_symbols_by_membership as membership_filter_symbols,
    query_membership as membership_query_membership,
    resolve_membership_target as membership_resolve_target,
)
from .models import Node
from .planner import GraphRegistry, QueryPlanner
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH, load_runtime_config
from .sql import SqlRenderer

DEFAULT_GRAPH_PATH = Path('config/graph.yaml')
DEFAULT_FIELDS_PATH = Path('config/fields.yaml')
MAX_MINUTE_QUERY_HOURS = 8
MAX_DAILY_UNIVERSE_DAYS = 31
MAX_INTRADAY_POINT_BATCH_DAYS = 120
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
    'empty_result': 1112,
    'query_failed': 1113,
    'missing_anchors': 1114,
    'missing_trading_days': 1115,
    'invalid_intraday_time': 1116,
    'trading_calendar_unavailable': 1117,
    'missing_execution_date': 1118,
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
    'level1_name': '一级行业名称',
    'level2_name': '二级行业名称',
    'level3_name': '三级行业名称',
    'industry_index_code': '行业指数代码',
    'industry_index_name': '行业指数名称',
    'industry_code': '行业代码',
    'industry_level1_name': '一级行业名称',
    'industry_level2_name': '二级行业名称',
    'industry_level3_name': '三级行业名称',
    'industry_name': '行业名称',
    'in_date': '纳入日期',
    'out_date': '剔除日期',
    'weight': '权重',
}

LEGACY_ASSET_NODE_MAP = {
    'fund': {'1d': 'fund_share_real'},
    'macro': {'1d': 'treasury_yield_real'},
    'kzz': {'1d': 'kzz_conv_real'},
}

IDENTITY_FIELDS = {
    'stock_daily_real': ['code', 'trade_time'],
    'stock_industry_daily_real': ['index_code', 'trade_date'],
    'stock_minute_real': ['code', 'trade_time'],
    'fund_share_real': ['market_code', 'fund_share_ann_date'],
    'treasury_yield_real': ['treasury_term', 'treasury_trade_date'],
    'kzz_conv_real': ['market_code', 'kzz_conv_rule_ann_date'],
}

ASSET_FIELD_ALLOWLIST = {
    ('stock', '1d'): {
        'code', 'trade_time', 'open', 'high', 'low', 'close', 'volume', 'amount',
        'security_type', 'security_name', 'list_date', 'delist_date',
        'close_adj', 'open_adj', 'high_adj', 'low_adj',
        'pre_close', 'backward_adj_factor',
        'high_limited', 'low_limited',
        'is_st', 'is_suspended', 'is_wd_sec', 'is_xr_sec',
        'is_kcb', 'is_cyb', 'is_bjs',
        'market_cap', 'float_market_cap', 'turnover_rate',
        'industry_index_code', 'industry_index_name', 'industry_code',
        'industry_level1_name', 'industry_level2_name', 'industry_level3_name',
        'industry_name', 'in_date', 'out_date',
        'index_constituent_code', 'index_constituent_name', 'index_name',
    },
    ('stock', '1m'): {
        'code', 'trade_time', 'open', 'high', 'low', 'close', 'volume', 'amount',
        'minute_open', 'minute_high', 'minute_low', 'minute_close', 'minute_volume', 'minute_amount',
        'security_type', 'security_name',
    },
}

INTRADAY_DIRECT_MINUTE_FIELDS = {
    'open',
    'high',
    'low',
    'close',
    'volume',
    'amount',
}

INTRADAY_LIMIT_DERIVED_FIELDS = {
    'is_limit_up',
    'is_limit_down',
    'limit_up_price',
    'limit_down_price',
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
        loaded_nodes, self.edges = load_nodes_and_edges(self.graph_path)
        self.wide_table_specs = {
            node.name: node.wide_table
            for node in loaded_nodes
            if isinstance(node.wide_table, dict) and str(node.wide_table.get('status') or 'enabled') == 'enabled'
        }
        self.nodes = self._apply_wide_table_overlays(loaded_nodes)
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
                    'graph_node': node,
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
        universe = str(request.get('universe') or '').strip()
        fields = list(request.get('fields') or [])
        freq = request.get('freq', '1d')
        requested_asset_type = request.get('asset_type', 'auto')
        requested_node = str(request.get('provider_node') or request.get('node') or '').strip()
        issues = []

        supports_universe = universe == 'all_a' and freq == '1d'
        if universe and universe != 'all_a':
            issues.append(
                self._issue(
                    code='unsupported_universe',
                    message=f'universe={universe} is not supported',
                    path='universe',
                )
            )
        elif universe == 'all_a' and freq != '1d':
            issues.append(
                self._issue(
                    code='unsupported_universe',
                    message='universe=all_a currently only supports freq=1d',
                    path='universe',
                )
            )

        if not symbols and not supports_universe:
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
        if requested_node:
            node = self.registry.nodes.get(requested_node)
            if node is None:
                issues.append(
                    self._issue(
                        code='missing_query_node',
                        message=f'provider_node={requested_node} is not registered',
                        path='provider_node',
                    )
                )
            else:
                node_asset_type, node_freq = self._asset_freq_for_node(requested_node)
                if requested_asset_type == 'auto' and node_asset_type:
                    resolved_asset_type = node_asset_type
                if node_freq and freq != node_freq:
                    issues.append(
                        self._issue(
                            code='unsupported_freq',
                            message=f'provider_node={requested_node} supports freq={node_freq}, got freq={freq}',
                            path='freq',
                        )
                    )
        if requested_asset_type == 'auto' and supports_universe:
            resolved_asset_type = 'stock'
        elif requested_asset_type == 'auto' and symbols:
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
        known_asset_types = set(self.asset_node_map.keys()) | set(self.application_query_node_map.keys())
        if resolved_asset_type and resolved_asset_type not in known_asset_types:
            issues.append(
                self._issue(
                    code='unsupported_asset_type',
                    message=f'asset_type={resolved_asset_type} is not supported',
                    path='symbols.0' if symbols else 'asset_type',
                )
            )

        if requested_node and self.registry.nodes.get(requested_node) is not None:
            resolved_best_node = {
                'ok': True,
                'node': requested_node,
                'asset_type': resolved_asset_type,
                'freq': freq,
                'candidates': [],
                'issues': [],
            }
        else:
            resolved_best_node = self.resolve_best_node(
                symbols=symbols,
                universe=universe or None,
                fields=fields,
                freq=freq,
                asset_type=resolved_asset_type,
            )
        resolved_node = resolved_best_node.get('node')
        if not resolved_node and resolved_asset_type and not any(issue['code'] == 'unsupported_asset_type' for issue in issues):
            issues.append(
                self._issue(
                    code='missing_query_node',
                    message=f'asset_type={resolved_asset_type} has no supported {freq} query node',
                    path='freq',
                )
            )

        if requested_node and self.registry.nodes.get(requested_node) is not None:
            supported_names = {item['field_name'] for item in self._fields_for_node(requested_node)}
        else:
            supported_names = {item['name'] for item in self.get_supported_fields(asset_type=resolved_asset_type, freq=freq)['fields']}
        for field in fields:
            allowed = self._is_node_field_allowed(field, requested_node) if requested_node else self._is_field_allowed_for_asset(field, resolved_asset_type, freq)
            if field not in supported_names or not allowed:
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
        universe: str | None = None,
        fields: list[str],
        freq: str = '1d',
        asset_type: str = 'auto',
    ) -> dict[str, Any]:
        resolved_asset_type = asset_type
        issues = []
        if asset_type == 'auto' and universe == 'all_a' and freq == '1d':
            resolved_asset_type = 'stock'
        elif asset_type == 'auto' and symbols:
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

        known_asset_types = set(self.asset_node_map.keys()) | set(self.application_query_node_map.keys())
        if resolved_asset_type and resolved_asset_type not in known_asset_types:
            issues.append(
                self._issue(
                    code='unsupported_asset_type',
                    message=f'asset_type={resolved_asset_type} is not supported',
                )
            )
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
                    'priority': self._query_entry_priority(
                        node_name,
                        asset_type=str(resolved_asset_type or ''),
                        freq=freq,
                    ),
                }
            )
        scored.sort(key=lambda item: (-item['score'], -item['priority'], item['node']))
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

    def execute_query_profile(self, request: dict[str, Any]) -> dict[str, Any]:
        profile = str(request.get('query_profile') or '').strip()
        if not profile:
            return self._profile_failure_result(
                request=request,
                code='unsupported_freq',
                message='query_profile is required',
                empty_reason='validation_failed',
            )
        if profile == 'panel_time_series':
            result = self._execute_panel_time_series_profile(request)
        elif profile == 'interval_membership':
            result = self._execute_interval_membership_profile(request)
        elif profile == 'event_stream':
            result = self._execute_event_stream_profile(request)
        elif profile == 'anchored_intraday_window':
            result = self._execute_anchored_intraday_window_profile(
                symbols=list(request.get('symbols') or []),
                trading_days=list(request.get('trading_days') or []),
                start_hhmm=str(request.get('start_hhmm') or ''),
                end_hhmm=str(request.get('end_hhmm') or ''),
                hhmm_list=list(request.get('hhmm_list') or []),
                fields=list(request.get('fields') or []),
                asset_type=str(request.get('asset_type') or 'stock'),
            )
        elif profile == 'next_trading_day_window':
            result = self._execute_next_trading_day_window_profile(
                anchors=list(request.get('anchors') or []),
                start_hhmm=str(request.get('start_hhmm') or ''),
                end_hhmm=str(request.get('end_hhmm') or ''),
                hhmm_list=list(request.get('hhmm_list') or []),
                fields=list(request.get('fields') or []),
                asset_type=str(request.get('asset_type') or 'stock'),
            )
        elif profile == 'membership':
            result = self._execute_membership_profile(request)
        elif profile == 'runtime_rule':
            result = self._execute_runtime_rule_profile(request)
        else:
            result = self._profile_failure_result(
                request=request,
                code='unsupported_freq',
                message=f'unsupported query_profile: {profile}',
                empty_reason='validation_failed',
            )
        return self._attach_query_profile_meta(result, request)

    def _execute_anchored_intraday_window_profile(
        self,
        *,
        symbols: list[str],
        trading_days: list[str],
        start_hhmm: str,
        end_hhmm: str,
        fields: list[str],
        asset_type: str = 'stock',
        hhmm_list: list[str] | None = None,
    ) -> dict[str, Any]:
        started = perf_counter()
        request = {
            'symbols': list(symbols or []),
            'trading_days': list(trading_days or []),
            'start_hhmm': start_hhmm,
            'end_hhmm': end_hhmm,
            'hhmm_list': list(hhmm_list or []),
            'fields': list(fields or []),
            'asset_type': asset_type,
        }
        validation = self._validate_intraday_window_request(request)
        if not validation['ok']:
            return self._intraday_failure_result(
                request=request,
                validation=validation,
                issues=validation['issues'],
                elapsed_ms=int((perf_counter() - started) * 1000),
                empty_reason='validation_failed',
                row_count=0,
                symbol_count=len(validation['resolved'].get('symbols') or list(symbols or [])),
                trading_day_count=len(validation['resolved'].get('trading_days') or list(trading_days or [])),
                query_count=0,
            )

        resolved = validation['resolved']
        requested_fields = list(resolved['fields'])
        symbols = list(resolved['symbols'])
        trading_days = list(resolved['trading_days'])
        hhmm_list = list(resolved.get('hhmm_list') or [])
        minute_fields = list(resolved['minute_query_fields'])
        direct_fields = list(resolved['direct_fields'])
        limit_fields = list(resolved['limit_fields'])
        daily_limit_fields = list(resolved['daily_limit_fields'])

        frames: list[pd.DataFrame] = []
        sqls: list[str] = []
        query_attempts: list[dict[str, Any]] = []
        query_issues: list[dict[str, Any]] = []
        missing_trading_days: list[str] = []

        if hhmm_list:
            batch_frames, batch_sqls, batch_attempts, batch_issues = self._query_intraday_exact_points_batches(
                node_name=resolved['node'],
                symbols=symbols,
                trading_days=trading_days,
                hhmm_list=hhmm_list,
                fields=minute_fields,
            )
            frames.extend(batch_frames)
            sqls.extend(batch_sqls)
            query_attempts.extend(batch_attempts)
            query_issues.extend(batch_issues)
        else:
            for trading_day in trading_days:
                day_start = f'{trading_day} {resolved["start_hhmm"]}:00'
                day_end = f'{trading_day} {resolved["end_hhmm"]}:00'
                day_result = self._execute_query_request(
                    {
                        'symbols': symbols,
                        'universe': None,
                        'fields': minute_fields,
                        'start': day_start,
                        'end': day_end,
                        'asset_type': resolved['asset_type'],
                        'freq': '1m',
                    },
                    page_size=None,
                )
                day_debug = day_result.get('debug') or {}
                sqls.append(str(day_debug.get('sql') or ''))
                query_attempts.append(
                    {
                        'trading_day': trading_day,
                        'ok': bool(day_result.get('ok')),
                        'issues': list(day_result.get('issues') or []),
                        'intent': day_debug.get('intent'),
                        'sql': day_debug.get('sql'),
                    }
                )
                if day_result.get('ok'):
                    day_df = day_result.get('df')
                    if isinstance(day_df, pd.DataFrame) and not day_df.empty:
                        frames.append(day_df.copy())
                        continue

                day_issue_codes = {issue.get('code') for issue in day_result.get('issues') or [] if isinstance(issue, dict)}
                if day_issue_codes and day_issue_codes.issubset({'empty_result'}):
                    missing_trading_days.append(trading_day)
                    continue
                if not day_issue_codes and not day_result.get('ok'):
                    query_issues.append(
                        self._issue(
                            code='query_failed',
                            message=f'intraday minute query failed for trading_day={trading_day}',
                            path='query',
                        )
                    )
                    continue
                query_issues.extend(list(day_result.get('issues') or []))

        if query_issues and not frames:
            return self._intraday_failure_result(
                request=request,
                validation=validation,
                issues=query_issues,
                elapsed_ms=int((perf_counter() - started) * 1000),
                empty_reason='query_failed',
                row_count=0,
                symbol_count=len(symbols),
                trading_day_count=len(trading_days),
                query_count=len(query_attempts),
                extra_meta={'missing_trading_days': missing_trading_days},
                debug_extra={'queries': query_attempts},
                sqls=sqls,
            )

        if not frames:
            return self._intraday_failure_result(
                request=request,
                validation=validation,
                issues=[
                    self._issue(
                        code='empty_result',
                        message='intraday minute query succeeded but returned no rows',
                        path='query',
                    )
                ],
                elapsed_ms=int((perf_counter() - started) * 1000),
                empty_reason='no_rows',
                row_count=0,
                symbol_count=len(symbols),
                trading_day_count=len(trading_days),
                query_count=len(query_attempts),
                extra_meta={'missing_trading_days': missing_trading_days},
                debug_extra={'queries': query_attempts},
                sqls=sqls,
            )

        df = pd.concat(frames, ignore_index=True)
        df = self._normalize_intraday_trade_time(df)
        if hhmm_list:
            observed_days = set(df['trade_date'].dropna().astype(str).tolist()) if 'trade_date' in df.columns else set()
            missing_trading_days = [trading_day for trading_day in trading_days if trading_day not in observed_days]
        if daily_limit_fields:
            limit_bundle = self._fetch_intraday_limit_frame(
                symbols=symbols,
                trading_days=trading_days,
                daily_fields=daily_limit_fields,
            )
            sqls.extend(limit_bundle.get('sqls') or [])
            if limit_bundle['ok']:
                daily_df = limit_bundle['df']
                if not daily_df.empty:
                    df = self._merge_intraday_limit_frame(df, daily_df)
            else:
                fatal_codes = {issue.get('code') for issue in limit_bundle.get('issues') or [] if isinstance(issue, dict)}
                if fatal_codes and not fatal_codes.issubset({'empty_result'}):
                    return self._intraday_failure_result(
                        request=request,
                        validation=validation,
                        issues=list(limit_bundle.get('issues') or []),
                        elapsed_ms=int((perf_counter() - started) * 1000),
                        empty_reason='query_failed',
                        row_count=0,
                        symbol_count=len(symbols),
                        trading_day_count=len(trading_days),
                        query_count=len(query_attempts),
                        extra_meta={'missing_trading_days': missing_trading_days},
                        debug_extra={'queries': query_attempts},
                        sqls=sqls,
                    )
            df = self._apply_intraday_limit_fields(df, limit_fields)

        ordered_fields = list(dict.fromkeys(['code', 'trade_time'] + direct_fields + limit_fields))
        df = df[[field for field in ordered_fields if field in df.columns]]
        df = df.sort_values(['code', 'trade_time']).reset_index(drop=True)

        return {
            'ok': True,
            'df': df,
            'issues': [],
            'meta': {
                'asset_type': resolved['asset_type'],
                'freq': '1m',
                'node': resolved['node'],
                'fields': requested_fields,
                'row_count': int(len(df)),
                'symbol_count': len(symbols),
                'trading_day_count': len(trading_days),
                'missing_trading_days': missing_trading_days,
                'empty': False,
                'empty_reason': None,
                'elapsed_ms': int((perf_counter() - started) * 1000),
                'query_count': len(query_attempts),
            },
            'debug': {
                'request': request,
                'validation': validation,
                'resolved': resolved,
                'intent': {
                    'kind': 'minute_window_by_trading_day',
                    'asset_type': resolved['asset_type'],
                    'node': resolved['node'],
                    'symbols': symbols,
                    'trading_days': trading_days,
                    'start_hhmm': resolved['start_hhmm'],
                    'end_hhmm': resolved['end_hhmm'],
                    'hhmm_list': hhmm_list,
                    'fields': requested_fields,
                    'minute_query_fields': minute_fields,
                    'daily_limit_fields': daily_limit_fields,
                },
                'sql': ';\n\n'.join(sql for sql in sqls if sql),
                'sqls': [sql for sql in sqls if sql],
                'queries': query_attempts,
            },
        }

    def _execute_next_trading_day_window_profile(
        self,
        *,
        anchors: list[dict[str, Any]],
        start_hhmm: str,
        end_hhmm: str,
        fields: list[str],
        asset_type: str = 'stock',
        hhmm_list: list[str] | None = None,
    ) -> dict[str, Any]:
        started = perf_counter()
        request = {
            'anchors': list(anchors or []),
            'start_hhmm': start_hhmm,
            'end_hhmm': end_hhmm,
            'hhmm_list': list(hhmm_list or []),
            'fields': list(fields or []),
            'asset_type': asset_type,
        }
        anchor_bundle = self._resolve_intraday_anchors(list(anchors or []))
        if not anchor_bundle['ok']:
            validation = {
                'ok': False,
                'issues': list(anchor_bundle['issues']),
                'resolved': {
                    'asset_type': asset_type,
                    'node': self.application_query_node_map.get(asset_type, {}).get('1m'),
                    'fields': list(fields or []),
                    'anchors': [],
                },
            }
            return self._intraday_failure_result(
                request=request,
                validation=validation,
                issues=list(anchor_bundle['issues']),
                elapsed_ms=int((perf_counter() - started) * 1000),
                empty_reason='validation_failed',
                row_count=0,
                symbol_count=0,
                trading_day_count=0,
                query_count=0,
            )

        resolved_anchors = list(anchor_bundle['anchors'])
        base_result = self._execute_anchored_intraday_window_profile(
            symbols=sorted({item['code'] for item in resolved_anchors}),
            trading_days=sorted({item['execution_date'] for item in resolved_anchors}),
            start_hhmm=start_hhmm,
            end_hhmm=end_hhmm,
            hhmm_list=hhmm_list,
            fields=fields,
            asset_type=asset_type,
        )
        df = base_result.get('df')
        if isinstance(df, pd.DataFrame) and not df.empty:
            enriched_df = df.copy()
            if 'execution_date' not in enriched_df.columns:
                trade_time = pd.to_datetime(enriched_df['trade_time'], errors='coerce')
                enriched_df['execution_date'] = trade_time.dt.date.astype(str)
            available_pairs = {
                (str(row.get('code')), str(row.get('execution_date')))
                for row in enriched_df[['code', 'execution_date']].to_dict(orient='records')
            }
            missing_anchor_ids = [
                item['anchor_id']
                for item in resolved_anchors
                if (item['code'], item['execution_date']) not in available_pairs
            ]
            base_result['df'] = enriched_df
            base_result['meta'] = {
                **dict(base_result.get('meta') or {}),
                'anchor_count': len(resolved_anchors),
                'missing_anchor_count': len(missing_anchor_ids),
                'missing_anchor_ids': missing_anchor_ids,
            }
            base_debug = dict(base_result.get('debug') or {})
            base_debug['request'] = request
            base_debug['intent'] = {
                'kind': 'next_trading_day_intraday_windows',
                'asset_type': asset_type,
                'anchor_count': len(resolved_anchors),
                'start_hhmm': start_hhmm,
                'end_hhmm': end_hhmm,
                'hhmm_list': list(hhmm_list or []),
                'fields': list(fields or []),
                'anchors': resolved_anchors,
                'base_intent': (base_result.get('debug') or {}).get('intent'),
            }
            base_debug['resolved_anchors'] = resolved_anchors
            base_result['debug'] = base_debug
            return base_result

        base_meta = dict(base_result.get('meta') or {})
        base_meta.update(
            {
                'anchor_count': len(resolved_anchors),
                'missing_anchor_count': len(resolved_anchors),
                'missing_anchor_ids': [item['anchor_id'] for item in resolved_anchors],
            }
        )
        base_result['meta'] = base_meta
        base_debug = dict(base_result.get('debug') or {})
        base_debug['request'] = request
        base_debug['resolved_anchors'] = resolved_anchors
        base_debug['intent'] = {
            'kind': 'next_trading_day_intraday_windows',
            'asset_type': asset_type,
            'anchor_count': len(resolved_anchors),
            'start_hhmm': start_hhmm,
            'end_hhmm': end_hhmm,
            'hhmm_list': list(hhmm_list or []),
            'fields': list(fields or []),
            'anchors': resolved_anchors,
            'base_intent': (base_result.get('debug') or {}).get('intent'),
        }
        base_result['debug'] = base_debug
        return base_result

    def _query_intraday_exact_points_batches(
        self,
        *,
        node_name: str,
        symbols: list[str],
        trading_days: list[str],
        hhmm_list: list[str],
        fields: list[str],
    ) -> tuple[list[pd.DataFrame], list[str], list[dict[str, Any]], list[dict[str, Any]]]:
        frames: list[pd.DataFrame] = []
        sqls: list[str] = []
        attempts: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []
        for index in range(0, len(trading_days), MAX_INTRADAY_POINT_BATCH_DAYS):
            day_chunk = trading_days[index : index + MAX_INTRADAY_POINT_BATCH_DAYS]
            sql = self._build_intraday_exact_points_sql(
                node_name=node_name,
                symbols=symbols,
                trading_days=day_chunk,
                hhmm_list=hhmm_list,
                fields=fields,
            )
            result = self.execute_sql(sql)
            attempts.append(
                {
                    'trading_day_count': len(day_chunk),
                    'ok': bool(result.get('code') == SUCCESS_CODE),
                    'hhmm_list': list(hhmm_list),
                    'sql': sql,
                    'intent': {
                        'kind': 'minute_window_by_trading_day_hhmm_list',
                        'node': node_name,
                        'symbols': symbols,
                        'trading_days': day_chunk,
                        'hhmm_list': hhmm_list,
                        'fields': fields,
                    },
                }
            )
            sqls.append(sql)
            if result.get('code') != SUCCESS_CODE:
                issues.append(
                    self._issue(
                        code='query_failed',
                        message=f'intraday exact-point query failed for trading_days={day_chunk[0]}..{day_chunk[-1]}',
                        path='query',
                    )
                )
                continue
            df = result.get('df')
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df.copy())
        return frames, sqls, attempts, issues

    def _build_intraday_exact_points_sql(
        self,
        *,
        node_name: str,
        symbols: list[str],
        trading_days: list[str],
        hhmm_list: list[str],
        fields: list[str],
    ) -> str:
        node = self.registry.nodes.get(node_name)
        table_name = node.table if node is not None else node_name
        identity_fields = self._identity_fields_for_node(node_name)
        symbol_field = identity_fields[0] if identity_fields else 'code'
        time_field = identity_fields[1] if len(identity_fields) > 1 else 'trade_time'
        select_fields = list(dict.fromkeys([symbol_field, time_field] + list(fields or [])))
        symbol_sql = ', '.join(self._quote_sql_string(item) for item in symbols)
        timestamp_sql = ', '.join(
            f"toDateTime({self._quote_sql_string(f'{trading_day} {hhmm}:00')})"
            for trading_day in trading_days
            for hhmm in hhmm_list
        )
        select_sql = ', '.join(select_fields)
        return (
            f"SELECT {select_sql} "
            f"FROM {table_name} "
            f"WHERE {symbol_field} IN ({symbol_sql}) "
            f"AND {time_field} IN ({timestamp_sql}) "
            f"ORDER BY {symbol_field}, {time_field}"
        )

    def _execute_panel_time_series_profile(self, request: dict[str, Any]) -> dict[str, Any]:
        profile_request = {
            'symbols': list(request.get('symbols') or []),
            'universe': request.get('universe'),
            'fields': list(request.get('fields') or []),
            'start': request.get('start'),
            'end': request.get('end'),
            'freq': request.get('freq') or ('1m' if request.get('minute') else '1d'),
            'asset_type': request.get('asset_type') or 'auto',
            'memberships': request.get('memberships') or None,
            'membership_path': request.get('membership_path'),
            'provider_node': request.get('provider_node') or request.get('node'),
            'query_profile': request.get('query_profile'),
            'capability': request.get('capability'),
            'where': request.get('where'),
        }
        return self._execute_query_request(profile_request, page_size=request.get('page_size'))

    def _execute_event_stream_profile(self, request: dict[str, Any]) -> dict[str, Any]:
        started = perf_counter()
        provider_node = str(request.get('provider_node') or request.get('node') or '').strip()
        node = self.registry.nodes.get(provider_node)
        if node is None:
            return self._profile_failure_result(
                request=request,
                code='missing_query_node',
                message=f'provider_node={provider_node or None} is not registered',
                empty_reason='validation_failed',
                elapsed_ms=int((perf_counter() - started) * 1000),
            )

        key_schema = dict(request.get('key_schema') or {})
        symbol_key = str(key_schema.get('symbol') or (node.entity_keys[0] if node.entity_keys else 'code')).strip()
        event_time_key = str(key_schema.get('event_time') or key_schema.get('time') or node.time_key or 'trade_time').strip()
        publish_time_key = str(key_schema.get('publish_time') or '').strip()
        symbols = sorted({str(item).strip() for item in list(request.get('symbols') or []) if str(item).strip()})
        fields = [str(item).strip() for item in list(request.get('fields') or []) if str(item).strip()]
        field_map = dict(request.get('requested_field_map') or request.get('field_map') or {})
        if not fields:
            fields = list(field_map.keys())
        if not symbols:
            return self._profile_failure_result(
                request=request,
                code='missing_symbols',
                message='symbols is required for event_stream query',
                empty_reason='validation_failed',
                elapsed_ms=int((perf_counter() - started) * 1000),
            )
        if not request.get('start') or not request.get('end'):
            return self._profile_failure_result(
                request=request,
                code='missing_execution_date',
                message='start/end is required for event_stream query',
                empty_reason='validation_failed',
                elapsed_ms=int((perf_counter() - started) * 1000),
            )

        select_parts = [f'{symbol_key} AS code']
        selected_aliases = {'code'}
        for field in fields:
            source = field_map.get(field, field)
            if isinstance(source, dict):
                source = source.get('source_field') or source.get('field') or field
            source = str(source or field).strip()
            if field in selected_aliases:
                continue
            select_parts.append(f'{source} AS {field}' if source != field else source)
            selected_aliases.add(field)

        symbol_sql = ', '.join(self._quote_sql_string(symbol) for symbol in symbols)
        start_sql = self._quote_sql_string(str(request.get('start')).strip())
        end_sql = self._quote_sql_string(str(request.get('end')).strip())
        where_parts = [
            f'{symbol_key} IN ({symbol_sql})',
            f'{event_time_key} IS NOT NULL',
            f'{event_time_key} BETWEEN toDate({start_sql}) AND toDate({end_sql})',
        ]
        filters = dict(request.get('filters') or {})
        finalized_filter = filters.get('finalized') if isinstance(filters.get('finalized'), dict) else None
        if bool(request.get('finalized_only', True)) and finalized_filter:
            field = str(finalized_filter.get('field') or '').strip()
            value = finalized_filter.get('value')
            if field:
                where_parts.append(f'{field} = {self._sql_literal(value)}')

        order_parts = [symbol_key, event_time_key]
        if publish_time_key:
            order_parts.append(publish_time_key)
        sql = (
            'SELECT '
            + ', '.join(select_parts)
            + f' FROM {node.table} WHERE '
            + ' AND '.join(where_parts)
            + ' ORDER BY '
            + ', '.join(order_parts)
        )
        execute_started = perf_counter()
        result = self.execute_sql(sql)
        execute_finished = perf_counter()
        df = result.get('df')
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame()
        if not df.empty:
            normalized = df.copy()
            for column in normalized.columns:
                if column.endswith('_date') or column in {'event_time', 'publish_time', 'payout_time'}:
                    series = pd.to_datetime(normalized[column], errors='coerce')
                    normalized[column] = series.dt.date.astype('string')
            df = normalized
        issues: list[dict[str, Any]] = []
        empty = False
        empty_reason = None
        if result.get('code') != SUCCESS_CODE:
            issues.append(self._issue(code='query_failed', message=str(result.get('message') or 'event_stream query failed'), path='query'))
            empty = True
            empty_reason = 'query_failed'
        elif df.empty:
            issues.append(self._issue(code='empty_result', message='event_stream query succeeded but returned no rows', path='query'))
            empty = True
            empty_reason = 'no_rows'
        elapsed_ms = int((perf_counter() - started) * 1000)
        query_timings = {
            'execute_seconds': round(execute_finished - execute_started, 6),
            'total_seconds': round(perf_counter() - started, 6),
        }
        executor_stats = result.get('statistics') or {}
        if executor_stats:
            query_timings['executor'] = executor_stats
        meta = {
            'query_profile': request.get('query_profile'),
            'capability': request.get('capability'),
            'provider_node': provider_node,
            'node': provider_node,
            'fields': fields,
            'row_count': int(len(df.index)),
            'symbol_count': len(symbols),
            'empty': empty,
            'empty_reason': empty_reason,
            'elapsed_ms': elapsed_ms,
            'timings': query_timings,
        }
        return {
            'ok': result.get('code') == SUCCESS_CODE and not issues,
            'df': df,
            'issues': issues,
            'meta': meta,
            'debug': {
                'request': request,
                'resolved': {
                    'provider_node': provider_node,
                    'symbol_key': symbol_key,
                    'event_time_key': event_time_key,
                    'field_map': field_map,
                },
                'intent': {
                    'query_profile': request.get('query_profile'),
                    'provider_node': provider_node,
                    'fields': fields,
                },
                'sql': sql,
                'timings': query_timings,
            },
        }

    def _execute_interval_membership_profile(self, request: dict[str, Any]) -> dict[str, Any]:
        started = perf_counter()
        provider_node = str(request.get('provider_node') or request.get('node') or '').strip()
        node = self.registry.nodes.get(provider_node)
        if node is None:
            return self._profile_failure_result(
                request=request,
                code='missing_query_node',
                message=f'provider_node={provider_node or None} is not registered',
                empty_reason='validation_failed',
                elapsed_ms=int((perf_counter() - started) * 1000),
            )

        key_schema = dict(request.get('key_schema') or {})
        symbol_key = str(key_schema.get('symbol') or (node.entity_keys[0] if node.entity_keys else 'code')).strip()
        start_key = str(key_schema.get('start') or node.time_key or 'in_date').strip()
        end_key = str(key_schema.get('end') or 'out_date').strip()
        symbols = sorted({str(item).strip() for item in list(request.get('symbols') or []) if str(item).strip()})
        fields = [str(item).strip() for item in list(request.get('fields') or []) if str(item).strip()]
        if not request.get('start') or not request.get('end'):
            return self._profile_failure_result(
                request=request,
                code='missing_execution_date',
                message='start/end is required for interval_membership query',
                empty_reason='validation_failed',
                elapsed_ms=int((perf_counter() - started) * 1000),
            )

        select_parts = [
            f'{symbol_key} AS code',
            f'{start_key} AS interval_start',
            f'{end_key} AS interval_end',
        ]
        selected_aliases = {'code', 'interval_start', 'interval_end'}
        for field in fields:
            if field in selected_aliases:
                continue
            select_parts.append(field)
            selected_aliases.add(field)

        start_sql = self._quote_sql_string(str(request.get('start')).strip())
        end_sql = self._quote_sql_string(str(request.get('end')).strip())
        where_parts = [
            f'{start_key} <= toDate({end_sql})',
            f'({end_key} IS NULL OR {end_key} > toDate({start_sql}))',
        ]
        if symbols:
            symbol_sql = ', '.join(self._quote_sql_string(symbol) for symbol in symbols)
            where_parts.append(f'{symbol_key} IN ({symbol_sql})')
        for item in self._query_request_where_items(request.get('where')):
            rendered = self._render_simple_where_item(item)
            if rendered:
                where_parts.append(rendered)

        sql = (
            'SELECT '
            + ', '.join(select_parts)
            + f' FROM {node.table} WHERE '
            + ' AND '.join(where_parts)
            + f' ORDER BY {symbol_key}, {start_key}'
        )
        execute_started = perf_counter()
        result = self.execute_sql(sql)
        execute_finished = perf_counter()
        df = result.get('df')
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame()
        issues: list[dict[str, Any]] = []
        empty = False
        empty_reason = None
        if result.get('code') != SUCCESS_CODE:
            issues.append(self._issue(code='query_failed', message=str(result.get('message') or 'interval_membership query failed'), path='query'))
            empty = True
            empty_reason = 'query_failed'
        elif df.empty:
            issues.append(self._issue(code='empty_result', message='interval_membership query succeeded but returned no rows', path='query'))
            empty = True
            empty_reason = 'no_rows'

        elapsed_ms = int((perf_counter() - started) * 1000)
        query_timings = {
            'execute_seconds': round(execute_finished - execute_started, 6),
            'total_seconds': round(perf_counter() - started, 6),
        }
        executor_stats = result.get('statistics') or {}
        if executor_stats:
            query_timings['executor'] = executor_stats
        meta = {
            'query_profile': request.get('query_profile'),
            'capability': request.get('capability'),
            'provider_node': provider_node,
            'node': provider_node,
            'fields': fields,
            'row_count': int(len(df.index)),
            'symbol_count': len(symbols),
            'empty': empty,
            'empty_reason': empty_reason,
            'elapsed_ms': elapsed_ms,
            'timings': query_timings,
        }
        return {
            'ok': result.get('code') == SUCCESS_CODE and not issues,
            'df': df,
            'issues': issues,
            'meta': meta,
            'debug': {
                'request': request,
                'resolved': {
                    'provider_node': provider_node,
                    'symbol_key': symbol_key,
                    'start_key': start_key,
                    'end_key': end_key,
                },
                'intent': {
                    'query_profile': request.get('query_profile'),
                    'provider_node': provider_node,
                    'fields': fields,
                    'where': request.get('where'),
                },
                'sql': sql,
                'timings': query_timings,
            },
        }

    def _render_simple_where_item(self, item: dict[str, Any]) -> str:
        field = str(item.get('field') or '').strip()
        op = str(item.get('op') or '').strip().lower()
        value = item.get('value')
        if not field or value is None:
            return ''
        if op in {'=', '!=', '<>', '>', '>=', '<', '<='}:
            return f'{field} {op} {self._sql_literal(value)}'
        if op in {'in', 'not in'}:
            values = value if isinstance(value, list) else [value]
            values = [entry for entry in values if entry is not None]
            if not values:
                return ''
            sql_values = ', '.join(self._sql_literal(entry) for entry in values)
            operator = 'IN' if op == 'in' else 'NOT IN'
            return f'{field} {operator} ({sql_values})'
        return ''

    def _execute_membership_profile(self, request: dict[str, Any]) -> dict[str, Any]:
        operation = str(request.get('operation') or 'filter_symbols').strip()
        if operation == 'resolve_target':
            item = membership_resolve_target(
                domain=str(request.get('domain') or ''),
                member_code=str(request.get('member_code') or ''),
                taxonomy=request.get('taxonomy'),
                member_name=request.get('member_name'),
                path=request.get('membership_path') or DEFAULT_MEMBERSHIP_PATH,
            )
            return {'ok': True, 'df': pd.DataFrame(), 'issues': [], 'meta': {'query_profile': 'membership', 'row_count': 0, 'empty': False}, 'debug': {'request': request, 'item': item}, 'item': item}
        if operation == 'query_membership':
            items = membership_query_membership(
                str(request.get('security_code') or ''),
                as_of_date=str(request.get('as_of_date') or ''),
                security_type=request.get('security_type'),
                path=request.get('membership_path') or DEFAULT_MEMBERSHIP_PATH,
                executor=self.executor,
            )
            return {'ok': True, 'df': pd.DataFrame(items), 'issues': [], 'meta': {'query_profile': 'membership', 'row_count': len(items), 'empty': not bool(items)}, 'debug': {'request': request}, 'items': items}
        result = membership_filter_symbols(
            dict(request.get('memberships') or {}),
            as_of_date=str(request.get('as_of_date') or ''),
            path=request.get('membership_path') or DEFAULT_MEMBERSHIP_PATH,
            security_type=request.get('security_type'),
            executor=self.executor,
        )
        frame = pd.DataFrame({'code': list(result.get('symbols') or [])})
        return {'ok': True, 'df': frame, 'issues': [], 'meta': {'query_profile': 'membership', 'row_count': len(frame), 'empty': frame.empty}, 'debug': {'request': request}, **result}

    def _execute_runtime_rule_profile(self, request: dict[str, Any]) -> dict[str, Any]:
        rules = dict(request.get('rules') or request.get('fields') or {})
        return {
            'ok': True,
            'df': pd.DataFrame(),
            'issues': [],
            'meta': {
                'query_profile': 'runtime_rule',
                'capability': request.get('capability'),
                'row_count': 0,
                'empty': False,
            },
            'debug': {'request': request},
            'rules': rules,
        }

    def _attach_query_profile_meta(self, result: dict[str, Any], request: dict[str, Any]) -> dict[str, Any]:
        meta = dict(result.get('meta') or {})
        meta.setdefault('query_profile', request.get('query_profile'))
        if request.get('capability') is not None:
            meta.setdefault('capability', request.get('capability'))
        provider_node = request.get('provider_node') or request.get('node') or meta.get('node')
        if provider_node is not None:
            meta.setdefault('provider_node', provider_node)
        result['meta'] = meta
        debug = dict(result.get('debug') or {})
        debug.setdefault('request', request)
        result['debug'] = debug
        return result

    def _profile_failure_result(
        self,
        *,
        request: dict[str, Any],
        code: str,
        message: str,
        empty_reason: str,
        elapsed_ms: int = 0,
    ) -> dict[str, Any]:
        return {
            'ok': False,
            'df': pd.DataFrame(),
            'issues': [self._issue(code=code, message=message, path='query_profile')],
            'meta': {
                'query_profile': request.get('query_profile'),
                'capability': request.get('capability'),
                'provider_node': request.get('provider_node') or request.get('node'),
                'row_count': 0,
                'symbol_count': len(list(request.get('symbols') or [])),
                'empty': True,
                'empty_reason': empty_reason,
                'elapsed_ms': elapsed_ms,
            },
            'debug': {'request': request, 'sql': ''},
        }

    def _execute_query_request(self, request: dict[str, Any], *, page_size: int | None) -> dict[str, Any]:
        started = perf_counter()
        membership_started = perf_counter()
        membership_bundle = self._resolve_membership_request(request)
        membership_finished = perf_counter()
        if membership_bundle.get('failed'):
            validation = {
                'ok': False,
                'issues': list(membership_bundle['issues']),
                'resolved': {
                    'asset_type': request.get('asset_type'),
                    'node': None,
                },
            }
            return self._query_failure_result(
                request=request,
                validation=validation,
                issues=membership_bundle['issues'],
                elapsed_ms=int((perf_counter() - started) * 1000),
                empty_reason=membership_bundle.get('empty_reason') or 'validation_failed',
                extra_meta={
                    'timings': {
                        'membership_seconds': round(membership_finished - membership_started, 6),
                        'total_seconds': round(perf_counter() - started, 6),
                    }
                },
            )

        request = membership_bundle.get('request') or request
        symbols = list(request.get('symbols') or [])
        fields = list(request.get('fields') or [])
        validation_started = perf_counter()
        validation = self.validate_query_request(request)
        validation_finished = perf_counter()
        if not validation['ok']:
            return self._query_failure_result(
                request=request,
                validation=validation,
                issues=validation['issues'],
                elapsed_ms=int((perf_counter() - started) * 1000),
                empty_reason='validation_failed',
                extra_meta={
                    'timings': {
                        'membership_seconds': round(membership_finished - membership_started, 6),
                        'validation_seconds': round(validation_finished - validation_started, 6),
                        'total_seconds': round(perf_counter() - started, 6),
                    }
                },
            )

        node = validation['resolved']['node']
        identity_fields = self._identity_fields_for_node(node)
        select_fields = list(dict.fromkeys(identity_fields + fields))
        symbol_field = identity_fields[0] if identity_fields else 'code'
        time_field = identity_fields[1] if len(identity_fields) > 1 else 'trade_time'
        where_items = (
            [{'field': symbol_field, 'op': 'in' if len(symbols) > 1 else '=', 'value': (symbols if len(symbols) > 1 else symbols[0])}]
            if symbols
            else []
        )
        where_items.extend(self._query_request_where_items(request.get('where')))
        intent = {
            'from': node,
            'select': select_fields,
            'where': {
                'mode': 'and',
                'items': where_items,
            },
            'time_range': {
                'field': time_field,
                'start': request.get('start'),
                'end': request.get('end'),
            },
            'safety': {
                'lookahead_safe': False,
                'strict_mode': True,
            },
        }
        if page_size is not None:
            intent['page'] = 1
            intent['page_size'] = page_size
        execute_started = perf_counter()
        result = self.execute_intent(intent)
        execute_finished = perf_counter()
        elapsed_ms = int((perf_counter() - started) * 1000)
        issues = []
        empty_reason = None
        empty = False
        if result['code'] != 0:
            issues.append(
                self._issue(
                    code='query_failed',
                    message=f"query failed for node={node} asset_type={validation['resolved']['asset_type']}",
                    path='query',
                )
            )
        elif result['df'].empty:
            empty = True
            empty_reason = 'no_rows'
            issues.append(
                self._issue(
                    code='empty_result',
                    message=f"query succeeded but returned no rows for node={node} asset_type={validation['resolved']['asset_type']}",
                    path='query',
                )
            )

        query_timings = {
            'membership_seconds': round(membership_finished - membership_started, 6),
            'validation_seconds': round(validation_finished - validation_started, 6),
            'execute_seconds': round(execute_finished - execute_started, 6),
            'total_seconds': round(perf_counter() - started, 6),
        }
        executor_stats = result.get('statistics') or {}
        if executor_stats:
            query_timings['executor'] = executor_stats

        meta = self._build_query_meta(
            validation=validation,
            request=request,
            row_count=int(len(result['df'])),
            symbol_count=len(symbols),
            empty=empty,
            elapsed_ms=elapsed_ms,
            empty_reason=empty_reason,
        )
        meta['timings'] = query_timings

        debug = self._build_debug_bundle(
            request=request,
            validation=validation,
            intent=intent,
            sql=result['sql'],
        )
        debug['timings'] = query_timings
        membership_info = {
            'applied': bool(request.get('memberships')),
            'resolved_symbol_count': len(symbols),
        }
        if membership_bundle.get('request') is not None:
            membership_info['resolved_request'] = membership_bundle.get('request')
        debug['membership'] = membership_info

        return {
            'ok': result['code'] == 0 and not issues,
            'df': result['df'],
            'issues': issues,
            'meta': meta,
            'debug': debug,
        }

    def _query_request_where_items(self, where: Any) -> list[dict[str, Any]]:
        if not where:
            return []
        raw_items: list[Any]
        if isinstance(where, dict):
            raw_items = list(where.get('items') or [])
        elif isinstance(where, list):
            raw_items = list(where)
        else:
            return []

        items: list[dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            field = str(item.get('field') or '').strip()
            op = str(item.get('op') or '').strip().lower()
            if op in {'eq', '=='}:
                op = '='
            if not field or op not in {'=', '!=', '<>', '>', '>=', '<', '<=', 'in', 'not in'}:
                continue
            value = item.get('value')
            if value is None:
                continue
            if op in {'in', 'not in'}:
                if not isinstance(value, list):
                    value = [value]
                value = [entry for entry in value if entry is not None]
                if not value:
                    continue
            items.append({'field': field, 'op': op, 'value': value})
        return items

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
        started = perf_counter()
        built = self.build_intent_from_requirement(data_requirement)
        if not built['ok']:
            return {
                'ok': False,
                'df': pd.DataFrame(),
                'issues': built['issues'],
                'meta': {
                    'asset_type': built['resolved'].get('asset_type'),
                    'node': built['resolved'].get('node'),
                    'fields': list(data_requirement.get('fields') or []),
                    'row_count': 0,
                    'symbol_count': len(list((data_requirement.get('scope') or {}).get('symbols') or [])),
                    'empty': True,
                    'empty_reason': 'validation_failed',
                    'elapsed_ms': int((perf_counter() - started) * 1000),
                },
                'resolved': built['resolved'],
                'debug': {
                    'request': data_requirement,
                    'validation': {
                        'issues': built['issues'],
                        'resolved': built['resolved'],
                    },
                    'resolved': built['resolved'],
                    'intent': None,
                    'sql': '',
                },
            }

        result = self.execute_intent(built['intent'])
        issues = []
        empty = False
        if result['code'] != 0:
            issues.append(
                self._issue(
                    code='query_failed',
                    message=f"query execution failed for node={built['resolved'].get('node')}",
                    path='query',
                )
            )
        elif result['df'].empty:
            empty = True
            issues.append(
                self._issue(
                    code='empty_result',
                    message=f"query succeeded but returned no rows for node={built['resolved'].get('node')} asset_type={built['resolved'].get('asset_type')}",
                    path='query',
                )
            )
        return {
            'ok': result['code'] == 0 and not issues,
            'df': result['df'],
            'issues': issues,
            'meta': {
                'asset_type': built['resolved'].get('asset_type'),
                'node': built['resolved'].get('node'),
                'fields': list(data_requirement.get('fields') or []),
                'row_count': int(len(result['df'])),
                'symbol_count': len(list((data_requirement.get('scope') or {}).get('symbols') or [])),
                'empty': empty,
                'empty_reason': 'no_rows' if result['df'].empty else None,
                'elapsed_ms': int((perf_counter() - started) * 1000),
            },
            'resolved': built['resolved'],
            'debug': {
                'request': data_requirement,
                'validation': {
                    'issues': [],
                    'resolved': built['resolved'],
                },
                'resolved': built['resolved'],
                'intent': built['intent'],
                'sql': result['sql'],
            },
        }

    def _build_query_meta(
        self,
        *,
        validation: dict[str, Any],
        request: dict[str, Any],
        row_count: int,
        symbol_count: int,
        empty: bool,
        elapsed_ms: int,
        empty_reason: str | None,
    ) -> dict[str, Any]:
        return {
            'asset_type': validation['resolved'].get('asset_type'),
            'node': validation['resolved'].get('node'),
            'fields': list(request.get('fields') or []),
            'row_count': row_count,
            'symbol_count': symbol_count,
            'empty': empty,
            'empty_reason': empty_reason,
            'elapsed_ms': elapsed_ms,
        }

    def _build_debug_bundle(
        self,
        *,
        request: dict[str, Any],
        validation: dict[str, Any],
        intent: dict[str, Any] | None,
        sql: str,
    ) -> dict[str, Any]:
        return {
            'request': request,
            'validation': validation,
            'resolved': validation.get('resolved', {}),
            'intent': intent,
            'sql': sql,
        }

    def _resolve_membership_request(self, request: dict[str, Any]) -> dict[str, Any]:
        memberships = request.get('memberships') or None
        if not memberships:
            return {'request': request, 'failed': False}

        freq = str(request.get('freq') or '1d')
        if freq != '1d':
            return {
                'failed': True,
                'empty_reason': 'validation_failed',
                'issues': [
                    self._issue(
                        code='unsupported_freq',
                        message='memberships filter currently only supports freq=1d',
                        path='freq',
                    )
                ],
            }

        end = str(request.get('end') or '').strip()
        membership_date = end[:10] if len(end) >= 10 else ''
        if not membership_date:
            return {
                'failed': True,
                'empty_reason': 'validation_failed',
                'issues': [
                    self._issue(
                        code='unsupported_field',
                        message='memberships filter requires end date to resolve as_of_date',
                        path='end',
                    )
                ],
            }

        asset_type = str(request.get('asset_type') or '').strip()
        resolved_asset_type = None if asset_type in {'', 'auto'} else asset_type
        membership_result = membership_filter_symbols(
            memberships,
            as_of_date=membership_date,
            path=request.get('membership_path') or DEFAULT_MEMBERSHIP_PATH,
            security_type=resolved_asset_type or 'stock',
            executor=self.executor,
        )
        membership_symbols = list(membership_result.get('symbols') or [])
        original_symbols = list(request.get('symbols') or [])
        if original_symbols:
            membership_symbols = [symbol for symbol in original_symbols if symbol in set(membership_symbols)]

        if not membership_symbols:
            return {
                'failed': True,
                'empty_reason': 'no_rows',
                'issues': [
                    self._issue(
                        code='empty_result',
                        message='memberships filter matched no symbols',
                        path='memberships',
                    )
                ],
            }

        resolved_request = {
            **request,
            'symbols': membership_symbols,
            'universe': None,
        }
        return {
            'request': resolved_request,
            'failed': False,
        }

    def _query_failure_result(
        self,
        *,
        request: dict[str, Any],
        validation: dict[str, Any],
        issues: list[dict[str, Any]],
        elapsed_ms: int,
        empty_reason: str,
        extra_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        symbols = list(request.get('symbols') or [])
        meta = self._build_query_meta(
            validation=validation,
            request=request,
            row_count=0,
            symbol_count=len(symbols),
            empty=True,
            elapsed_ms=elapsed_ms,
            empty_reason=empty_reason,
        )
        if extra_meta:
            meta.update(extra_meta)
        return {
            'ok': False,
            'df': pd.DataFrame(),
            'issues': issues,
            'meta': meta,
            'debug': self._build_debug_bundle(
                request=request,
                validation=validation,
                intent=None,
                sql='',
            ),
        }

    def _intraday_failure_result(
        self,
        *,
        request: dict[str, Any],
        validation: dict[str, Any],
        issues: list[dict[str, Any]],
        elapsed_ms: int,
        empty_reason: str,
        row_count: int,
        symbol_count: int,
        trading_day_count: int,
        query_count: int,
        extra_meta: dict[str, Any] | None = None,
        debug_extra: dict[str, Any] | None = None,
        sqls: list[str] | None = None,
    ) -> dict[str, Any]:
        sql_items = [sql for sql in (sqls or []) if sql]
        meta = {
            'asset_type': validation.get('resolved', {}).get('asset_type'),
            'freq': '1m',
            'node': validation.get('resolved', {}).get('node'),
            'fields': list(validation.get('resolved', {}).get('fields') or request.get('fields') or []),
            'row_count': row_count,
            'symbol_count': symbol_count,
            'trading_day_count': trading_day_count,
            'empty': True,
            'empty_reason': empty_reason,
            'elapsed_ms': elapsed_ms,
            'query_count': query_count,
        }
        if extra_meta:
            meta.update(extra_meta)
        debug = {
            'request': request,
            'validation': validation,
            'resolved': validation.get('resolved', {}),
            'intent': None,
            'sql': ';\n\n'.join(sql_items),
            'sqls': sql_items,
        }
        if debug_extra:
            debug.update(debug_extra)
        return {
            'ok': False,
            'df': pd.DataFrame(),
            'issues': issues,
            'meta': meta,
            'debug': debug,
        }

    def _validate_intraday_window_request(self, request: dict[str, Any]) -> dict[str, Any]:
        symbols = sorted({str(item).strip() for item in (request.get('symbols') or []) if str(item).strip()})
        trading_days = sorted({str(item).strip() for item in (request.get('trading_days') or []) if str(item).strip()})
        requested_fields = list(dict.fromkeys(str(item).strip() for item in (request.get('fields') or []) if str(item).strip()))
        hhmm_list = list(dict.fromkeys(str(item).strip() for item in (request.get('hhmm_list') or []) if str(item).strip()))
        asset_type = str(request.get('asset_type') or 'stock').strip() or 'stock'
        issues: list[dict[str, Any]] = []

        if not symbols:
            issues.append(
                self._issue(
                    code='missing_symbols',
                    message='symbols is required for intraday minute window query',
                    path='symbols',
                )
            )
        if not trading_days:
            issues.append(
                self._issue(
                    code='missing_trading_days',
                    message='trading_days is required for intraday minute window query',
                    path='trading_days',
                )
            )
        if not requested_fields:
            issues.append(
                self._issue(
                    code='unsupported_field',
                    message='fields must not be empty for intraday minute window query',
                    path='fields',
                )
            )

        start_hhmm = self._normalize_hhmm(str(request.get('start_hhmm') or ''))
        end_hhmm = self._normalize_hhmm(str(request.get('end_hhmm') or ''))
        if not start_hhmm or not end_hhmm:
            issues.append(
                self._issue(
                    code='invalid_intraday_time',
                    message='start_hhmm and end_hhmm must use HH:MM format',
                    path='time_window',
                )
            )
        elif start_hhmm > end_hhmm:
            issues.append(
                self._issue(
                    code='invalid_intraday_time',
                    message='start_hhmm must be earlier than or equal to end_hhmm',
                    path='time_window',
                )
            )

        normalized_hhmm_list: list[str] = []
        for raw_hhmm in hhmm_list:
            normalized_hhmm = self._normalize_hhmm(raw_hhmm)
            if not normalized_hhmm:
                issues.append(
                    self._issue(
                        code='invalid_intraday_time',
                        message=f'hhmm_list item must use HH:MM format: {raw_hhmm}',
                        path='hhmm_list',
                    )
                )
                continue
            normalized_hhmm_list.append(normalized_hhmm)
        hhmm_list = list(dict.fromkeys(normalized_hhmm_list))

        node_resolution = self.resolve_best_node(
            symbols=symbols,
            fields=['open'],
            freq='1m',
            asset_type=asset_type,
        )
        issues.extend(list(node_resolution.get('issues') or []))

        direct_fields = [field for field in requested_fields if field in INTRADAY_DIRECT_MINUTE_FIELDS]
        limit_fields = [field for field in requested_fields if field in INTRADAY_LIMIT_DERIVED_FIELDS]
        unsupported_fields = [
            field
            for field in requested_fields
            if field not in INTRADAY_DIRECT_MINUTE_FIELDS
            and field not in INTRADAY_LIMIT_DERIVED_FIELDS
            and field not in {'code', 'trade_time'}
        ]
        for field in unsupported_fields:
            issues.append(
                self._issue(
                    code='unsupported_field',
                    message=f'{field} is not supported by intraday execution interface',
                    path=f'fields.{field}',
                )
            )

        if limit_fields and node_resolution.get('asset_type') not in {None, 'stock'}:
            for field in limit_fields:
                issues.append(
                    self._issue(
                        code='unsupported_field',
                        message=f'{field} currently only supports asset_type=stock in intraday execution interface',
                        path=f'fields.{field}',
                    )
                )

        minute_query_fields = list(dict.fromkeys(direct_fields + (['close'] if any(field in limit_fields for field in {'is_limit_up', 'is_limit_down'}) else [])))
        if not minute_query_fields:
            minute_query_fields = ['close']
        daily_limit_fields: list[str] = []
        if 'is_limit_up' in limit_fields or 'limit_up_price' in limit_fields:
            daily_limit_fields.append('high_limited')
        if 'is_limit_down' in limit_fields or 'limit_down_price' in limit_fields:
            daily_limit_fields.append('low_limited')

        return {
            'ok': not issues,
            'issues': issues,
            'resolved': {
                'asset_type': node_resolution.get('asset_type') or asset_type,
                'node': node_resolution.get('node'),
                'symbols': symbols,
                'trading_days': trading_days,
                'fields': requested_fields,
                'direct_fields': direct_fields,
                'limit_fields': limit_fields,
                'minute_query_fields': minute_query_fields,
                'daily_limit_fields': daily_limit_fields,
                'start_hhmm': start_hhmm,
                'end_hhmm': end_hhmm,
                'hhmm_list': hhmm_list,
            },
        }

    def _resolve_intraday_anchors(self, anchors: list[dict[str, Any]]) -> dict[str, Any]:
        normalized: list[dict[str, str]] = []
        issues: list[dict[str, Any]] = []
        for index, item in enumerate(anchors):
            if not isinstance(item, dict):
                issues.append(
                    self._issue(
                        code='missing_anchors',
                        message=f'anchors[{index}] must be an object',
                        path=f'anchors.{index}',
                    )
                )
                continue
            code = str(item.get('code') or '').strip()
            signal_date = str(item.get('signal_date') or '').strip()
            execution_date = str(item.get('execution_date') or '').strip()
            anchor_id = str(item.get('anchor_id') or '').strip() or f'{code}__{signal_date}__{execution_date or "next"}'
            if not code:
                issues.append(
                    self._issue(
                        code='missing_anchors',
                        message=f'anchors[{index}].code is required',
                        path=f'anchors.{index}.code',
                    )
                )
                continue
            if not signal_date:
                issues.append(
                    self._issue(
                        code='missing_anchors',
                        message=f'anchors[{index}].signal_date is required',
                        path=f'anchors.{index}.signal_date',
                    )
                )
                continue
            normalized.append(
                {
                    'anchor_id': anchor_id,
                    'code': code,
                    'signal_date': signal_date,
                    'execution_date': execution_date,
                }
            )

        if issues:
            return {
                'ok': False,
                'issues': issues,
                'anchors': [],
            }

        if all(item.get('execution_date') for item in normalized):
            return {
                'ok': True,
                'issues': [],
                'anchors': normalized,
            }

        trading_calendar_table = self._get_trading_calendar_table()
        if not trading_calendar_table:
            return {
                'ok': False,
                'issues': [
                    self._issue(
                        code='trading_calendar_unavailable',
                        message='trade_calendar_real node is required when anchors omit execution_date',
                        path='anchors',
                    )
                ],
                'anchors': [],
            }

        next_day_map = self._load_next_trading_day_map(
            sorted({item['signal_date'] for item in normalized if not item.get('execution_date')})
        )
        if not next_day_map['ok']:
            return {
                'ok': False,
                'issues': list(next_day_map['issues']),
                'anchors': [],
            }

        resolved_anchors: list[dict[str, str]] = []
        missing_execution_issues: list[dict[str, Any]] = []
        for item in normalized:
            execution_date = item.get('execution_date') or next_day_map['mapping'].get(item['signal_date'])
            if not execution_date:
                missing_execution_issues.append(
                    self._issue(
                        code='missing_execution_date',
                        message=f'cannot resolve next trading day for signal_date={item["signal_date"]}',
                        path=f'anchors.{item["anchor_id"]}.execution_date',
                    )
                )
                continue
            resolved_anchors.append(
                {
                    **item,
                    'execution_date': execution_date,
                }
            )

        return {
            'ok': not missing_execution_issues,
            'issues': missing_execution_issues,
            'anchors': resolved_anchors,
        }

    def _load_next_trading_day_map(self, signal_dates: list[str]) -> dict[str, Any]:
        if not signal_dates:
            return {
                'ok': True,
                'issues': [],
                'mapping': {},
            }
        trading_calendar_table = self._get_trading_calendar_table()
        if not trading_calendar_table:
            return {
                'ok': False,
                'issues': [
                    self._issue(
                        code='trading_calendar_unavailable',
                        message='trade_calendar_real node is not configured',
                        path='graph.nodes.trade_calendar_real',
                    )
                ],
                'mapping': {},
            }

        start_date = min(signal_dates)
        sql = (
            f'SELECT DISTINCT trade_date '
            f'FROM {trading_calendar_table} '
            f"WHERE trade_date > toDate('{start_date}') "
            f'ORDER BY trade_date ASC'
        )
        result = self.execute_sql(sql)
        if result['code'] != 0:
            return {
                'ok': False,
                'issues': [
                    self._issue(
                        code='query_failed',
                        message='failed to load trading calendar while resolving next trading day',
                        path='runtime.discovery.trading_calendar_table',
                    )
                ],
                'mapping': {},
            }

        calendar_df = result['df']
        if calendar_df.empty or 'trade_date' not in calendar_df.columns:
            return {
                'ok': False,
                'issues': [
                    self._issue(
                        code='trading_calendar_unavailable',
                        message='trading calendar query returned no trade_date column',
                        path='runtime.discovery.trading_calendar_table',
                    )
                ],
                'mapping': {},
            }

        calendar_dates = sorted(
            {
                str(value)
                for value in pd.to_datetime(calendar_df['trade_date'], errors='coerce').dt.date.astype(str).tolist()
                if value and value != 'NaT'
            }
        )
        mapping: dict[str, str] = {}
        for signal_date in signal_dates:
            next_date = next((trade_date for trade_date in calendar_dates if trade_date > signal_date), None)
            if next_date:
                mapping[signal_date] = next_date
        return {
            'ok': True,
            'issues': [],
            'mapping': mapping,
        }

    def _get_trading_calendar_table(self) -> str:
        configured = str(self.runtime_config.discovery.trading_calendar_table or '').strip()
        if configured:
            return configured
        node = next((item for item in self.nodes if item.name == 'trade_calendar_real' and item.status == 'enabled'), None)
        if node is None:
            return ''
        return str(node.table or '').strip()

    def _fetch_intraday_limit_frame(
        self,
        *,
        symbols: list[str],
        trading_days: list[str],
        daily_fields: list[str],
    ) -> dict[str, Any]:
        if not daily_fields:
            return {
                'ok': True,
                'df': pd.DataFrame(),
                'issues': [],
                'sqls': [],
            }
        daily_result = self.execute_query_profile(
            {
                'query_profile': 'panel_time_series',
                'symbols': symbols,
                'fields': daily_fields,
                'start': f'{min(trading_days)} 00:00:00',
                'end': f'{max(trading_days)} 23:59:59',
                'asset_type': 'stock',
                'freq': '1d',
            }
        )
        debug = dict(daily_result.get('debug') or {})
        if daily_result.get('ok'):
            df = daily_result.get('df')
            if isinstance(df, pd.DataFrame) and not df.empty:
                daily_df = df.copy()
                trade_time = pd.to_datetime(daily_df['trade_time'], errors='coerce')
                daily_df['trade_date'] = trade_time.dt.date.astype(str)
                daily_df = daily_df[daily_df['trade_date'].isin(set(trading_days))].reset_index(drop=True)
                return {
                    'ok': True,
                    'df': daily_df,
                    'issues': [],
                    'sqls': [str(debug.get('sql') or '')],
                }
            return {
                'ok': True,
                'df': pd.DataFrame(),
                'issues': [],
                'sqls': [str(debug.get('sql') or '')],
            }

        issue_codes = {issue.get('code') for issue in daily_result.get('issues') or [] if isinstance(issue, dict)}
        return {
            'ok': False,
            'df': pd.DataFrame(),
            'issues': list(daily_result.get('issues') or []),
            'sqls': [str(debug.get('sql') or '')],
            'empty_only': bool(issue_codes and issue_codes.issubset({'empty_result'})),
        }

    def _normalize_intraday_trade_time(self, df: pd.DataFrame) -> pd.DataFrame:
        normalized = df.copy()
        trade_time = pd.to_datetime(normalized['trade_time'], errors='coerce')
        normalized = normalized.loc[~trade_time.isna()].copy()
        normalized['trade_time'] = trade_time.loc[~trade_time.isna()].dt.strftime('%Y-%m-%d %H:%M:%S')
        normalized['trade_date'] = trade_time.loc[~trade_time.isna()].dt.date.astype(str).values
        return normalized.reset_index(drop=True)

    def _merge_intraday_limit_frame(self, minute_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.DataFrame:
        merged = minute_df.merge(
            daily_df[['code', 'trade_date'] + [field for field in ['high_limited', 'low_limited'] if field in daily_df.columns]],
            how='left',
            left_on=['code', 'trade_date'],
            right_on=['code', 'trade_date'],
        )
        return merged

    def _apply_intraday_limit_fields(self, df: pd.DataFrame, limit_fields: list[str]) -> pd.DataFrame:
        enriched = df.copy()
        high_limited = pd.to_numeric(
            enriched['high_limited'] if 'high_limited' in enriched.columns else pd.Series([None] * len(enriched), index=enriched.index),
            errors='coerce',
        )
        low_limited = pd.to_numeric(
            enriched['low_limited'] if 'low_limited' in enriched.columns else pd.Series([None] * len(enriched), index=enriched.index),
            errors='coerce',
        )
        close_values = pd.to_numeric(
            enriched['close'] if 'close' in enriched.columns else pd.Series([None] * len(enriched), index=enriched.index),
            errors='coerce',
        )
        if 'limit_up_price' in limit_fields:
            enriched['limit_up_price'] = high_limited
        if 'limit_down_price' in limit_fields:
            enriched['limit_down_price'] = low_limited
        if 'is_limit_up' in limit_fields:
            enriched['is_limit_up'] = ((close_values >= high_limited) & high_limited.notna()).astype(int)
        if 'is_limit_down' in limit_fields:
            enriched['is_limit_down'] = ((close_values <= low_limited) & low_limited.notna()).astype(int)
        return enriched

    def _normalize_hhmm(self, value: str) -> str | None:
        try:
            parsed = datetime.strptime(value.strip(), '%H:%M')
        except Exception:
            return None
        return parsed.strftime('%H:%M')

    def _quote_sql_string(self, value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    def _sql_literal(self, value: Any) -> str:
        if value is None:
            return 'NULL'
        if isinstance(value, bool):
            return '1' if value else '0'
        if isinstance(value, (int, float)):
            return str(value)
        return self._quote_sql_string(str(value))

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
            if hasattr(self.executor, 'execute_sql_df_timed'):
                payload = self.executor.execute_sql_df_timed(sql)
                return self._build_result(
                    code=SUCCESS_CODE,
                    message='success',
                    sql=payload.get('sql') or sql,
                    df=payload.get('df') if isinstance(payload.get('df'), pd.DataFrame) else pd.DataFrame(),
                    statistics={
                        'executor_timings': payload.get('timings') or {},
                        'response_bytes': payload.get('response_bytes'),
                        'row_count': payload.get('row_count'),
                        'parser_engine': payload.get('parser_engine'),
                    },
                )
            if hasattr(self.executor, 'execute_sql_df'):
                df = self.executor.execute_sql_df(sql)
                return self._build_result(
                    code=SUCCESS_CODE,
                    message='success',
                    sql=sql,
                    df=df,
                )
            result = self.executor.execute_sql(sql)
            return self._build_result(
                code=SUCCESS_CODE,
                message='success',
                sql=result.sql,
                df=pd.DataFrame(result.data),
                statistics={
                    'rows': result.rows,
                    'statistics': result.statistics,
                    'meta': result.meta,
                },
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
                    'description_zh': node.description_zh or node.description or node.name,
                    'status': node.status,
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

    def _build_result(
        self,
        code: int,
        message: str,
        sql: str,
        df: pd.DataFrame,
        statistics: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            'code': code,
            'message': message,
            'sql': sql,
            'df': df,
        }
        if statistics:
            payload['statistics'] = statistics
        return payload

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
            'can_delete': not affected_enabled_nodes,
            'affected_enabled_nodes': affected_enabled_nodes,
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
            asset_type = self._effective_node_asset_type(node)
            if not asset_type:
                continue
            freq = self._freq_from_node(node)
            if not freq:
                continue
            current = mapping.setdefault(asset_type, {}).get(freq)
            if current is None or self._query_entry_priority(node.name, asset_type=asset_type, freq=freq) > self._query_entry_priority(current, asset_type=asset_type, freq=freq):
                mapping.setdefault(asset_type, {})[freq] = node.name
        return mapping

    def _apply_wide_table_overlays(self, nodes: list[Node]) -> list[Node]:
        if not self.wide_table_specs:
            return nodes

        overlaid_nodes: list[Node] = []
        for node in nodes:
            spec = self.wide_table_specs.get(node.name)
            if not spec:
                overlaid_nodes.append(node)
                continue
            overlaid_nodes.append(self._overlay_node_with_wide_table(node, spec))
        return overlaid_nodes

    def _overlay_node_with_wide_table(self, node: Node, spec: dict[str, Any]) -> Node:
        target_table = self._wide_table_target_ref(spec, fallback=node.table)
        overlay_fields = self._wide_table_fields(spec)
        time_key = self._wide_table_time_key(spec, fallback=node.time_key)
        entity_keys = self._wide_table_entity_keys(spec, fallback=node.entity_keys, time_key=time_key)
        return Node(
            name=node.name,
            table=target_table,
            entity_keys=entity_keys,
            time_key=time_key,
            time_key_mode=node.time_key_mode,
            interval_keys=dict(node.interval_keys or {}),
            grain=node.grain,
            fields=overlay_fields,
            description=str(spec.get('description') or node.description or '').strip() or node.description,
            description_zh=str(spec.get('description_zh') or node.description_zh or node.description or '').strip() or node.description_zh,
            node_role=node.node_role,
            status=node.status,
            asset_type=node.asset_type,
            query_freq=node.query_freq,
            # Wide table target already materializes the asset-specific base universe.
            base_filters=[],
        )

    def _wide_table_target_ref(self, spec: dict[str, Any], *, fallback: str) -> str:
        database = str(spec.get('target_database') or '').strip()
        table = str(spec.get('target_table') or '').strip()
        if not database or not table:
            return fallback
        return f'{database}.{table}'

    def _wide_table_fields(self, spec: dict[str, Any]) -> list[str]:
        ordered: list[str] = []
        for item in list(spec.get('fields') or []) + list(spec.get('key_fields') or []):
            field_name = str(item or '').strip()
            if field_name and field_name not in ordered:
                ordered.append(field_name)
        return ordered

    def _wide_table_time_key(self, spec: dict[str, Any], *, fallback: str | None) -> str | None:
        candidates = self._wide_table_fields(spec)
        for field_name in candidates:
            if field_name in {'trade_time', 'trade_date', 'date', 'ann_date', 'change_date', 'report_date', 'end_date'}:
                return field_name
        return fallback

    def _wide_table_entity_keys(
        self,
        spec: dict[str, Any],
        *,
        fallback: list[str],
        time_key: str | None,
    ) -> list[str]:
        key_fields = [
            str(item or '').strip()
            for item in list(spec.get('key_fields') or [])
            if str(item or '').strip()
        ]
        if not key_fields:
            return list(fallback)
        return [item for item in key_fields if item != time_key]

    def _build_application_query_node_map(self) -> dict[str, dict[str, str]]:
        mapping: dict[str, dict[str, str]] = {}
        for node in self.nodes:
            asset_type = self._effective_node_asset_type(node)
            status = self._effective_node_status(node)
            if status != 'enabled' or not asset_type:
                continue
            freq = self._freq_from_node(node)
            if not freq:
                continue
            current = mapping.setdefault(asset_type, {}).get(freq)
            if current is None or self._query_entry_priority(node.name, asset_type=asset_type, freq=freq) > self._query_entry_priority(current, asset_type=asset_type, freq=freq):
                mapping.setdefault(asset_type, {})[freq] = node.name
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
        asset_type = self._effective_node_asset_type(node)
        freq = self._freq_from_node(node)
        base_filters = list(node.base_filters)
        if not base_filters and not asset_type and not freq:
            return None
        return {
            'asset_type': asset_type,
            'freq': freq,
            'base_filters': base_filters,
        }

    def _effective_node_asset_type(self, node) -> str | None:
        if node.asset_type:
            return node.asset_type
        name = str(getattr(node, 'name', '') or '').strip().lower()
        if name.startswith('stock_'):
            return 'stock'
        if name.startswith('etf_'):
            return 'etf'
        if name.startswith('index_') or name.startswith('industry_'):
            return 'index'
        if name.startswith('fund_'):
            return 'fund'
        if name.startswith('treasury_'):
            return 'macro'
        if name.startswith('kzz_'):
            return 'kzz'
        return None

    def _effective_node_status(self, node) -> str:
        if node.status:
            return node.status
        return 'enabled'

    def _query_entry_priority(self, node_name: str, *, asset_type: str, freq: str) -> int:
        normalized = str(node_name or '').strip().lower()
        direct_targets = {
            ('stock', '1d'): 'stock_daily_real',
            ('stock', '1m'): 'stock_minute_real',
        }
        preferred = direct_targets.get((asset_type, freq))
        if preferred and normalized == preferred:
            return 100
        score = 0
        if normalized.endswith('_daily_real') and freq == '1d':
            score += 20
        if normalized.endswith('_minute_real') and freq == '1m':
            score += 20
        if f'{asset_type}_' in normalized:
            score += 10
        if 'snapshot' in normalized:
            score -= 20
        if 'basic' in normalized:
            score -= 10
        if 'pcf' in normalized:
            score -= 5
        return score

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
        return list(IDENTITY_FIELDS.get(node_name or '', []))

    def _is_node_field_allowed(self, field_name: str, node_name: str) -> bool:
        node = self.registry.nodes.get(node_name)
        if node and field_name in set(node.fields):
            return True
        if node:
            # Graph-registered nodes already limit fields through node.fields and fields.yaml
            # path resolution. Do not block custom extension fields with legacy asset allowlists.
            return True
        asset_type, freq = self._asset_freq_for_node(node_name)
        if not asset_type or not freq:
            return True
        return self._is_field_allowed_for_asset(field_name, asset_type, freq)

    def _expand_logical_intent(self, intent: dict[str, Any]) -> dict[str, Any]:
        expanded = dict(intent)
        logical_node = expanded.get('from')
        if logical_node:
            expanded['from'] = logical_node

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
        if not asset_type:
            return []
        nodes = []
        for node in self.nodes:
            if self._effective_node_status(node) != 'enabled':
                continue
            if self._effective_node_asset_type(node) != asset_type:
                continue
            if self._freq_from_node(node) != freq:
                continue
            nodes.append(node.name)
        if not nodes:
            node = self.application_query_node_map.get(asset_type or '', {}).get(freq)
            return [node] if node else []
        return sorted(
            nodes,
            key=lambda name: self._query_entry_priority(name, asset_type=asset_type, freq=freq),
            reverse=True,
        )

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
        effective_node_name = node_name
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
