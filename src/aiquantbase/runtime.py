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
from .wide_table import DEFAULT_WIDE_TABLE_PATH, load_wide_table_workspace

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
        wide_table_path: str | Path = DEFAULT_WIDE_TABLE_PATH,
    ) -> None:
        self.graph_path = Path(graph_path)
        self.fields_path = Path(fields_path)
        self.runtime_path = Path(runtime_path)
        self.wide_table_path = Path(wide_table_path)

        self.runtime_config = load_runtime_config(self.runtime_path)
        raw_nodes, self.edges = load_nodes_and_edges(self.graph_path)
        self.wide_table_workspace = load_wide_table_workspace(self.wide_table_path)
        self.wide_table_specs = {
            str(item.get('name') or '').strip(): item
            for item in self.wide_table_workspace.get('wide_tables', [])
            if str(item.get('status') or 'enabled').strip() == 'enabled'
        }
        self.nodes = self._apply_wide_table_overlays(raw_nodes)
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
            wide_table_path=DEFAULT_WIDE_TABLE_PATH,
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
        memberships: dict[str, Any] | None = None,
        membership_path: str | Path | None = None,
    ) -> dict[str, Any]:
        request = {
            'symbols': symbols or [],
            'universe': universe,
            'fields': fields,
            'start': start,
            'end': end,
            'freq': freq,
            'asset_type': asset_type,
            'memberships': memberships or None,
            'membership_path': membership_path,
        }
        return self._execute_query_request(request, page_size=None)

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
        return self._execute_query_request(request, page_size=None)

    def query_membership(
        self,
        security_code: str,
        *,
        as_of_date: str,
        security_type: str | None = None,
        membership_path: str | Path | None = None,
    ) -> dict[str, Any]:
        items = membership_query_membership(
            security_code,
            as_of_date=as_of_date,
            path=membership_path or DEFAULT_MEMBERSHIP_PATH,
            security_type=security_type,
            executor=self.executor,
        )
        return {
            'ok': True,
            'items': items,
            'count': len(items),
        }

    def resolve_membership_target(
        self,
        *,
        domain: str,
        member_code: str,
        taxonomy: str | None = None,
        member_name: str | None = None,
        membership_path: str | Path | None = None,
    ) -> dict[str, Any]:
        item = membership_resolve_target(
            domain=domain,
            member_code=member_code,
            taxonomy=taxonomy,
            member_name=member_name,
            path=membership_path or DEFAULT_MEMBERSHIP_PATH,
        )
        return {
            'ok': True,
            'item': item,
        }

    def filter_symbols_by_membership(
        self,
        memberships: dict[str, Any],
        *,
        as_of_date: str,
        security_type: str | None = None,
        membership_path: str | Path | None = None,
    ) -> dict[str, Any]:
        return membership_filter_symbols(
            memberships,
            as_of_date=as_of_date,
            path=membership_path or DEFAULT_MEMBERSHIP_PATH,
            security_type=security_type,
            executor=self.executor,
        )

    def query_minute_window_by_trading_day(
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
                day_result = self.query_minute(
                    symbols=symbols,
                    fields=minute_fields,
                    start=day_start,
                    end=day_end,
                    asset_type=resolved['asset_type'],
                    freq='1m',
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

    def query_next_trading_day_intraday_windows(
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
        base_result = self.query_minute_window_by_trading_day(
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
        table_name = node.table if node is not None else (self._physical_node_for(node_name) or node_name)
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
        intent = {
            'from': node,
            'select': select_fields,
            'where': {
                'mode': 'and',
                'items': (
                    [{'field': symbol_field, 'op': 'in' if len(symbols) > 1 else '=', 'value': (symbols if len(symbols) > 1 else symbols[0])}]
                    if symbols
                    else []
                ),
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
        membership_result = self.filter_symbols_by_membership(
            memberships,
            as_of_date=membership_date,
            security_type=resolved_asset_type or 'stock',
            membership_path=request.get('membership_path'),
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

        trading_calendar_table = str(self.runtime_config.discovery.trading_calendar_table or '').strip()
        if not trading_calendar_table:
            return {
                'ok': False,
                'issues': [
                    self._issue(
                        code='trading_calendar_unavailable',
                        message='runtime config discovery.trading_calendar_table is required when anchors omit execution_date',
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
        trading_calendar_table = str(self.runtime_config.discovery.trading_calendar_table or '').strip()
        if not trading_calendar_table:
            return {
                'ok': False,
                'issues': [
                    self._issue(
                        code='trading_calendar_unavailable',
                        message='runtime config discovery.trading_calendar_table is not configured',
                        path='runtime.discovery.trading_calendar_table',
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
        daily_result = self.query_daily(
            symbols=symbols,
            fields=daily_fields,
            start=f'{min(trading_days)} 00:00:00',
            end=f'{max(trading_days)} 23:59:59',
            asset_type='stock',
            freq='1d',
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
            if not node.asset_type:
                continue
            freq = self._freq_from_node(node)
            if not freq:
                continue
            mapping.setdefault(node.asset_type, {})[freq] = node.name
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
            grain=node.grain,
            fields=overlay_fields,
            description=str(spec.get('description') or node.description or '').strip() or node.description,
            description_zh=str(spec.get('description_zh') or node.description_zh or node.description or '').strip() or node.description_zh,
            node_role=node.node_role,
            status=node.status,
            physical_node=None,
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
        asset_type = node.asset_type
        freq = self._freq_from_node(node)
        base_filters = list(node.base_filters)
        if not base_filters and not asset_type and not freq:
            return None
        return {
            'asset_type': asset_type,
            'freq': freq,
            'base_filters': base_filters,
        }

    def _physical_node_for(self, node_name: str | None) -> str | None:
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
        return list(IDENTITY_FIELDS.get(node_name or '', []))

    def _is_node_field_allowed(self, field_name: str, node_name: str) -> bool:
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
