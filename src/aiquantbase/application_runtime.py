from __future__ import annotations

from pathlib import Path
from typing import Any

from .runtime import DEFAULT_FIELDS_PATH, DEFAULT_GRAPH_PATH, GraphRuntime
from .runtime_config import DEFAULT_RUNTIME_CONFIG_PATH


class ApplicationRuntime:
    """面向上层应用的接口封装层。

    这个 facade 只暴露应用侧真正需要的接口，
    底层图谱、SQL 渲染和执行能力继续由 GraphRuntime 承担。
    """

    def __init__(
        self,
        graph_path: str | Path = DEFAULT_GRAPH_PATH,
        fields_path: str | Path = DEFAULT_FIELDS_PATH,
        runtime_path: str | Path = DEFAULT_RUNTIME_CONFIG_PATH,
    ) -> None:
        self.graph_runtime = GraphRuntime(
            graph_path=graph_path,
            fields_path=fields_path,
            runtime_path=runtime_path,
        )

    @classmethod
    def from_defaults(cls) -> 'ApplicationRuntime':
        return cls(
            graph_path=DEFAULT_GRAPH_PATH,
            fields_path=DEFAULT_FIELDS_PATH,
            runtime_path=DEFAULT_RUNTIME_CONFIG_PATH,
        )

    def resolve_symbols(self, symbols: list[str]) -> dict[str, Any]:
        return self.graph_runtime.resolve_symbols(symbols)

    def resolve_best_node(
        self,
        *,
        symbols: list[str] | None,
        universe: str | None = None,
        fields: list[str],
        freq: str = '1d',
        asset_type: str = 'auto',
    ) -> dict[str, Any]:
        return self.graph_runtime.resolve_best_node(
            symbols=symbols,
            universe=universe,
            fields=fields,
            freq=freq,
            asset_type=asset_type,
        )

    def get_supported_fields(
        self,
        *,
        asset_type: str | None = None,
        freq: str = '1d',
        node: str | None = None,
        field_role: str | None = None,
        derived_only: bool | None = None,
    ) -> dict[str, Any]:
        return self.graph_runtime.get_supported_fields(
            asset_type=asset_type,
            freq=freq,
            node=node,
            field_role=field_role,
            derived_only=derived_only,
        )

    def validate_query_request(self, request: dict[str, Any]) -> dict[str, Any]:
        return self.graph_runtime.validate_query_request(request)

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
        return self.graph_runtime.query_daily(
            symbols=symbols,
            universe=universe,
            fields=fields,
            start=start,
            end=end,
            asset_type=asset_type,
            freq=freq,
            memberships=memberships,
            membership_path=membership_path,
        )

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
        return self.graph_runtime.query_minute(
            symbols=symbols,
            universe=universe,
            fields=fields,
            start=start,
            end=end,
            asset_type=asset_type,
            freq=freq,
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
    ) -> dict[str, Any]:
        return self.graph_runtime.query_minute_window_by_trading_day(
            symbols=symbols,
            trading_days=trading_days,
            start_hhmm=start_hhmm,
            end_hhmm=end_hhmm,
            fields=fields,
            asset_type=asset_type,
        )

    def query_next_trading_day_intraday_windows(
        self,
        *,
        anchors: list[dict[str, Any]],
        start_hhmm: str,
        end_hhmm: str,
        fields: list[str],
        asset_type: str = 'stock',
    ) -> dict[str, Any]:
        return self.graph_runtime.query_next_trading_day_intraday_windows(
            anchors=anchors,
            start_hhmm=start_hhmm,
            end_hhmm=end_hhmm,
            fields=fields,
            asset_type=asset_type,
        )

    def query_membership(
        self,
        security_code: str,
        *,
        as_of_date: str,
        security_type: str | None = None,
        membership_path: str | Path | None = None,
    ) -> dict[str, Any]:
        return self.graph_runtime.query_membership(
            security_code,
            as_of_date=as_of_date,
            security_type=security_type,
            membership_path=membership_path,
        )

    def resolve_membership_target(
        self,
        *,
        domain: str,
        member_code: str,
        taxonomy: str | None = None,
        member_name: str | None = None,
        membership_path: str | Path | None = None,
    ) -> dict[str, Any]:
        return self.graph_runtime.resolve_membership_target(
            domain=domain,
            member_code=member_code,
            taxonomy=taxonomy,
            member_name=member_name,
            membership_path=membership_path,
        )

    def filter_symbols_by_membership(
        self,
        memberships: dict[str, Any],
        *,
        as_of_date: str,
        security_type: str | None = None,
        membership_path: str | Path | None = None,
    ) -> dict[str, Any]:
        return self.graph_runtime.filter_symbols_by_membership(
            memberships,
            as_of_date=as_of_date,
            security_type=security_type,
            membership_path=membership_path,
        )

    def build_intent_from_requirement(self, data_requirement: dict[str, Any]) -> dict[str, Any]:
        return self.graph_runtime.build_intent_from_requirement(data_requirement)

    def execute_requirement(self, data_requirement: dict[str, Any]) -> dict[str, Any]:
        return self.graph_runtime.execute_requirement(data_requirement)
