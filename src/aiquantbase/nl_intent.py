from __future__ import annotations

from copy import deepcopy


FIELD_ALIASES = {
    "收盘价": "close",
    "开盘价": "open",
    "最高价": "high",
    "最低价": "low",
    "前复权收盘价": "close_adj",
    "前复权开盘价": "open_adj",
    "前复权最高价": "high_adj",
    "前复权最低价": "low_adj",
    "昨收": "pre_close",
    "前收": "pre_close",
    "是否st": "is_st",
    "是否停牌": "is_suspended",
    "涨停价": "high_limited",
    "跌停价": "low_limited",
    "前复权因子": "forward_adj_factor",
    "换手率": "turnover_rate",
    "市值": "market_cap",
    "总市值": "market_cap",
    "流通市值": "float_market_cap",
    "总股本": "tot_share",
    "流通股本": "float_share",
    "流通a股": "float_a_share",
    "跳空开盘收益": "gap_open_return",
    "日期": "trade_time",
    "date": "trade_time",
}


def enrich_query_intent_with_aliases(
    query_intent: dict | None,
    natural_language_query: str,
    available_fields: set[str],
) -> dict:
    intent = deepcopy(query_intent or {})
    existing_select = intent.get("select", [])
    normalized_select: list[str] = []
    for item in existing_select:
        if isinstance(item, dict):
            normalized_select.append(item.get("field", ""))
        else:
            normalized_select.append(str(item))

    lower_query = natural_language_query.lower().replace(" ", "")
    for alias, standard_field in FIELD_ALIASES.items():
        if alias in lower_query and standard_field in available_fields and standard_field not in normalized_select:
            normalized_select.append(standard_field)

    if normalized_select:
        intent["select"] = normalized_select
    return intent


def normalize_query_intent_defaults(intent: dict | None, default_time_field: str = "trade_time") -> dict:
    normalized = deepcopy(intent or {})
    if not isinstance(normalized.get("from"), str):
        normalized["from"] = "stock_daily_real"

    select_items = normalized.get("select", [])
    if isinstance(select_items, str):
        select_items = [select_items]
    normalized_select: list[str] = []
    for item in select_items:
        if isinstance(item, str):
            normalized_select.append(_normalize_field_alias(item, default_time_field))
        elif isinstance(item, dict) and "field" in item:
            item["field"] = _normalize_field_alias(str(item["field"]), default_time_field)
            normalized_select.append(item["field"])
        else:
            normalized_select.append(item)
    normalized["select"] = normalized_select

    where = normalized.get("where")
    if where is None:
        normalized["where"] = {"mode": "and", "items": []}
    elif isinstance(where, dict) and "conditions" in where and "items" not in where:
        normalized["where"] = {
            "mode": str(where.get("operator", "and")).lower(),
            "items": where.get("conditions", []),
        }
    elif isinstance(where, list):
        normalized["where"] = {"mode": "and", "items": where}
    _normalize_filter_group_fields(normalized["where"], default_time_field)

    time_range = normalized.get("time_range")
    if isinstance(time_range, dict) and "field" not in time_range:
        time_range["field"] = default_time_field
    if isinstance(time_range, dict) and "field" in time_range:
        time_range["field"] = _normalize_field_alias(str(time_range["field"]), default_time_field)
    order_by = normalized.get("order_by", [])
    if isinstance(order_by, dict):
        order_by = [order_by]
    if isinstance(order_by, list):
        for item in order_by:
            if isinstance(item, dict) and "field" not in item:
                item["field"] = default_time_field
            if isinstance(item, dict) and "field" in item:
                item["field"] = _normalize_field_alias(str(item["field"]), default_time_field)
    normalized["order_by"] = order_by

    safety = normalized.get("safety")
    if not isinstance(safety, dict):
        normalized["safety"] = {"lookahead_safe": True, "strict_mode": True}

    if "page" not in normalized:
        normalized["page"] = 1
    if "page_size" not in normalized:
        normalized["page_size"] = 20
    return normalized


def validate_query_intent_fields(
    intent: dict | None,
    available_fields: set[str],
    default_time_field: str = "trade_time",
) -> dict:
    normalized = deepcopy(intent or {})

    select_items = normalized.get("select", [])
    normalized["select"] = [item for item in select_items if isinstance(item, str) and item in available_fields]

    _filter_group_by_available_fields(normalized.get("where"), available_fields)
    _filter_group_by_available_fields(normalized.get("having"), available_fields)

    order_by = normalized.get("order_by", [])
    normalized["order_by"] = [
        item
        for item in order_by
        if isinstance(item, dict) and item.get("field") in available_fields | {default_time_field, "code"}
    ]

    time_range = normalized.get("time_range")
    if isinstance(time_range, dict) and time_range.get("field") not in available_fields | {default_time_field, "code"}:
        time_range["field"] = default_time_field

    return normalized


def _normalize_filter_group_fields(group: dict, default_time_field: str) -> None:
    items = group.get("items", [])
    for item in items:
        if isinstance(item, dict) and "items" in item:
            _normalize_filter_group_fields(item, default_time_field)
        elif isinstance(item, dict) and "conditions" in item and "items" not in item:
            item["mode"] = str(item.get("operator", "and")).lower()
            item["items"] = item.pop("conditions")
            _normalize_filter_group_fields(item, default_time_field)
        elif isinstance(item, dict) and "field" in item:
            item["field"] = _normalize_field_alias(str(item["field"]), default_time_field)
            if "operator" in item and "op" not in item:
                item["op"] = item.pop("operator")


def _normalize_field_alias(field_name: str, default_time_field: str) -> str:
    normalized = field_name.strip()
    if normalized.lower() in {"date", "trade_date", "日期", "交易日"}:
        return default_time_field
    if normalized.lower() in {"symbol", "ticker", "security_code"}:
        return "code"
    return normalized


def _filter_group_by_available_fields(group: dict | None, available_fields: set[str]) -> None:
    if not isinstance(group, dict):
        return
    items = group.get("items", [])
    filtered_items = []
    for item in items:
        if isinstance(item, dict) and "items" in item:
            _filter_group_by_available_fields(item, available_fields)
            if item.get("items"):
                filtered_items.append(item)
        elif isinstance(item, dict) and item.get("field") in available_fields | {"code", "trade_time"}:
            filtered_items.append(item)
    group["items"] = filtered_items
