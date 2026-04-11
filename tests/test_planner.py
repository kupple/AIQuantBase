from aiquantbase.config import load_field_catalog, load_nodes_and_edges, load_query_intent
from aiquantbase.planner import GraphRegistry, QueryPlanner
from aiquantbase.runtime_config import load_runtime_config
from aiquantbase.sql import SqlRenderer


def test_demo_plan_and_render():
    nodes, edges = load_nodes_and_edges("examples/demo_graph.yaml")
    intent = load_query_intent("examples/demo_intent.yaml")
    registry = GraphRegistry(nodes, edges)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.base_node == "stock_daily"
    assert plan.field_bindings["stock_daily.close"] == "stock_daily"
    assert plan.field_bindings["is_st"] == "stock_st_status"
    assert plan.field_bindings["financial_announcement.revenue"] == "financial_announcement"
    assert plan.field_bindings["concept_name"] == "concept_info"
    assert plan.resolved_fields["stock_daily.close"] == "close"

    sql = SqlRenderer(registry).render(plan)
    assert "LEFT JOIN market.stock_st_status" in sql
    assert "ASOF LEFT JOIN fundamental.stock_financial_announcement" in sql
    assert "LEFT JOIN reference.stock_concept_membership" in sql
    assert "LEFT JOIN reference.concept_info" in sql
    assert "b0.trade_date >= t2.announce_date" in sql
    assert "b0.close AS close_price" in sql
    assert "LIKE '%白酒%'" in sql
    assert " OR " in sql
    assert "NOT IN ('测试概念', '停用概念')" in sql
    assert "LIMIT 100" in sql
    assert "OFFSET 200" in sql


def test_filter_field_can_live_on_related_node():
    nodes, edges = load_nodes_and_edges("examples/demo_graph.yaml")
    registry = GraphRegistry(nodes, edges)
    intent = load_query_intent("examples/demo_intent.yaml")
    intent.where.items.append(
        type(intent.where.items[0])(
            field="concept_info.concept_name",
            op="=",
            value="白酒概念",
        )
    )

    plan = QueryPlanner(registry).plan(intent)
    sql = SqlRenderer(registry).render(plan)

    assert "t3.concept_name = '白酒概念'" in sql or "t2.concept_name = '白酒概念'" in sql


def test_grouped_query_intent_renders_group_by_and_aggregations():
    nodes, edges = load_nodes_and_edges("examples/demo_graph.yaml")
    intent = load_query_intent("examples/demo_grouped_intent.yaml")
    registry = GraphRegistry(nodes, edges)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.group_by == ["concept_name"]
    assert len(plan.aggregations) == 2

    sql = SqlRenderer(registry).render(plan)

    assert "AVG(" in sql
    assert "MAX(" in sql
    assert "GROUP BY" in sql
    assert "concept_name AS concept_name" in sql
    assert "HAVING avg_close >= 100 AND latest_revenue IS NOT NULL" in sql
    assert "LIMIT 50" in sql
    assert "OFFSET 50" in sql


def test_show_intent_preserves_select_alias_and_where_group():
    intent = load_query_intent("examples/demo_intent.yaml")
    grouped_intent = load_query_intent("examples/demo_grouped_intent.yaml")

    assert intent.select[0].field == "stock_daily.close"
    assert intent.select[0].alias == "close_price"
    assert intent.where.mode == "and"
    assert len(intent.where.items) == 3
    assert intent.page == 3
    assert intent.page_size == 100
    assert intent.limit == 100
    assert intent.offset == 200
    assert grouped_intent.having.items[0].field == "avg_close"


def test_runtime_config_loads():
    runtime = load_runtime_config("config/runtime.example.yaml")

    assert runtime.llm.provider_name == "deepseek"
    assert runtime.datasource.db_type == "clickhouse"
    assert "starlight" in runtime.discovery.allow_databases


def test_query_intent_pagination_conflict_raises(tmp_path):
    path = tmp_path / "conflict.yaml"
    path.write_text(
        "\n".join(
            [
                "from: stock_daily",
                "select:",
                "  - stock_daily.close",
                "page: 2",
                "page_size: 50",
                "limit: 10",
            ]
        ),
        encoding="utf-8",
    )

    try:
        load_query_intent(path)
    except ValueError as exc:
        assert "Pagination conflict" in str(exc)
    else:
        raise AssertionError("Expected pagination conflict to raise ValueError")


def test_query_planner_resolves_standard_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_standard_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["forward_adj_factor"] == "adj_factor_real"
    assert plan.resolved_fields["forward_adj_factor"] == "factor_value"
    assert plan.field_bindings["is_st"] == "history_stock_status_real"
    assert plan.resolved_fields["is_st"] == "is_st_sec"
    assert "close_adj" in plan.derived_fields


def test_query_planner_resolves_turnover_rate_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_turnover_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["float_share"] == "equity_structure_real"
    assert plan.resolved_fields["float_share"] == "float_share"
    assert "turnover_rate" in plan.derived_fields


def test_query_planner_resolves_industry_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_industry_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["industry_index_code"] == "industry_weight_real"
    assert plan.field_bindings["industry_level1_name"] == "industry_base_info_real"
    assert "industry_name" in plan.derived_fields


def test_query_planner_resolves_index_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_index_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["index_code"] == "index_weight_real"
    assert plan.field_bindings["index_weight"] == "index_weight_real"
    assert plan.field_bindings["index_component_close"] == "index_weight_real"


def test_query_planner_resolves_index_constituent_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_index_constituent_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["index_constituent_code"] == "index_constituent_real"
    assert plan.field_bindings["index_name"] == "index_constituent_real"


def test_query_planner_resolves_index_alias_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)

    weight_intent = load_query_intent("examples/real_index_alias_intent.yaml")
    weight_plan = QueryPlanner(registry).plan(weight_intent)
    assert weight_plan.field_bindings["index_weight_code"] == "index_weight_real"

    constituent_intent = load_query_intent("examples/real_index_constituent_alias_intent.yaml")
    constituent_plan = QueryPlanner(registry).plan(constituent_intent)
    assert constituent_plan.field_bindings["index_constituent_name"] == "index_constituent_real"


def test_query_planner_resolves_industry_daily_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_industry_daily_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["industry_daily_close"] == "industry_daily_real"
    assert plan.field_bindings["industry_name"] == "industry_daily_real"


def test_query_planner_resolves_financial_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_financial_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["revenue"] == "income_real"
    assert plan.resolved_fields["revenue"] == "opera_rev"
    assert plan.field_bindings["basic_eps"] == "income_real"


def test_query_planner_resolves_extended_financial_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_financial_package_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["total_assets"] == "balance_sheet_real"
    assert plan.field_bindings["net_cash_flows_oper_act"] == "cash_flow_real"
    assert plan.field_bindings["profit_notice_change_max"] == "profit_notice_real"
    assert plan.field_bindings["express_revenue"] == "profit_express_real"


def test_query_planner_resolves_basic_info_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_basic_info_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["security_name"] == "stock_basic_real"
    assert plan.field_bindings["security_type"] == "code_info_real"


def test_query_planner_resolves_backward_factor_and_snapshot_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")

    factor_intent = load_query_intent("examples/real_backward_factor_intent.yaml")
    factor_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    factor_plan = QueryPlanner(factor_registry).plan(factor_intent)
    assert factor_plan.field_bindings["backward_adj_factor"] == "backward_factor_real"

    snapshot_intent = load_query_intent("examples/real_snapshot_intent.yaml")
    snapshot_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    snapshot_plan = QueryPlanner(snapshot_registry).plan(snapshot_intent)
    assert snapshot_plan.field_bindings["last"] == "stock_snapshot_real"
    assert snapshot_plan.field_bindings["ask_price1"] == "stock_snapshot_real"


def test_query_planner_resolves_minute_and_calendar_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")

    minute_intent = load_query_intent("examples/real_minute_intent.yaml")
    minute_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    minute_plan = QueryPlanner(minute_registry).plan(minute_intent)
    assert minute_plan.field_bindings["minute_close"] == "stock_minute_real"

    calendar_intent = load_query_intent("examples/real_calendar_intent.yaml")
    calendar_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    calendar_plan = QueryPlanner(calendar_registry).plan(calendar_intent)
    assert calendar_plan.field_bindings["calendar_trade_date"] == "trade_calendar_real"


def test_query_planner_resolves_corporate_action_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_corporate_action_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["dividend_progress"] == "dividend_real"
    assert plan.field_bindings["holder_total_num"] == "holder_num_real"
    assert plan.field_bindings["share_holder_name"] == "share_holder_real"


def test_query_planner_resolves_right_issue_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_right_issue_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["right_issue_price"] == "right_issue_real"
    assert plan.field_bindings["right_issue_ratio"] == "right_issue_real"


def test_query_planner_resolves_equity_restricted_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_equity_restricted_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["restricted_share_ratio"] == "equity_restricted_real"
    assert plan.field_bindings["restricted_share_market_value"] == "equity_restricted_real"


def test_query_planner_resolves_equity_pledge_fields_from_catalog():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_pledge_freeze_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["pledge_holder_name"] == "equity_pledge_freeze_real"
    assert plan.field_bindings["pledge_total_pledge_shr"] == "equity_pledge_freeze_real"


def test_query_planner_resolves_snapshot_macro_fund_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")

    snapshot_intent = load_query_intent("examples/real_snapshot_fund_macro_intent.yaml")
    snapshot_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    snapshot_plan = QueryPlanner(snapshot_registry).plan(snapshot_intent)
    assert snapshot_plan.field_bindings["snapshot_last"] == "stock_snapshot_real"

    treasury_intent = load_query_intent("examples/real_macro_treasury_intent.yaml")
    treasury_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    treasury_plan = QueryPlanner(treasury_registry).plan(treasury_intent)
    assert treasury_plan.field_bindings["treasury_yield"] == "treasury_yield_real"

    fund_intent = load_query_intent("examples/real_fund_share_intent.yaml")
    fund_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    fund_plan = QueryPlanner(fund_registry).plan(fund_intent)
    assert fund_plan.field_bindings["fund_share"] == "fund_share_real"

    fund_iopv_intent = load_query_intent("examples/real_fund_iopv_intent.yaml")
    fund_iopv_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    fund_iopv_plan = QueryPlanner(fund_iopv_registry).plan(fund_iopv_intent)
    assert fund_iopv_plan.field_bindings["fund_iopv"] == "fund_iopv_real"


def test_query_planner_resolves_remaining_reference_and_etf_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")

    bj_intent = load_query_intent("examples/real_bj_mapping_intent.yaml")
    bj_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    bj_plan = QueryPlanner(bj_registry).plan(bj_intent)
    assert bj_plan.field_bindings["bj_new_code"] == "bj_code_mapping_real"

    etf_intent = load_query_intent("examples/real_etf_pcf_intent.yaml")
    etf_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    etf_plan = QueryPlanner(etf_registry).plan(etf_intent)
    assert etf_plan.field_bindings["etf_nav"] == "etf_pcf_real"

    constituent_intent = load_query_intent("examples/real_etf_constituent_intent.yaml")
    constituent_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    constituent_plan = QueryPlanner(constituent_registry).plan(constituent_intent)
    assert constituent_plan.field_bindings["etf_constituent_component_share"] == "etf_pcf_constituent_real"


def test_query_planner_resolves_remaining_industry_kzz_and_sync_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")

    industry_intent = load_query_intent("examples/real_industry_constituent_intent.yaml")
    industry_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    industry_plan = QueryPlanner(industry_registry).plan(industry_intent)
    assert industry_plan.field_bindings["industry_constituent_index_name"] == "industry_constituent_real"
    assert industry_plan.field_bindings["industry_level1_name"] == "industry_base_info_real"

    kzz_intent = load_query_intent("examples/real_kzz_intent.yaml")
    kzz_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    kzz_plan = QueryPlanner(kzz_registry).plan(kzz_intent)
    assert kzz_plan.field_bindings["kzz_clause_ini_conv_price"] == "kzz_issuance_real"
    assert kzz_plan.field_bindings["kzz_coupon_rate"] == "kzz_issuance_real"

    sync_intent = load_query_intent("examples/real_sync_task_intent.yaml")
    sync_registry = GraphRegistry(nodes, edges, field_catalog=fields)
    sync_plan = QueryPlanner(sync_registry).plan(sync_intent)
    assert sync_plan.field_bindings["sync_task_status"] == "sync_task_log_real"
    assert sync_plan.field_bindings["sync_task_finished_at"] == "sync_task_log_real"


def test_query_planner_resolves_block_trading_and_new_kzz_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)

    block_intent = load_query_intent("examples/real_block_trading_intent.yaml")
    block_plan = QueryPlanner(registry).plan(block_intent)
    assert block_plan.field_bindings["block_trade_frequency"] == "block_trading_real"
    assert "block_trade_avg_amount_per_freq" in block_plan.derived_fields

    kzz_clause_intent = load_query_intent("examples/real_kzz_clause_intent.yaml")
    kzz_clause_plan = QueryPlanner(registry).plan(kzz_clause_intent)
    assert kzz_clause_plan.field_bindings["kzz_call_trigger_ratio"] == "kzz_call_real"
    assert kzz_clause_plan.field_bindings["kzz_put_trigger_ratio"] == "kzz_put_real"

    kzz_conv_intent = load_query_intent("examples/real_kzz_conv_detail_intent.yaml")
    kzz_conv_plan = QueryPlanner(registry).plan(kzz_conv_intent)
    assert kzz_conv_plan.field_bindings["kzz_conv_code"] == "kzz_conv_real"
    assert kzz_conv_plan.field_bindings["kzz_conv_rule_price"] == "kzz_conv_real"
    assert kzz_conv_plan.field_bindings["kzz_is_forced"] == "kzz_conv_real"

    kzz_item_intent = load_query_intent("examples/real_kzz_put_call_item_intent.yaml")
    kzz_item_plan = QueryPlanner(registry).plan(kzz_item_intent)
    assert kzz_item_plan.field_bindings["kzz_item_is_put_item"] == "kzz_put_call_item_real"
    assert kzz_item_plan.field_bindings["kzz_item_call_tri_con_ins"] == "kzz_put_call_item_real"


def test_query_planner_resolves_kzz_alias_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    intent = load_query_intent("examples/real_kzz_alias_intent.yaml")
    plan = QueryPlanner(registry).plan(intent)

    assert plan.field_bindings["kzz_conv_rule_announce_date"] == "kzz_conv_real"
    assert plan.field_bindings["kzz_conv_rule_price"] == "kzz_conv_real"
    assert plan.field_bindings["kzz_conv_rule_start_date"] == "kzz_conv_real"
    assert plan.field_bindings["kzz_conv_rule_end_date"] == "kzz_conv_real"


def test_query_planner_resolves_long_hu_and_margin_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)

    long_hu_intent = load_query_intent("examples/real_long_hu_bang_intent.yaml")
    long_hu_plan = QueryPlanner(registry).plan(long_hu_intent)
    assert long_hu_plan.field_bindings["long_hu_reason_type_name"] == "long_hu_bang_real"
    assert "long_hu_net_amount" in long_hu_plan.derived_fields

    margin_detail_intent = load_query_intent("examples/real_margin_detail_intent.yaml")
    margin_detail_plan = QueryPlanner(registry).plan(margin_detail_intent)
    assert margin_detail_plan.field_bindings["margin_borrow_money_balance"] == "margin_detail_real"
    assert "margin_net_borrow_money" in margin_detail_plan.derived_fields

    margin_summary_intent = load_query_intent("examples/real_margin_summary_intent.yaml")
    margin_summary_plan = QueryPlanner(registry).plan(margin_summary_intent)
    assert margin_summary_plan.field_bindings["margin_summary_trade_balance"] == "margin_summary_real"
    assert "margin_summary_net_borrow_money" in margin_summary_plan.derived_fields


def test_long_hu_and_margin_fields_have_refined_semantic_roles():
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    by_name = {item.standard_field: item for item in fields}

    assert by_name["long_hu_reason_type_name"].field_role == "long_hu_reason_field"
    assert by_name["long_hu_trader_name"].field_role == "long_hu_trader_field"
    assert by_name["long_hu_net_amount"].field_role == "long_hu_derived_field"
    assert by_name["long_hu_reason_type_name"].path_domain == "trading_event"
    assert by_name["long_hu_reason_type_name"].path_group == "long_hu_bang"

    assert by_name["margin_borrow_money_balance"].field_role == "margin_detail_balance_field"
    assert by_name["margin_purch_with_borrow_money"].field_role == "margin_detail_flow_field"
    assert by_name["margin_net_borrow_money"].field_role == "margin_detail_derived_field"
    assert by_name["margin_borrow_money_balance"].path_domain == "margin"
    assert by_name["margin_borrow_money_balance"].path_group == "detail"

    assert by_name["margin_summary_trade_balance"].field_role == "margin_summary_balance_field"
    assert by_name["margin_summary_net_borrow_money"].field_role == "margin_summary_derived_field"
    assert by_name["margin_summary_trade_balance"].path_domain == "margin"
    assert by_name["margin_summary_trade_balance"].path_group == "summary"


def test_all_field_catalog_entries_have_chinese_descriptions():
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    missing = [item.standard_field for item in fields if not item.description_zh]
    assert not missing


def test_key_date_fields_have_time_semantics():
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    by_name = {item.standard_field: item for item in fields}

    assert by_name["calendar_trade_date"].time_semantics == "market_trade_date"
    assert by_name["ann_date"].time_semantics == "financial_announce_date"
    assert by_name["dividend_ann_date"].time_semantics == "corporate_action_announce_date"
    assert by_name["fund_share_ann_date"].time_semantics == "fund_announce_date"
    assert by_name["kzz_conv_rule_ann_date"].time_semantics == "kzz_announce_date"
    assert by_name["kzz_call_clause_start_date"].time_semantics == "effective_start_date"
    assert by_name["kzz_call_clause_end_date"].time_semantics == "effective_end_date"
    assert by_name["option_std_listed_date"].time_semantics == "listing_start_date"
    assert by_name["option_std_delist_date"].time_semantics == "listing_end_date"
    assert by_name["sync_task_run_date"].time_semantics == "system_run_date"


def test_lookahead_category_only_marks_published_event_fields_first_phase():
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    by_name = {item.standard_field: item for item in fields}

    assert by_name["ann_date"].lookahead_category == "published_event"
    assert by_name["dividend_ann_date"].lookahead_category == "published_event"
    assert by_name["kzz_conv_rule_ann_date"].lookahead_category == "published_event"
    assert by_name["kzz_item_con_put_start_date"].lookahead_category == "published_event"

    assert by_name["market_cap"].lookahead_category == "none"
    assert by_name["turnover_rate"].lookahead_category == "none"
    assert by_name["close_adj"].lookahead_category == "none"


def test_lookahead_safe_tightens_published_event_time_binding_only():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)

    event_intent = load_query_intent("examples/real_kzz_clause_intent.yaml")
    event_intent.safety.lookahead_safe = True
    event_sql = SqlRenderer(registry).render(QueryPlanner(registry).plan(event_intent))
    assert "toDate(b0.trade_time) > t1.begin_date" in event_sql
    assert "toDate(b0.trade_time) > t2.begin_date" in event_sql

    normal_intent = load_query_intent("examples/real_market_cap_intent.yaml")
    normal_intent.safety.lookahead_safe = True
    normal_sql = SqlRenderer(registry).render(QueryPlanner(registry).plan(normal_intent))
    assert "toDate(b0.trade_time) >= t1.change_date" in normal_sql


def test_industry_constituent_path_uses_constituent_route_not_weight_route():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent("examples/real_industry_constituent_intent.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)
    plan = QueryPlanner(registry).plan(intent)
    sql = SqlRenderer(registry).render(plan)

    assert plan.field_bindings["industry_level1_name"] == "industry_base_info_real"
    assert "LEFT JOIN starlight.ad_industry_constituent" in sql
    assert "LEFT JOIN starlight.ad_industry_weight" not in sql


def test_conflicting_industry_path_groups_raise(tmp_path):
    path = tmp_path / "conflicting_industry_paths.yaml"
    path.write_text(
        "\n".join(
            [
                "from: stock_daily_real",
                "select:",
                "  - code",
                "  - industry_index_code",
                "  - industry_constituent_index_code",
                "where:",
                "  mode: and",
                "  items:",
                "    - field: code",
                "      op: '='",
                "      value: '600602.SH'",
            ]
        ),
        encoding="utf-8",
    )

    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent(path)
    registry = GraphRegistry(nodes, edges, field_catalog=fields)

    try:
        QueryPlanner(registry).plan(intent)
    except ValueError as exc:
        assert "Conflicting path groups for domain 'industry'" in str(exc)
    else:
        raise AssertionError("Expected conflicting industry path groups to raise ValueError")


def test_conflicting_index_path_groups_raise(tmp_path):
    path = tmp_path / "conflicting_index_paths.yaml"
    path.write_text(
        "\n".join(
            [
                "from: stock_daily_real",
                "select:",
                "  - code",
                "  - index_weight",
                "  - index_name",
                "where:",
                "  mode: and",
                "  items:",
                "    - field: code",
                "      op: '='",
                "      value: '000004.SZ'",
            ]
        ),
        encoding="utf-8",
    )

    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    intent = load_query_intent(path)
    registry = GraphRegistry(nodes, edges, field_catalog=fields)

    try:
        QueryPlanner(registry).plan(intent)
    except ValueError as exc:
        assert "Conflicting path groups for domain 'index'" in str(exc)
    else:
        raise AssertionError("Expected conflicting index path groups to raise ValueError")


def test_query_planner_resolves_asset_derived_fields():
    nodes, edges = load_nodes_and_edges("examples/real_combined_graph.yaml")
    fields = load_field_catalog("examples/real_combined_fields.yaml")
    registry = GraphRegistry(nodes, edges, field_catalog=fields)

    etf_intent = load_query_intent("examples/real_etf_derived_intent.yaml")
    etf_plan = QueryPlanner(registry).plan(etf_intent)
    assert "etf_cash_component_ratio" in etf_plan.derived_fields
    assert "etf_limit_spread" in etf_plan.derived_fields

    kzz_intent = load_query_intent("examples/real_kzz_derived_intent.yaml")
    kzz_plan = QueryPlanner(registry).plan(kzz_intent)
    assert "kzz_coupon_rate_decimal" in kzz_plan.derived_fields
    assert "kzz_clause_ini_conv_premium_ratio_decimal" in kzz_plan.derived_fields

    option_intent = load_query_intent("examples/real_option_derived_intent.yaml")
    option_plan = QueryPlanner(registry).plan(option_intent)
    assert "option_std_contract_value_per_multiplier" in option_plan.derived_fields
    assert "option_std_option_strike_price_num" in option_plan.derived_fields
