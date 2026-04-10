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
    assert plan.field_bindings["index_name"] == "index_constituent_real"


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
