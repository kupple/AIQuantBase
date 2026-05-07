from __future__ import annotations

from pathlib import Path

from aiquantbase.config import load_nodes_and_edges


def test_load_node_record_grain_detail_rollup(tmp_path: Path) -> None:
    graph_path = tmp_path / "graph.yaml"
    graph_path.write_text(
        """
nodes:
- name: long_hu_bang_real
  table: starlight.ad_long_hu_bang
  entity_keys:
  - market_code
  time_key: trade_date
  fields:
  - market_code
  - trade_date
  - buy_amount
  - sell_amount
  record_grain: detail
  detail_keys:
  - reason_type
  - trader_name
  default_rollup: entity_daily
  rollups:
  - name: entity_daily
    group_by:
    - market_code
    - trade_date
    aggregations:
    - name: buy_amount
      source: buy_amount
      func: sum
      null_policy: zero
edges: []
""",
        encoding="utf-8",
    )

    nodes, edges = load_nodes_and_edges(graph_path)

    assert edges == []
    assert len(nodes) == 1
    node = nodes[0]
    assert node.record_grain == "detail"
    assert node.detail_keys == ["reason_type", "trader_name"]
    assert node.default_rollup == "entity_daily"
    assert node.rollups[0]["aggregations"][0]["func"] == "sum"
