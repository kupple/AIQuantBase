import json

from aiquantbase.discovery import (
    EdgeInferenceService,
    FieldCatalogExportService,
    FieldCatalogInferenceService,
    GraphExportService,
    GraphInferenceService,
    NodeInferenceService,
    ResearchPackageService,
)


def test_node_inference_demo():
    with open("examples/discovered_schema_sample.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    result = NodeInferenceService().infer_nodes(payload)
    nodes = {item["name"]: item for item in result["inferred_nodes"]}

    assert "market_kline_daily" in nodes
    assert nodes["market_kline_daily"]["candidate_node_type"] == "market_timeseries"
    assert nodes["adj_factor"]["candidate_node_type"] == "adjustment_factor"
    assert nodes["history_stock_status"]["candidate_node_type"] == "status_snapshot"


def test_edge_inference_demo():
    with open("examples/discovered_schema_sample.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    inferred_nodes = NodeInferenceService().infer_nodes(payload)
    inferred_edges = EdgeInferenceService().infer_edges(inferred_nodes)

    edge_names = {item["name"] for item in inferred_edges["inferred_edges"]}
    assert "market_kline_daily_to_adj_factor" in edge_names
    assert "market_kline_daily_to_history_stock_status" in edge_names


def test_graph_inference_demo():
    with open("examples/discovered_schema_sample.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    inferred_nodes = NodeInferenceService().infer_nodes(payload)
    inferred_edges = EdgeInferenceService().infer_edges(inferred_nodes)
    graph_payload = GraphInferenceService().infer_graph(inferred_edges)

    candidate_graph = graph_payload["candidate_graph"]
    assert len(candidate_graph["nodes"]) >= 3
    assert len(candidate_graph["edges"]) >= 2
    assert candidate_graph["edges"][0]["relation_type"] == "direct"


def test_graph_export_demo():
    with open("examples/discovered_schema_sample.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    inferred_nodes = NodeInferenceService().infer_nodes(payload)
    inferred_edges = EdgeInferenceService().infer_edges(inferred_nodes)
    graph_payload = GraphInferenceService().infer_graph(inferred_edges)
    exported = GraphExportService().export_graph_yaml_payload(graph_payload)

    assert "nodes" in exported
    assert "edges" in exported
    assert exported["nodes"][0]["table"].startswith("starlight.")


def test_field_catalog_inference_and_export():
    with open("examples/discovered_schema_sample.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    inferred_nodes = NodeInferenceService().infer_nodes(payload)
    inferred_edges = EdgeInferenceService().infer_edges(inferred_nodes)
    graph_payload = GraphInferenceService().infer_graph(inferred_edges)
    field_payload = FieldCatalogInferenceService().infer_field_catalog(graph_payload)
    exported = FieldCatalogExportService().export_field_catalog_yaml_payload(field_payload)

    standard_fields = {item["standard_field"] for item in field_payload["candidate_field_catalog"]}
    assert "forward_adj_factor" in standard_fields
    assert "close_adj" in standard_fields
    assert "open_adj" in standard_fields
    assert "high_adj" in standard_fields
    assert "low_adj" in standard_fields
    assert "gap_open_return" in standard_fields
    assert "is_st" in standard_fields
    assert "close" in standard_fields
    assert "fields" in exported


def test_research_package_service():
    with open("examples/discovered_schema_sample.json", "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    inferred_nodes = NodeInferenceService().infer_nodes(payload)
    inferred_edges = EdgeInferenceService().infer_edges(inferred_nodes)
    graph_payload = GraphInferenceService().infer_graph(inferred_edges)
    field_payload = FieldCatalogInferenceService().infer_field_catalog(graph_payload)
    package_payload = ResearchPackageService().build_candidate_package(graph_payload, field_payload)

    assert package_payload["summary"]["node_count"] >= 3
    assert package_payload["summary"]["edge_count"] >= 2
    assert package_payload["summary"]["field_count"] >= 3
