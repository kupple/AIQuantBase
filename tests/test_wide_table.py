from pathlib import Path

from aiquantbase.config import dump_yaml
from aiquantbase.wide_table import (
    build_wide_table_export_payload,
    delete_wide_table,
    get_wide_table_summary,
    list_wide_tables,
    save_wide_table_workspace,
    upsert_wide_table,
)


def _write_wide_table_graph(path: Path) -> None:
    path.write_text(
        dump_yaml(
            {
                'nodes': [
                    {
                        'name': 'stock_daily_real',
                        'table': 'starlight.ad_market_kline_daily',
                        'entity_keys': ['code'],
                        'time_key': 'trade_time',
                        'grain': 'daily',
                        'fields': ['code', 'trade_time', 'close', 'market_cap', 'industry_name'],
                    }
                ],
                'edges': [],
            }
        ),
        encoding='utf-8',
    )


def _write_wide_table_fields(path: Path) -> None:
    path.write_text(
        dump_yaml(
            {
                'fields': [
                    {
                        'standard_field': field_name,
                        'source_node': 'stock_daily_real',
                        'source_field': field_name,
                        'field_role': 'metric',
                        'base_node': 'stock_daily_real',
                    }
                    for field_name in ['code', 'trade_time', 'close', 'market_cap', 'industry_name']
                ]
            }
        ),
        encoding='utf-8',
    )


def test_wide_table_crud_and_export_payload(tmp_path: Path):
    graph_path = tmp_path / 'graph.yaml'
    fields_path = tmp_path / 'fields.yaml'
    _write_wide_table_graph(graph_path)
    _write_wide_table_fields(fields_path)

    design = upsert_wide_table(
        {
            'name': 'stock_daily_research_wide',
            'source_node': 'stock_daily_real',
            'target_database': 'research',
            'target_table': 'stock_daily_research_wide',
            'engine': 'ReplacingMergeTree',
            'fields': ['code', 'trade_time', 'close', 'market_cap', 'industry_name'],
            'key_fields': ['code', 'trade_time'],
            'partition_by': ['toYYYYMM(trade_time)'],
            'order_by': ['code', 'trade_time'],
            'version_field': 'updated_at',
        },
        graph_path=graph_path,
    )

    items = list_wide_tables(graph_path=graph_path, source_node='stock_daily_real')
    assert len(items) == 1
    assert items[0]['engine'] == 'ReplacingMergeTree'

    exported = build_wide_table_export_payload(design['id'], graph_path=graph_path, fields_path=fields_path)
    assert exported['wide_table']['source_node'] == 'stock_daily_real'
    assert exported['wide_table']['target']['engine'] == 'ReplacingMergeTree'
    assert exported['materialization_bundle']['query_plan']
    assert exported['materialization_bundle']['base_context']['base_table'] == 'starlight.ad_market_kline_daily'

    removed = delete_wide_table(design['id'], graph_path=graph_path)
    assert removed['id'] == design['id']
    assert list_wide_tables(graph_path=graph_path) == []


def test_wide_table_updates_existing_name(tmp_path: Path):
    graph_path = tmp_path / 'graph.yaml'
    graph_path.write_text(dump_yaml({'nodes': [], 'edges': []}), encoding='utf-8')

    created = upsert_wide_table(
        {
            'name': 'stock_minute_exec_wide',
            'source_node': 'stock_minute_real',
            'target_database': 'research',
            'target_table': 'stock_minute_exec_wide',
            'engine': 'Memory',
            'fields': ['code', 'trade_time', 'open'],
            'key_fields': ['code', 'trade_time'],
        },
        graph_path=graph_path,
    )

    updated = upsert_wide_table(
        {
            'id': created['id'],
            'name': 'stock_minute_exec_wide',
            'source_node': 'stock_minute_real',
            'target_database': 'research',
            'target_table': 'stock_minute_exec_wide_v2',
            'engine': 'Memory',
            'fields': ['code', 'trade_time', 'close'],
            'key_fields': ['code', 'trade_time'],
        },
        graph_path=graph_path,
    )

    items = list_wide_tables(graph_path=graph_path)
    assert updated['target_table'] == 'stock_minute_exec_wide_v2'
    assert len(items) == 1
    assert items[0]['target_table'] == 'stock_minute_exec_wide_v2'


def test_wide_table_summary(tmp_path: Path):
    path = tmp_path / 'graph.yaml'
    save_wide_table_workspace(
        {
            'version': 1,
            'wide_tables': [
                {
                    'name': 'demo_wide',
                    'source_node': 'stock_daily_real',
                    'target_database': 'research',
                    'target_table': 'demo_wide',
                    'engine': 'Memory',
                    'fields': ['code', 'trade_time', 'close'],
                    'key_fields': ['code', 'trade_time'],
                    'status': 'enabled',
                }
            ],
        },
        path,
    )
    summary = get_wide_table_summary(path)
    assert summary['wide_table_count'] == 1
    assert summary['enabled_count'] == 1
