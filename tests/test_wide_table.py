from pathlib import Path

from aiquantbase.wide_table import (
    delete_wide_table,
    export_wide_table_yaml,
    get_wide_table_summary,
    list_wide_tables,
    save_wide_table_workspace,
    upsert_wide_table,
)


def test_wide_table_crud_and_export(tmp_path: Path):
    path = tmp_path / 'wide_tables.yaml'
    save_wide_table_workspace({'version': 1, 'wide_tables': []}, path)

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
        path,
    )

    items = list_wide_tables(path, source_node='stock_daily_real')
    assert len(items) == 1
    assert items[0]['engine'] == 'ReplacingMergeTree'

    exported = export_wide_table_yaml(design['id'], path)
    assert 'wide_table:' in exported
    assert 'source_node: stock_daily_real' in exported
    assert 'engine: ReplacingMergeTree' in exported

    removed = delete_wide_table(design['id'], path)
    assert removed['id'] == design['id']
    assert list_wide_tables(path) == []


def test_wide_table_rejects_duplicate_name(tmp_path: Path):
    path = tmp_path / 'wide_tables.yaml'
    save_wide_table_workspace({'version': 1, 'wide_tables': []}, path)

    upsert_wide_table(
        {
            'name': 'stock_minute_exec_wide',
            'source_node': 'stock_minute_real',
            'target_database': 'research',
            'target_table': 'stock_minute_exec_wide',
            'engine': 'Memory',
            'fields': ['code', 'trade_time', 'open'],
            'key_fields': ['code', 'trade_time'],
        },
        path,
    )

    try:
        upsert_wide_table(
            {
                'name': 'stock_minute_exec_wide',
                'source_node': 'stock_minute_real',
                'target_database': 'research',
                'target_table': 'stock_minute_exec_wide_v2',
                'engine': 'Memory',
                'fields': ['code', 'trade_time', 'close'],
                'key_fields': ['code', 'trade_time'],
            },
            path,
        )
    except ValueError as exc:
        assert 'wide table name already exists' in str(exc)
    else:
        raise AssertionError('expected duplicate wide table name to raise ValueError')


def test_wide_table_summary(tmp_path: Path):
    path = tmp_path / 'wide_tables.yaml'
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
