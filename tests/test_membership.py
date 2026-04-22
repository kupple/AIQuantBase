from pathlib import Path

from aiquantbase import ApplicationRuntime, GraphRuntime
from aiquantbase.executor import QueryExecutionResult
from aiquantbase.membership import (
    delete_source,
    filter_symbols_by_membership,
    import_membership_payload,
    list_domains,
    list_members,
    list_relations,
    list_sources,
    list_taxonomies,
    patch_relation,
    preview_source_rows,
    preview_source_rows_with_lookup,
    query_membership,
    resolve_membership_target,
    save_membership_workspace,
    upsert_member,
    upsert_relation,
    upsert_source,
    upsert_taxonomy,
)


class MembershipExecutor:
    def execute_sql(self, sql: str) -> QueryExecutionResult:
        return QueryExecutionResult(
            sql=sql,
            data=[
                {"code": "000001.SZ", "trade_time": "2024-01-02 00:00:00", "close": 10.0},
                {"code": "000002.SZ", "trade_time": "2024-01-02 00:00:00", "close": 12.0},
            ],
            rows=2,
            statistics={},
            meta=[],
        )


def _membership_path(tmp_path: Path) -> Path:
    path = tmp_path / 'membership.yaml'
    save_membership_workspace(
        {
            'version': 1,
            'taxonomies': [
                {
                    'domain': 'board',
                    'taxonomy': 'exchange_board',
                    'display_name': '交易所板块',
                    'status': 'enabled',
                }
            ],
            'members': [
                {
                    'domain': 'board',
                    'taxonomy': 'exchange_board',
                    'member_code': 'sme',
                    'member_name': '中小板',
                },
                {
                    'domain': 'board',
                    'taxonomy': 'exchange_board',
                    'member_code': 'gem',
                    'member_name': '创业板',
                },
            ],
            'relations': [
                {
                    'id': 'rel_sme',
                    'security_code': '000001.SZ',
                    'security_type': 'stock',
                    'domain': 'board',
                    'taxonomy': 'exchange_board',
                    'member_code': 'sme',
                    'member_name': '中小板',
                    'effective_from': '2020-01-01',
                    'effective_to': '',
                    'source_system': 'manual',
                    'status': 'enabled',
                },
                {
                    'id': 'rel_gem',
                    'security_code': '000002.SZ',
                    'security_type': 'stock',
                    'domain': 'board',
                    'taxonomy': 'exchange_board',
                    'member_code': 'gem',
                    'member_name': '创业板',
                    'effective_from': '2020-01-01',
                    'effective_to': '',
                    'source_system': 'manual',
                    'status': 'enabled',
                },
            ],
        },
        path,
    )
    return path


def test_membership_discovery_and_query(tmp_path: Path):
    path = _membership_path(tmp_path)

    assert list_domains(path)[0]['domain'] == 'board'
    assert list_taxonomies(path, domain='board')[0]['taxonomy'] == 'exchange_board'
    assert len(list_members(path, domain='board', taxonomy='exchange_board')) == 2
    assert len(list_relations(path, security_code='000001.SZ', as_of_date='2024-01-02')) == 1
    assert query_membership('000001.SZ', as_of_date='2024-01-02', path=path)[0]['member_code'] == 'sme'


def test_membership_filter_symbols(tmp_path: Path):
    path = _membership_path(tmp_path)
    result = filter_symbols_by_membership(
        {
            'include': [{'domain': 'board', 'taxonomy': 'exchange_board', 'member_code': 'sme'}],
            'exclude': [{'domain': 'board', 'taxonomy': 'exchange_board', 'member_code': 'gem'}],
        },
        as_of_date='2024-01-02',
        path=path,
        security_type='stock',
    )

    assert result['ok'] is True
    assert result['symbols'] == ['000001.SZ']


def test_resolve_membership_target(tmp_path: Path):
    path = tmp_path / 'membership.yaml'
    save_membership_workspace(
        {
            'version': 1,
            'sources': [
                {
                    'source_name': 'index_source',
                    'source_kind': 'relation',
                    'database': 'starlight',
                    'table': 'ad_index_constituent',
                    'domain': 'index',
                    'taxonomy': 'csi_index',
                    'security_code_field': 'con_code',
                    'member_code_field': 'index_code',
                    'member_name_field': 'index_name',
                }
            ],
            'members': [
                {
                    'domain': 'index',
                    'taxonomy': 'csi_index',
                    'member_code': '399101.SZ',
                    'member_name': '中小综指',
                    'status': 'enabled',
                }
            ],
        },
        path,
    )

    resolved = resolve_membership_target(
        domain='index',
        member_code='399101.SZ',
        path=path,
    )

    assert resolved['taxonomy'] == 'csi_index'
    assert resolved['member_name'] == '中小综指'
    assert resolved['source_count'] == 1


def test_resolve_membership_target_rejects_ambiguous_match(tmp_path: Path):
    path = tmp_path / 'membership.yaml'
    save_membership_workspace(
        {
            'version': 1,
            'members': [
                {
                    'domain': 'index',
                    'taxonomy': 'csi_index',
                    'member_code': '399101.SZ',
                    'member_name': '中小综指',
                    'status': 'enabled',
                },
                {
                    'domain': 'index',
                    'taxonomy': 'legacy_index',
                    'member_code': '399101.SZ',
                    'member_name': '中小综指',
                    'status': 'enabled',
                },
            ],
        },
        path,
    )

    try:
        resolve_membership_target(
            domain='index',
            member_code='399101.SZ',
            path=path,
        )
    except ValueError as exc:
        assert 'ambiguous membership target' in str(exc)
    else:
        raise AssertionError('expected ValueError for ambiguous membership target')


def test_membership_mutations_and_import(tmp_path: Path):
    path = tmp_path / 'membership.yaml'

    upsert_taxonomy(
        {
            'domain': 'index',
            'taxonomy': 'csi_index',
            'display_name': '中证指数',
        },
        path,
    )
    upsert_member(
        {
            'domain': 'index',
            'taxonomy': 'csi_index',
            'member_code': '000300.SH',
            'member_name': '沪深300',
        },
        path,
    )
    relation = upsert_relation(
        {
            'security_code': '000001.SZ',
            'security_type': 'stock',
            'domain': 'index',
            'taxonomy': 'csi_index',
            'member_code': '000300.SH',
            'effective_from': '2024-01-01',
        },
        path,
    )

    patched = patch_relation({'id': relation['id'], 'status': 'disabled'}, path)
    assert patched['status'] == 'disabled'

    workspace = import_membership_payload(
        {
            'sources': [
                {
                    'source_name': 'index_source',
                    'source_kind': 'relation',
                    'database': 'starlight',
                    'table': 'ad_index_components',
                    'domain': 'index',
                    'taxonomy': 'csi_index',
                    'security_code_field': 'code',
                    'member_code_field': 'index_code',
                    'member_name_field': 'index_name',
                    'effective_from_field': 'trade_date',
                }
            ],
            'members': [
                {
                    'domain': 'index',
                    'taxonomy': 'csi_index',
                    'member_code': '000905.SH',
                    'member_name': '中证500',
                }
            ]
        },
        path,
    )
    assert list_sources(path)[0]['source_name'] == 'index_source'
    assert any(item['member_code'] == '000905.SH' for item in workspace['members'])


def test_membership_source_preview(tmp_path: Path):
    source = upsert_source(
        {
            'source_name': 'index_members',
            'source_kind': 'relation',
            'database': 'starlight',
            'table': 'ad_index_components',
            'domain': 'index',
            'taxonomy': 'csi_index',
            'security_code_field': 'code',
            'member_code_field': 'index_code',
            'effective_from_field': 'trade_date',
        },
        tmp_path / 'membership.yaml',
    )
    preview = preview_source_rows(
        source,
        [
            {
                'code': '000001.SZ',
                'index_code': '000300.SH',
                'index_name': '沪深300',
                'trade_date': '2024-01-02',
            }
        ],
    )

    assert preview['taxonomy_preview']['taxonomy'] == 'csi_index'
    assert preview['member_preview'][0]['member_code'] == '000300.SH'
    assert preview['relation_preview'][0]['security_code'] == '000001.SZ'


def test_membership_relation_source_preview_with_lookup_table(tmp_path: Path):
    source = upsert_source(
        {
            'source_name': 'industry_constituent',
            'source_kind': 'relation',
            'database': 'starlight',
            'table': 'ad_industry_constituent',
            'domain': 'industry',
            'taxonomy': 'sw2021',
            'security_code_field': 'con_code',
            'member_code_field': 'industry_code',
            'effective_from_field': 'in_date',
            'effective_to_field': 'out_date',
            'lookup_database': 'starlight',
            'lookup_table': 'ad_industry_base_info',
            'lookup_source_field': 'industry_code',
            'lookup_target_field': 'industry_code',
            'lookup_member_name_field': 'level3_name',
            'attribute_mappings': [
                {'key': 'level1_name', 'label': '一级行业名称', 'field': 'level1_name'},
                {'key': 'level2_name', 'label': '二级行业名称', 'field': 'level2_name'},
                {'key': 'level3_name', 'label': '三级行业名称', 'field': 'level3_name'},
            ],
        },
        tmp_path / 'membership.yaml',
    )
    preview = preview_source_rows_with_lookup(
        source,
        [
            {
                'con_code': '000001.SZ',
                'industry_code': '801080',
                'in_date': '2024-01-01',
                'out_date': '',
            }
        ],
        [
            {
                'industry_code': '801080',
                'level1_name': '电子',
                'level2_name': '半导体',
                'level3_name': '电子元件',
            }
        ],
    )

    assert preview['relation_preview'][0]['member_name'] == '电子元件'
    assert preview['relation_preview'][0]['level1_name'] == '电子'
    assert preview['attribute_columns'][0]['label'] == '一级行业名称'
    assert preview['member_preview'][0]['level2_name'] == '半导体'


def test_membership_delete_source(tmp_path: Path):
    path = tmp_path / 'membership.yaml'
    source = upsert_source(
        {
            'source_name': 'index_members',
            'database': 'starlight',
            'table': 'ad_index_components',
            'domain': 'index',
            'taxonomy': 'csi_index',
            'security_code_field': 'code',
            'member_code_field': 'index_code',
            'member_name_field': 'index_name',
            'effective_from_field': 'trade_date',
        },
        path,
    )

    removed = delete_source(source['id'], path)
    assert removed['id'] == source['id']
    assert list_sources(path) == []


def test_membership_create_source_rejects_duplicate_name(tmp_path: Path):
    path = tmp_path / 'membership.yaml'
    upsert_source(
        {
            'source_name': 'industry_members',
            'source_kind': 'relation',
            'database': 'starlight',
            'table': 'ad_industry_constituent',
            'domain': 'industry',
            'taxonomy': 'sw2021',
            'security_code_field': 'con_code',
            'member_code_field': 'industry_code',
            'effective_from_field': 'in_date',
        },
        path,
    )

    try:
        upsert_source(
            {
                'source_name': 'industry_members',
                'source_kind': 'member_dimension',
                'database': 'starlight',
                'table': 'ad_industry_base_info',
                'domain': 'industry',
                'taxonomy': 'sw2021',
                'member_code_field': 'industry_code',
                'member_name_field': 'level3_name',
            },
            path,
        )
    except ValueError as exc:
        assert 'source_name already exists' in str(exc)
    else:
        raise AssertionError('expected duplicate source_name to raise ValueError')


def test_membership_member_dimension_source_preview(tmp_path: Path):
    source = upsert_source(
        {
            'source_name': 'industry_base_info',
            'source_kind': 'member_dimension',
            'database': 'starlight',
            'table': 'ad_industry_base_info',
            'domain': 'industry',
            'taxonomy': 'sw2021',
            'member_code_field': 'industry_code',
            'member_name_field': 'level3_name',
            'attribute_mappings': [
                {'key': 'level1_name', 'label': '一级行业名称', 'field': 'level1_name'},
                {'key': 'level2_name', 'label': '二级行业名称', 'field': 'level2_name'},
                {'key': 'level3_name', 'label': '三级行业名称', 'field': 'level3_name'},
            ],
        },
        tmp_path / 'membership.yaml',
    )
    preview = preview_source_rows(
        source,
        [
            {
                'industry_code': '801080',
                'level1_name': '电子',
                'level2_name': '半导体',
                'level3_name': '电子元件',
            }
        ],
    )

    assert preview['taxonomy_preview']['source_kind'] == 'member_dimension'
    assert preview['member_preview'][0]['member_code'] == '801080'
    assert preview['member_preview'][0]['level1_name'] == '电子'
    assert preview['member_preview'][0]['level2_name'] == '半导体'
    assert preview['member_preview'][0]['level3_name'] == '电子元件'
    assert preview['attribute_columns'][2]['label'] == '三级行业名称'
    assert preview['relation_preview'] == []


def test_runtime_query_daily_supports_memberships(tmp_path: Path):
    path = _membership_path(tmp_path)
    runtime = GraphRuntime.from_defaults()
    runtime.executor = MembershipExecutor()

    result = runtime.query_daily(
        fields=['close'],
        start='2024-01-01 00:00:00',
        end='2024-01-02 23:59:59',
        memberships={
            'include': [{'domain': 'board', 'taxonomy': 'exchange_board', 'member_code': 'sme'}],
        },
        membership_path=path,
    )

    assert result['ok'] is True
    assert "b0.code = '000001.SZ'" in result['debug']['sql']


def test_application_runtime_membership_helpers(tmp_path: Path):
    path = _membership_path(tmp_path)
    runtime = ApplicationRuntime.from_defaults()

    query_result = runtime.query_membership('000001.SZ', as_of_date='2024-01-02', membership_path=path)
    assert query_result['count'] == 1

    filter_result = runtime.filter_symbols_by_membership(
        {'include': [{'domain': 'board', 'taxonomy': 'exchange_board', 'member_code': 'gem'}]},
        as_of_date='2024-01-02',
        membership_path=path,
        security_type='stock',
    )
    assert filter_result['symbols'] == ['000002.SZ']
