from aiquantbase.nl_intent import enrich_query_intent_with_aliases


def test_enrich_query_intent_with_aliases_adds_missing_fields():
    intent = {
        "from": "stock_daily_real",
        "select": ["close_adj", "pre_close", "is_st"],
    }
    enriched = enrich_query_intent_with_aliases(
        intent,
        "查询收盘价、前复权收盘价、昨收和是否ST",
        {"close", "close_adj", "pre_close", "is_st"},
    )

    assert "close" in enriched["select"]
    assert "close_adj" in enriched["select"]
