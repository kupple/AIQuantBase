from aiquantbase.llm import _extract_json


def test_extract_json_direct():
    payload = _extract_json('{"a": 1, "b": 2}')
    assert payload == {"a": 1, "b": 2}


def test_extract_json_wrapped():
    payload = _extract_json("text before {\"a\": 1} text after")
    assert payload == {"a": 1}
