from src.process import normalize_price_string, transform_records


def test_normalize_price_string_basic():
    assert normalize_price_string("  $0.002  / 1K   tokens ") == "$0.002 / 1K tokens"


def test_normalize_price_string_none():
    assert normalize_price_string(None) is None


def test_transform_records_fills_missing_output():
    raw = [
        {
            "company": "OpenAI",
            "model_name": "gpt-4.1",
            "input_raw": "$0.002 / 1K tokens",
            "output_raw": None,
        }
    ]
    res = transform_records(raw)
    assert len(res) == 1
    rec = res[0]
    assert rec["company"] == "OpenAI"
    assert rec["model_name"] == "gpt-4.1"
    assert rec["input_price"] == "$0.002 / 1K tokens"
    assert rec["output_price"] == "$0.002 / 1K tokens"

