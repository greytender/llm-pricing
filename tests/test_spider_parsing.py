from pathlib import Path

from src.spider import parse_pricing_html


def test_parse_generic_table_basic():
    html = """
    <html>
      <body>
        <table>
          <thead>
            <tr><th>Model</th><th>Input</th><th>Output</th></tr>
          </thead>
          <tbody>
            <tr><td>test-model</td><td>$0.001 / 1K tokens</td><td>$0.002 / 1K tokens</td></tr>
          </tbody>
        </table>
      </body>
    </html>
    """
    config = {"parser": "table"}
    records = parse_pricing_html("TestCo", html, config)
    assert len(records) == 1
    rec = records[0]
    assert rec["company"] == "TestCo"
    assert rec["model_name"] == "test-model"
    assert rec["input_raw"] == "$0.001 / 1K tokens"
    assert rec["output_raw"] == "$0.002 / 1K tokens"

