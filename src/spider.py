from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
import yaml


REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


DEFAULT_HEADERS = {
    # A reasonably recent Chrome UA; update as needed.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


@dataclass
class RawPriceRecord:
    company: str
    model_name: str
    input_raw: Optional[str]
    output_raw: Optional[str]
    extra: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        return data


def build_headers(provider: Dict[str, Any]) -> Dict[str, str]:
    """Build realistic browser headers, allowing per-provider overrides."""
    headers = DEFAULT_HEADERS.copy()
    referer = provider.get("referer")
    if referer:
        headers["Referer"] = referer
    # Allow full override/extension via config if needed.
    for key, value in provider.get("headers", {}).items():
        headers[key] = value
    return headers


def _random_human_delay(min_s: float = 1.0, max_s: float = 3.0) -> None:
    delay = random.uniform(min_s, max_s)
    print(f"[delay] Sleeping {delay:.2f}s to mimic human behaviour")
    time.sleep(delay)


def fetch_with_requests(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    max_retries: int = MAX_RETRIES,
) -> str:
    """Fetch a page using requests with retries and realistic headers."""
    session = requests.Session()
    if headers:
        session.headers.update(headers)

    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        _random_human_delay()
        print(f"[requests] Fetching URL (attempt {attempt}/{max_retries}): {url}")
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            print(f"[requests] Success {url} (status={response.status_code})")
            return response.text
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            print(
                f"[requests][WARN] Timeout fetching {url} on attempt {attempt}/"
                f"{max_retries} after {timeout}s: {exc}"
            )
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            print(
                f"[requests][WARN] Error fetching {url} on attempt {attempt}/"
                f"{max_retries}: {exc}"
            )

        if attempt < max_retries:
            print(f"[requests] Retrying after {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)

    # All retries exhausted
    msg = f"Max retries exceeded for {url}"
    print(f"[requests][ERROR] {msg}")
    if last_exc is not None:
        raise RuntimeError(f"{msg}: {last_exc}") from last_exc
    raise RuntimeError(msg)


def fetch_with_playwright(
    url: str,
    *,
    wait_selector: str = "table",
    timeout_ms: int = REQUEST_TIMEOUT_SECONDS * 1000,
) -> str:
    """Fetch a page using Playwright, waiting for a key selector to appear."""
    _random_human_delay()
    print(f"[playwright] Launching browser for {url}, waiting for selector '{wait_selector}'")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(url, timeout=timeout_ms, wait_until="networkidle")
                page.wait_for_selector(wait_selector, timeout=timeout_ms)
                html = page.content()
                print(f"[playwright] Successfully loaded {url} with selector '{wait_selector}'")
                return html
            except PlaywrightTimeoutError as exc:
                print(
                    f"[playwright][ERROR] Timeout while loading {url} "
                    f"or waiting for selector '{wait_selector}': {exc}"
                )
                raise
            finally:
                browser.close()
    except Exception as exc:  # noqa: BLE001
        print(f"[playwright][ERROR] Failed to fetch {url}: {exc}")
        raise


def fetch_page_html(provider: Dict[str, Any]) -> str:
    """Unified entry to fetch HTML for a provider (requests or Playwright)."""
    url = provider.get("url")
    if not url:
        raise ValueError("Provider configuration missing 'url'")

    render_mode = provider.get("render", "static").lower()
    wait_selector = provider.get("wait_selector", "table")

    if render_mode == "playwright":
        return fetch_with_playwright(url, wait_selector=wait_selector)

    # Default: use requests for static pages.
    headers = build_headers(provider)
    return fetch_with_requests(url, headers=headers)


def _warn_if_none(field_name: str, value: Optional[str], html_snippet: str) -> None:
    """Print a warning if a field value is None, with a short HTML snippet."""
    if value is None:
        snippet = html_snippet.strip().replace("\n", " ")
        if len(snippet) > 120:
            snippet = snippet[:117] + "..."
        print(f"[WARN] 字段 '{field_name}' 提取失败，当前 HTML 片段：{snippet}")


def _extract_model_name_from_cells(cells: List[str]) -> Optional[str]:
    if not cells:
        return None
    name = (cells[0] or "").strip()
    return name or None


def _extract_input_price_from_cells(cells: List[str], input_idx: Optional[int]) -> Optional[str]:
    if input_idx is None or input_idx < 0 or input_idx >= len(cells):
        return None
    value = (cells[input_idx] or "").strip()
    return value or None


def _extract_output_price_from_cells(cells: List[str], output_idx: Optional[int]) -> Optional[str]:
    if output_idx is None or output_idx < 0 or output_idx >= len(cells):
        return None
    value = (cells[output_idx] or "").strip()
    return value or None


def _parse_openai(html: str, company: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Very lightweight, best-effort parsing for OpenAI pricing page.

    NOTE: The official pricing page structure may change at any time.
    This parser aims to be robust but is not guaranteed to cover all models.
    """
    print(f"[parse_openai] 正在解析 {company}，页面长度：{len(html)} 字符")
    soup = BeautifulSoup(html, "html.parser")
    records: List[Dict[str, Any]] = []

    css = config.get("css_selectors", {}) or {}
    table_selector = css.get("table", "table")
    header_selector = css.get("header_row", "thead tr")
    body_row_selector = css.get("body_rows", "tbody tr")

    print(
        f"[parse_openai] {company}: table_selector='{table_selector}', "
        f"header_selector='{header_selector}', body_row_selector='{body_row_selector}'"
    )

    # Heuristic: look for tables where header row mentions tokens / input / output.
    tables = soup.select(table_selector)
    print(f"[parse_openai] {company}: found {len(tables)} table(s)")

    container_count = 0

    for table in tables:
        header_row = table.select_one(header_selector)
        if not header_row:
            continue
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all("th")]
        if not headers:
            continue
        if not any("token" in h for h in headers):
            continue

        rows = table.select(body_row_selector) or table.find_all("tr")
        container_count += len(rows)
        print(f"[parse_openai] {company}: rows in current table = {len(rows)}")

        for row in rows:
            tds = row.find_all("td")
            cells = [td.get_text(" ", strip=True) for td in tds]
            if len(cells) < 2:
                continue

            model_name = _extract_model_name_from_cells(cells)
            # Simple heuristic: try to map second/third columns to input/output.
            input_raw = cells[1] if len(cells) >= 2 else None
            output_raw = cells[2] if len(cells) >= 3 else None

            html_snippet = row.prettify() if hasattr(row, "prettify") else str(row)
            _warn_if_none("model_name", model_name, html_snippet)
            _warn_if_none("input_price", input_raw, html_snippet)
            _warn_if_none("output_price", output_raw, html_snippet)

            if model_name is None:
                continue

            print(
                f"[parse_openai] 尝试提取字段：model_name={model_name}, "
                f"input_price={input_raw}, output_price={output_raw}"
            )

            records.append(
                RawPriceRecord(
                    company=company,
                    model_name=model_name,
                    input_raw=input_raw,
                    output_raw=output_raw,
                    extra={"source": "openai-table"},
                ).to_dict()
            )

    print(f"[parse_openai] {company}: 找到价格容器元素：{container_count} 个")
    print(f"[parse_openai] {company}: extracted {len(records)} record(s)")
    for rec in records:
        print(f"[parse_openai] record dict = {rec}")
    return records


def _parse_generic_table(html: str, company: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Fallback parser for providers whose pricing is mainly in tables."""
    print(f"[parse_generic] 正在解析 {company}，页面长度：{len(html)} 字符")
    soup = BeautifulSoup(html, "html.parser")
    records: List[Dict[str, Any]] = []

    css = config.get("css_selectors", {}) or {}
    table_selector = css.get("table", "table")
    header_selector = css.get("header_row", "thead tr")
    body_row_selector = css.get("body_rows", "tbody tr")

    print(
        f"[parse_generic] {company}: table_selector='{table_selector}', "
        f"header_selector='{header_selector}', body_row_selector='{body_row_selector}'"
    )

    tables = soup.select(table_selector)
    print(f"[parse_generic] {company}: found {len(tables)} table(s)")

    container_count = 0

    for table in tables:
        header_row = table.select_one(header_selector)
        if not header_row:
            continue
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all("th")]
        if not headers:
            continue
        if not any("model" in h or "name" in h for h in headers):
            continue

        model_idx = None
        input_idx = None
        output_idx = None
        for idx, header in enumerate(headers):
            if model_idx is None and ("model" in header or "name" in header):
                model_idx = idx
            if input_idx is None and ("input" in header or "prompt" in header):
                input_idx = idx
            if output_idx is None and ("output" in header or "completion" in header):
                output_idx = idx

        rows = table.select(body_row_selector) or table.find_all("tr")
        container_count += len(rows)
        print(f"[parse_generic] {company}: rows in current table = {len(rows)}")

        for row in rows:
            tds = row.find_all("td")
            cells = [td.get_text(" ", strip=True) for td in tds]
            if not cells:
                continue

            model_name = _extract_model_name_from_cells(cells)
            input_raw = _extract_input_price_from_cells(cells, input_idx)
            output_raw = _extract_output_price_from_cells(cells, output_idx)

            html_snippet = row.prettify() if hasattr(row, "prettify") else str(row)
            _warn_if_none("model_name", model_name, html_snippet)
            _warn_if_none("input_price", input_raw, html_snippet)
            _warn_if_none("output_price", output_raw, html_snippet)

            if model_name is None:
                continue

            print(
                f"[parse_generic] 尝试提取字段：model_name={model_name}, "
                f"input_price={input_raw}, output_price={output_raw}"
            )

            records.append(
                RawPriceRecord(
                    company=company,
                    model_name=model_name,
                    input_raw=input_raw,
                    output_raw=output_raw,
                    extra={"source": "generic-table"},
                ).to_dict()
            )

    print(f"[parse_generic] {company}: 找到价格容器元素：{container_count} 个")
    print(f"[parse_generic] {company}: extracted {len(records)} record(s)")
    for rec in records:
        print(f"[parse_generic] record dict = {rec}")
    return records


PARSER_MAP = {
    "custom_openai": _parse_openai,
    "table": _parse_generic_table,
}


def parse_pricing_html(company: str, html: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Dispatch to company-specific parser based on config."""
    parser_key = config.get("parser", "table")
    parser = PARSER_MAP.get(parser_key, _parse_generic_table)
    return parser(html, company, config)


def crawl_all_providers(config_path: str = "data/providers.yaml", delay_seconds: float = 1.5) -> List[Dict[str, Any]]:
    """Crawl all providers defined in the YAML config and return raw records."""
    print(f"[crawl_all_providers] Loading config from {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        providers = yaml.safe_load(f) or []

    print(f"[crawl_all_providers] Loaded {len(providers)} provider(s)")
    all_records: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for idx, provider in enumerate(providers, start=1):
        # Support both old 'company' and new 'name' field for provider id.
        company = provider.get("company") or provider.get("name") or "Unknown"
        url = provider.get("url")
        if not url:
            errors.append({"company": company, "error": "missing_url"})
            print(f"[crawl_all_providers][WARN] Provider #{idx} ({company}) has no URL configured, skipping.")
            continue

        provider_delay = float(provider.get("delay", delay_seconds))

        try:
            print(f"[crawl_all_providers] [{idx}/{len(providers)}] Crawling {company} ({url})")
            html = fetch_page_html(provider)
            records = parse_pricing_html(company, html, provider)
            all_records.extend(records)
            print(f"[crawl_all_providers] {company}: parsed {len(records)} record(s)")
        except Exception as exc:  # noqa: BLE001
            errors.append({"company": company, "error": str(exc)})
            print(f"[crawl_all_providers][ERROR] Failed to crawl {company}: {exc}")
        finally:
            # Simple delay between providers to avoid hammering endpoints.
            print(f"[crawl_all_providers] Sleeping for {provider_delay} seconds before next provider")
            time.sleep(provider_delay)

    # Persist errors for debugging, but do not fail the run.
    if errors:
        print(f"[crawl_all_providers] Completed with {len(errors)} error(s), writing data/crawl_errors.json")
        with open("data/crawl_errors.json", "w", encoding="utf-8") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
    else:
        print("[crawl_all_providers] Completed without errors")

    success_count = len(all_records)
    failed_count = len(errors)
    print(f"[summary] 成功抓取 {success_count} 条，失败 {failed_count} 条")

    return all_records


def main() -> None:
    """Entry point for running the spider as a script."""
    print("[main] Starting crawl_all_providers")
    records = crawl_all_providers()
    print(f"[main] Collected {len(records)} total record(s), writing data/raw_prices.json")
    with open("data/raw_prices.json", "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print("[main] Finished writing data/raw_prices.json")


if __name__ == "__main__":
    main()

