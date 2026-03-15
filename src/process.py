from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def normalize_price_string(raw: Optional[str]) -> Optional[str]:
    """Normalize a raw price string into a consistent human-readable format.

    This function is intentionally conservative: it mostly trims whitespace
    and normalizes internal spacing, without attempting aggressive parsing
    that might silently corrupt values when provider formats change.
    """
    if raw is None:
        return None

    # Collapse multiple spaces, strip leading/trailing whitespace.
    text = " ".join(raw.split())
    # Normalize common currency spacing like '$ 0.002' -> '$0.002'
    text = text.replace("$ ", "$")
    return text


def _today_utc_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def transform_records(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform raw spider records into the final JSON schema."""
    today = _today_utc_date_str()
    normalized: List[Dict[str, Any]] = []

    for rec in raw_records:
        company = str(rec.get("company", "")).strip()
        model_name = str(rec.get("model_name", "")).strip()
        if not company or not model_name:
            # Skip obviously malformed records.
            continue

        input_price_raw = rec.get("input_raw")
        output_price_raw = rec.get("output_raw")

        input_price = normalize_price_string(input_price_raw)
        output_price = normalize_price_string(output_price_raw)

        # If only one side is present, use it for both directions as a fallback.
        if input_price is None and output_price is not None:
            input_price = output_price
        if output_price is None and input_price is not None:
            output_price = input_price

        normalized.append(
            {
                "date": today,
                "company": company,
                "model_name": model_name,
                "input_price": input_price,
                "output_price": output_price,
            }
        )

    return normalized


def save_latest(prices: List[Dict[str, Any]], path: str = "data/latest_prices.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(prices, f, ensure_ascii=False, indent=2)


def main() -> None:
    with open("data/raw_prices.json", "r", encoding="utf-8") as f:
        raw_records = json.load(f)

    prices = transform_records(raw_records)
    save_latest(prices)


if __name__ == "__main__":
    main()

