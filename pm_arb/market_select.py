from __future__ import annotations

import re
from typing import Any

from .gamma import parse_clob_token_ids


def _market_text(market: dict[str, Any]) -> str:
    return " ".join(
        str(market.get(key, "")) for key in ("question", "title", "description", "slug")
    )


def _regex_haystack(market: dict[str, Any]) -> str:
    return f"{market.get('slug','')}\n{market.get('question','')}"


def _secondary_match(text: str) -> bool:
    lower = text.lower()
    has_15 = "15" in lower
    has_min = ("min" in lower) or ("minute" in lower)
    has_asset = any(asset in lower for asset in ("btc", "eth", "sol"))
    return has_15 and has_min and has_asset


def filter_markets(
    markets: list[dict[str, Any]],
    *,
    market_regex: str,
    secondary_filter_enable: bool,
    max_markets: int,
) -> list[dict[str, Any]]:
    pattern = re.compile(market_regex, re.MULTILINE)
    selected: list[dict[str, Any]] = []
    for market in markets:
        active = market.get("active")
        if active is not None and not active:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        text = _market_text(market)
        regex_haystack = _regex_haystack(market)
        reasons: list[str] = []
        if pattern.search(regex_haystack):
            reasons.append("regex")
        if secondary_filter_enable and _secondary_match(text):
            reasons.append("secondary")
        if reasons:
            market["_match_reasons"] = reasons
            selected.append(market)
        if len(selected) >= max_markets:
            break
    return selected


def select_active_binary_markets(
    markets: list[dict[str, Any]],
    *,
    max_markets: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for market in markets:
        active = market.get("active")
        if active is not None and not active:
            continue
        if market.get("enableOrderBook") is False:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        selected.append(market)
        if len(selected) >= max_markets:
            break
    return selected
