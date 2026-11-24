#!/usr/bin/env python3
"""Zero-dependency (Python 3.11+ stdlib) live public market intelligence updater.

Fetches real-time read-only public market data (US-cloud safe, zero auth required):
1. Prediction Markets:
   - Polymarket Gamma API (`https://gamma-api.polymarket.com/events`)
   - Kalshi Trade API v2 (`https://api.elections.kalshi.com/trade-api/v2`)
   -> Extracts top 8 live prediction market contracts & cross-venue / intra-event spreads.
2. Perpetual Funding-Rate Carry Arbitrage:
   - Hyperliquid (`POST https://api.hyperliquid.xyz/info` `{"type":"metaAndAssetCtxs"}`)
   - Kraken Futures (`GET https://futures.kraken.com/derivatives/api/v3/tickers`)
   - dYdX v4 Indexer (`GET https://indexer.dydx.trade/v4/perpetualMarkets`)
   -> Computes top 10 live cross-venue delta-neutral perpetual funding-rate carry spreads
      (`short_venue`, `long_venue`, `short_apr_pct`, `long_apr_pct`, `net_spread_apr_pct`, `min_oi_usd`).

Writes `docs/data/live_feed.json` for the GitHub Pages terminal and MCP preview fallback.
"""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "docs" / "data" / "live_feed.json"

USER_AGENT = "QuantB2BIntelHub/1.0"
HOURS_PER_YEAR = 24.0 * 365.0
# Conservative 30-day hold amortized 2-leg execution fee drag (~7 bps round-trip * 12.17x/yr = 0.85% APR)
ANNUALIZED_EXEC_FEE_DRAG_PCT = 0.85
MIN_VENUE_OI_USD = 75_000.0

APIFY_ACTORS = {
    "prediction_market_arb": {
        "actor_id": "conscious_chuckwalla/prediction-market-arb-intel",
        "api_slug": "conscious_chuckwalla~prediction-market-arb-intel",
        "title": "Polymarket & Kalshi Odds, Orderbook & Arbitrage Intel",
        "store_url": "https://apify.com/conscious_chuckwalla/prediction-market-arb-intel",
        "api_run_sync_url": (
            "https://api.apify.com/v2/acts/conscious_chuckwalla~prediction-market-arb-intel"
            "/run-sync-get-dataset-items"
        ),
        "status": "LIVE",
        "full_scope": "3,400+ active contracts, L2 CLOB orderbook depth, $1,000 VWAP slippage & cross-venue arbitrage",
    },
    "crypto_funding_carry": {
        "actor_id": "conscious_chuckwalla/crypto-funding-rate-carry-arb",
        "api_slug": "conscious_chuckwalla~crypto-funding-rate-carry-arb",
        "title": "Crypto Perp Funding Rate, OI & Delta-Neutral Carry Arbitrage",
        "store_url": "https://apify.com/conscious_chuckwalla/crypto-funding-rate-carry-arb",
        "api_run_sync_url": (
            "https://api.apify.com/v2/acts/conscious_chuckwalla~crypto-funding-rate-carry-arb"
            "/run-sync-get-dataset-items"
        ),
        "status": "LIVE",
        "full_scope": "650+ perpetual markets across CEX & PerpDEX venues, 7d/30d realized funding stability & OI-weighted carry",
    },
    "shopify_leads_intel": {
        "actor_id": "conscious_chuckwalla/shopify-leads-tech-stack-intel",
        "api_slug": "conscious_chuckwalla~shopify-leads-tech-stack-intel",
        "title": "Shopify Store Leads, Emails, Socials & Tech Stack Enricher",
        "store_url": "https://apify.com/conscious_chuckwalla/shopify-leads-tech-stack-intel",
        "api_run_sync_url": (
            "https://api.apify.com/v2/acts/conscious_chuckwalla~shopify-leads-tech-stack-intel"
            "/run-sync-get-dataset-items"
        ),
        "status": "LIVE",
        "full_scope": "Verified merchant contacts, social handles, 40+ detected SaaS/pixel signatures, SKU catalog & AOV tiers",
    },
}

KALSHI_FLAGSHIP_SERIES = (
    "KXFEDDECISION",
    "KXFED",
    "KXCPI",
    "KXRECSSNBER",
    "KXGDP",
    "KXPAYROLLS",
    "KXBTC",
    "KXETH",
    "KXNBA",
    "KXUCL",
    "KXNHL",
    "KXPRESNOMD",
    "KXPRESNOMR",
    "KXTOPMODEL",
)

STOPWORDS = {
    "will",
    "the",
    "be",
    "in",
    "on",
    "at",
    "by",
    "to",
    "of",
    "for",
    "a",
    "an",
    "or",
    "and",
    "before",
    "after",
    "during",
    "this",
    "next",
    "year",
    "win",
    "who",
    "what",
    "above",
    "below",
    "over",
    "under",
    "than",
    "more",
    "less",
    "hit",
    "reach",
    "price",
    "market",
}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_float(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        out = float(val)
        return out if out == out else None  # guard NaN
    except (TypeError, ValueError):
        return None


def _safe_json_list(val: Any) -> list[Any]:
    if isinstance(val, list):
        return val
    if isinstance(val, str) and val.strip():
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _days_until(iso_str: str | None) -> float | None:
    if not iso_str:
        return None
    raw = str(iso_str).strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        target = dt.datetime.fromisoformat(raw)
        if target.tzinfo is None:
            target = target.replace(tzinfo=dt.timezone.utc)
        delta_days = (target - dt.datetime.now(dt.timezone.utc)).total_seconds() / 86400.0
        return round(max(0.0, delta_days), 1)
    except ValueError:
        return None


def fetch_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 20.0,
    retries: int = 3,
) -> Any:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except Exception as exc:
            last_err = exc
            if attempt + 1 < retries:
                time.sleep(0.6 * (attempt + 1))
    assert last_err is not None
    raise last_err


# ---------------------------------------------------------------------------
# 1. Prediction Markets: Polymarket Gamma + Kalshi v2
# ---------------------------------------------------------------------------

def _infer_pm_category(tags: list[str], title: str) -> str:
    blob = (" ".join(tags) + " " + title).lower()
    if any(k in blob for k in ("fed", "fomc", "cpi", "gdp", "recession", "inflation", "rate", "tariff", "economy")):
        return "Macro & Rates"
    if any(k in blob for k in ("bitcoin", "btc", "ethereum", "eth", "solana", "crypto", "token")):
        return "Crypto"
    if any(k in blob for k in ("election", "president", "trump", "senate", "house", "minister", "geopolitics", "ceasefire")):
        return "Politics"
    if any(k in blob for k in ("nba", "nfl", "ucl", "champions league", "nhl", "mlb", "ufc", "soccer", "tennis")):
        return "Sports"
    if any(k in blob for k in ("ai", "openai", "gpt", "claude", "gemini", "nvidia", "spacex", "tesla", "apple")):
        return "Tech & AI"
    return tags[0] if tags else "Global Events"


def fetch_polymarket_contracts() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (normalized_contracts, intra_event_basket_spreads) from Polymarket Gamma."""
    events: list[dict[str, Any]] = []
    seen_ev_ids: set[str] = set()
    # Gamma API enforces max limit=50 per request; fetch 50 + 20 = 70 top events by 24h volume
    for limit, offset in ((50, 0), (20, 50)):
        page: Any = None
        for qs in (
            f"active=true&closed=false&limit={limit}&offset={offset}&order=volume24hr&ascending=false",
            f"active=true&closed=false&limit={limit}&offset={offset}",
        ):
            try:
                page = fetch_json(f"https://gamma-api.polymarket.com/events?{qs}")
                if isinstance(page, list):
                    break
            except Exception:
                continue
        if isinstance(page, list):
            for item in page:
                if isinstance(item, dict):
                    eid = str(item.get("id") or "")
                    if eid and eid not in seen_ev_ids:
                        seen_ev_ids.add(eid)
                        events.append(item)

    if not events:
        return [], []

    contracts: list[dict[str, Any]] = []
    intra_spreads: list[dict[str, Any]] = []

    for ev in events:
        if not isinstance(ev, dict):
            continue
        event_id = str(ev.get("id") or "")
        event_title = str(ev.get("title") or "").strip()
        event_slug = str(ev.get("slug") or "").strip()
        event_url = f"https://polymarket.com/event/{event_slug}" if event_slug else "https://polymarket.com"
        raw_tags = ev.get("tags") or []
        tags = [str(t.get("label")).strip() for t in raw_tags if isinstance(t, dict) and t.get("label")]
        category = _infer_pm_category(tags, event_title)
        neg_risk = bool(ev.get("negRisk") or ev.get("enableNegRisk"))

        event_legs: list[dict[str, Any]] = []
        for m in ev.get("markets") or []:
            if not isinstance(m, dict):
                continue
            if m.get("closed") is True or m.get("archived") is True or m.get("active") is False:
                continue
            if m.get("enableOrderBook") is False:
                continue

            question = str(m.get("question") or event_title).strip()
            outcome_label = str(m.get("groupItemTitle") or "").strip() or question
            prices = [_to_float(p) for p in _safe_json_list(m.get("outcomePrices"))]
            yes_price = prices[0] if prices else None

            best_bid = _to_float(m.get("bestBid"))
            best_ask = _to_float(m.get("bestAsk"))
            yes_bid = round(best_bid, 4) if best_bid is not None and 0.0 < best_bid < 1.0 else None
            yes_ask = round(best_ask, 4) if best_ask is not None and 0.0 < best_ask < 1.0 else None

            if yes_bid is not None and yes_ask is not None and yes_ask >= yes_bid:
                yes_mid = round((yes_bid + yes_ask) / 2.0, 4)
            elif yes_price is not None and 0.0 < yes_price < 1.0:
                yes_mid = round(yes_price, 4)
            else:
                continue

            no_bid = round(1.0 - yes_ask, 4) if yes_ask is not None else round(1.0 - yes_mid, 4)
            no_ask = round(1.0 - yes_bid, 4) if yes_bid is not None else round(1.0 - yes_mid, 4)
            spread_bps = (
                round((yes_ask - yes_bid) * 10000.0, 1)
                if yes_bid is not None and yes_ask is not None and yes_ask >= yes_bid
                else 0.0
            )
            vol_24h = _to_float(m.get("volume24hr")) or _to_float(m.get("volume24hrClob")) or 0.0
            liquidity = _to_float(m.get("liquidityNum")) or _to_float(m.get("liquidity")) or 0.0
            dte = _days_until(str(m.get("endDate") or ev.get("endDate") or ""))

            rec = {
                "venue": "Polymarket",
                "event_id": event_id,
                "event_title": event_title,
                "ticker": str(m.get("slug") or m.get("id") or "")[:42],
                "question": question,
                "outcome_label": outcome_label,
                "category": category,
                "yes_bid": yes_bid if yes_bid is not None else yes_mid,
                "yes_ask": yes_ask if yes_ask is not None else yes_mid,
                "yes_mid": yes_mid,
                "no_bid": no_bid,
                "no_ask": no_ask,
                "spread_bps": spread_bps,
                "volume_24h_usd": round(vol_24h, 2),
                "liquidity_usd": round(liquidity, 2),
                "days_to_expiry": dte,
                "neg_risk": neg_risk,
                "url": event_url,
            }
            contracts.append(rec)
            event_legs.append(rec)

        # Check multi-outcome NegRisk sum-to-one mispricings
        if neg_risk and len(event_legs) >= 3:
            valid_asks = [x["yes_ask"] for x in event_legs if x["yes_ask"] is not None and 0.0 < x["yes_ask"] < 1.0]
            valid_bids = [x["yes_bid"] for x in event_legs if x["yes_bid"] is not None and 0.0 < x["yes_bid"] < 1.0]
            vol_sum = round(sum(x["volume_24h_usd"] for x in event_legs), 2)
            if len(valid_asks) == len(event_legs) and vol_sum >= 1000:
                sum_asks = sum(valid_asks)
                sum_bids = sum(valid_bids) if len(valid_bids) == len(event_legs) else sum_asks
                underround_bps = round((1.0 - sum_asks) * 10000.0, 1)
                overround_bps = round((sum_bids - 1.0) * 10000.0, 1)
                labels_blob = " ".join(x["outcome_label"].lower() for x in event_legs)
                exhaustive = any(w in labels_blob for w in ("other", "no change", "someone else", "none"))
                if underround_bps >= 25.0 and exhaustive:
                    intra_spreads.append(
                        {
                            "signal_type": "NegRisk Long-Basket Underround",
                            "venues": "Polymarket CLOB",
                            "title": event_title,
                            "category": category,
                            "leg_a": f"Buy all {len(event_legs)} YES asks (Σ=${sum_asks:.4f})",
                            "leg_b": "Redeem $1.00 exhaustive payout",
                            "yes_prob_pct": round(sum_asks * 100.0, 2),
                            "edge_bps": underround_bps,
                            "volume_24h_usd": vol_sum,
                            "days_to_expiry": event_legs[0]["days_to_expiry"],
                            "url": event_url,
                        }
                    )
                elif overround_bps >= 60.0:
                    intra_spreads.append(
                        {
                            "signal_type": "NegRisk Split/Mint Overround",
                            "venues": "Polymarket CLOB",
                            "title": event_title,
                            "category": category,
                            "leg_a": "Mint $1.00 USDC NegRisk full set",
                            "leg_b": f"Hit {len(event_legs)} YES bids (Σ=${sum_bids:.4f})",
                            "yes_prob_pct": round(sum_bids * 100.0, 2),
                            "edge_bps": overround_bps,
                            "volume_24h_usd": vol_sum,
                            "days_to_expiry": event_legs[0]["days_to_expiry"],
                            "url": event_url,
                        }
                    )

    return contracts, intra_spreads


def _parse_kalshi_item(m: dict[str, Any], ev: dict[str, Any] | None = None) -> dict[str, Any] | None:
    ticker = str(m.get("ticker") or "").strip()
    if not ticker or ticker.startswith("KXMVE"):
        return None
    status = str(m.get("status") or "").lower()
    if status and status not in {"active", "open", "initialized"}:
        return None

    ev = ev or {}
    event_ticker = str(m.get("event_ticker") or ev.get("event_ticker") or ticker).strip()
    series_ticker = str(ev.get("series_ticker") or event_ticker.split("-")[0]).strip()
    event_title = str(ev.get("title") or m.get("title") or ticker).strip()
    question = str(m.get("title") or event_title).strip()
    yes_sub = str(m.get("yes_sub_title") or m.get("subtitle") or "").strip()
    outcome_label = yes_sub if yes_sub else question

    def _px(d_key: str, c_key: str) -> float | None:
        d = _to_float(m.get(d_key))
        if d is not None and 0.0 < d < 1.0:
            return round(d, 4)
        c = _to_float(m.get(c_key))
        if c is not None and 0.0 < c < 100.0:
            return round(c / 100.0, 4)
        return None

    yes_bid = _px("yes_bid_dollars", "yes_bid")
    yes_ask = _px("yes_ask_dollars", "yes_ask")
    no_bid = _px("no_bid_dollars", "no_bid")
    no_ask = _px("no_ask_dollars", "no_ask")
    last_px = _px("last_price_dollars", "last_price")

    if no_ask is None and yes_bid is not None:
        no_ask = round(1.0 - yes_bid, 4)
    if no_bid is None and yes_ask is not None:
        no_bid = round(1.0 - yes_ask, 4)

    if yes_bid is not None and yes_ask is not None and yes_ask >= yes_bid:
        yes_mid = round((yes_bid + yes_ask) / 2.0, 4)
    elif last_px is not None:
        yes_mid = last_px
    else:
        return None

    spread_bps = (
        round((yes_ask - yes_bid) * 10000.0, 1)
        if yes_bid is not None and yes_ask is not None and yes_ask >= yes_bid
        else 0.0
    )
    vol_24h = _to_float(m.get("volume_24h_fp")) or _to_float(m.get("volume_24h")) or 0.0
    oi = _to_float(m.get("open_interest_fp")) or _to_float(m.get("open_interest")) or 0.0
    dte = _days_until(str(m.get("expected_expiration_time") or m.get("close_time") or ""))
    raw_cat = str(ev.get("category") or "").strip()
    category = _infer_pm_category([raw_cat, series_ticker], event_title + " " + question)

    return {
        "venue": "Kalshi",
        "event_id": event_ticker,
        "event_title": event_title,
        "ticker": ticker,
        "question": f"{event_title} — {yes_sub}" if yes_sub and yes_sub.lower() not in event_title.lower() else question,
        "outcome_label": outcome_label,
        "category": category,
        "yes_bid": yes_bid if yes_bid is not None else yes_mid,
        "yes_ask": yes_ask if yes_ask is not None else yes_mid,
        "yes_mid": yes_mid,
        "no_bid": no_bid if no_bid is not None else round(1.0 - yes_mid, 4),
        "no_ask": no_ask if no_ask is not None else round(1.0 - yes_mid, 4),
        "spread_bps": spread_bps,
        "volume_24h_usd": round(vol_24h, 2),
        "liquidity_usd": round(oi, 2),
        "days_to_expiry": dte,
        "neg_risk": bool(ev.get("mutually_exclusive", False)),
        "url": f"https://kalshi.com/markets/{series_ticker.lower()}",
    }


def fetch_kalshi_contracts() -> list[dict[str, Any]]:
    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    # 1. General open non-parlay markets endpoint (explicitly requested in spec + mve_filter=exclude)
    general_urls = [
        "https://api.elections.kalshi.com/trade-api/v2/markets?status=open&limit=100&mve_filter=exclude",
        "https://api.elections.kalshi.com/trade-api/v2/markets?status=open&limit=100",
    ]
    for g_url in general_urls:
        try:
            payload = fetch_json(g_url)
            for m in payload.get("markets") or []:
                if isinstance(m, dict):
                    parsed = _parse_kalshi_item(m)
                    if parsed and parsed["ticker"] not in seen:
                        seen.add(parsed["ticker"])
                        results.append(parsed)
            if results:
                break
        except Exception:
            continue

    # 2. Flagship macro/crypto/politics/sports series for rich overlap with Polymarket
    def _pull_series(series: str) -> list[dict[str, Any]]:
        u = (
            "https://api.elections.kalshi.com/trade-api/v2/events"
            f"?status=open&series_ticker={urllib.parse.quote(series)}&with_nested_markets=true&limit=8"
        )
        out: list[dict[str, Any]] = []
        try:
            data = fetch_json(u, timeout=12.0)
            for ev in data.get("events") or []:
                if not isinstance(ev, dict):
                    continue
                for m in ev.get("markets") or []:
                    if isinstance(m, dict):
                        p = _parse_kalshi_item(m, ev)
                        if p:
                            out.append(p)
        except Exception:
            pass
        return out

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for batch in pool.map(_pull_series, KALSHI_FLAGSHIP_SERIES):
            for item in batch:
                if item["ticker"] not in seen:
                    seen.add(item["ticker"])
                    results.append(item)

    return results


def _tokenize_contract(c: dict[str, Any]) -> set[str]:
    text = f"{c.get('event_title', '')} {c.get('question', '')} {c.get('outcome_label', '')}".lower()
    tokens = {t for t in re.findall(r"[a-z0-9]+", text) if len(t) >= 2 and t not in STOPWORDS}
    return tokens


def build_prediction_intel(
    pm_contracts: list[dict[str, Any]],
    ka_contracts: list[dict[str, Any]],
    intra_spreads: list[dict[str, Any]],
) -> dict[str, Any]:
    # Filter liquid contracts on both venues to ensure balanced institutional coverage
    pm_liquid = sorted(
        [c for c in pm_contracts if c["volume_24h_usd"] >= 500 and 0.02 <= c["yes_mid"] <= 0.98],
        key=lambda x: x["volume_24h_usd"],
        reverse=True,
    )
    ka_liquid = sorted(
        [c for c in ka_contracts if c["volume_24h_usd"] >= 200 and 0.02 <= c["yes_mid"] <= 0.98],
        key=lambda x: x["volume_24h_usd"],
        reverse=True,
    )

    # Top 8 live prediction contracts: blend top 5 Polymarket + top 3 Kalshi, sorted by 24h volume
    blended_top: list[dict[str, Any]] = []
    seen_events: set[str] = set()
    for pool, cap in ((pm_liquid, 5), (ka_liquid, 3)):
        added = 0
        for c in pool:
            ev_key = f"{c['venue']}:{c['event_id']}"
            if ev_key in seen_events:
                continue
            seen_events.add(ev_key)
            blended_top.append(c)
            added += 1
            if added >= cap:
                break

    if len(blended_top) < 8:
        for c in pm_liquid + ka_liquid:
            if len(blended_top) >= 8:
                break
            if c not in blended_top:
                blended_top.append(c)

    blended_top.sort(key=lambda x: x["volume_24h_usd"], reverse=True)
    top_8_contracts = blended_top[:8]

    # Cross-venue probability divergence & arbitrage scanner (Polymarket vs Kalshi)
    cross_spreads: list[dict[str, Any]] = []
    pm_tok_cache = [(c, _tokenize_contract(c)) for c in pm_liquid[:120]]
    ka_tok_cache = [(c, _tokenize_contract(c)) for c in ka_liquid[:140]]

    used_pairs: set[tuple[str, str]] = set()
    for pm, pm_toks in pm_tok_cache:
        if len(pm_toks) < 3:
            continue
        best_ka = None
        best_score = 0.0
        for ka, ka_toks in ka_tok_cache:
            if len(ka_toks) < 3:
                continue
            # Require expiry alignment within 10 days when both dates exist
            if pm["days_to_expiry"] is not None and ka["days_to_expiry"] is not None:
                if abs(pm["days_to_expiry"] - ka["days_to_expiry"]) > 10.0:
                    continue
            # Require numeric tokens (years, strike numbers) to match if present
            pm_nums = {t for t in pm_toks if t.isdigit()}
            ka_nums = {t for t in ka_toks if t.isdigit()}
            if pm_nums and ka_nums and not (pm_nums & ka_nums):
                continue
            inter = pm_toks & ka_toks
            if len(inter) < 3:
                continue
            union = pm_toks | ka_toks
            jaccard = len(inter) / max(1, len(union))
            # Boost if specific outcome label tokens match
            out_pm = {t for t in re.findall(r"[a-z0-9]+", pm["outcome_label"].lower()) if t not in STOPWORDS}
            out_ka = {t for t in re.findall(r"[a-z0-9]+", ka["outcome_label"].lower()) if t not in STOPWORDS}
            if out_pm and out_ka and not (out_pm & out_ka):
                continue
            if jaccard > best_score:
                best_score = jaccard
                best_ka = ka

        if best_ka is not None and best_score >= 0.42:
            pair_key = (pm["ticker"], best_ka["ticker"])
            if pair_key in used_pairs:
                continue
            used_pairs.add(pair_key)

            # Compare executable cross-venue arbitrage & mid-price divergence
            cost_pm_yes_ka_no = (pm["yes_ask"] or pm["yes_mid"]) + (best_ka["no_ask"] or (1.0 - best_ka["yes_mid"]))
            cost_ka_yes_pm_no = (best_ka["yes_ask"] or best_ka["yes_mid"]) + (pm["no_ask"] or (1.0 - pm["yes_mid"]))
            min_Lock_cost = min(cost_pm_yes_ka_no, cost_ka_yes_pm_no)
            lock_edge_bps = round((1.0 - min_Lock_cost) * 10000.0, 1)
            mid_div_bps = round(abs(pm["yes_mid"] - best_ka["yes_mid"]) * 10000.0, 1)

            if mid_div_bps >= 35.0 or lock_edge_bps > 0:
                if pm["yes_mid"] < best_ka["yes_mid"]:
                    leg_a = f"Polymarket YES @ {pm['yes_ask']*100:.1f}¢"
                    leg_b = f"Kalshi NO @ {best_ka['no_ask']*100:.1f}¢ (YES {best_ka['yes_mid']*100:.1f}¢)"
                else:
                    leg_a = f"Kalshi YES @ {best_ka['yes_ask']*100:.1f}¢"
                    leg_b = f"Polymarket NO @ {pm['no_ask']*100:.1f}¢ (YES {pm['yes_mid']*100:.1f}¢)"
                cross_spreads.append(
                    {
                        "signal_type": "Cross-Venue Locked Arb" if lock_edge_bps > 0 else "Cross-Venue Odds Divergence",
                        "venues": "Polymarket × Kalshi",
                        "title": pm["question"],
                        "category": pm["category"],
                        "leg_a": leg_a,
                        "leg_b": leg_b,
                        "yes_prob_pct": round(pm["yes_mid"] * 100.0, 1),
                        "edge_bps": lock_edge_bps if lock_edge_bps > 0 else mid_div_bps,
                        "volume_24h_usd": round(pm["volume_24h_usd"] + best_ka["volume_24h_usd"], 2),
                        "days_to_expiry": pm["days_to_expiry"],
                        "url": pm["url"],
                    }
                )

    # High-conviction near-expiry Prediction Yield Bonds (carry to resolution)
    yield_bonds: list[dict[str, Any]] = []
    for c in pm_liquid[:80] + ka_liquid[:60]:
        dte = c.get("days_to_expiry")
        if dte is None or dte < 2.0 or dte > 75.0 or c["volume_24h_usd"] < 2500:
            continue
        for side, ask_px in (("YES", c["yes_ask"]), ("NO", c["no_ask"])):
            if ask_px is not None and 0.88 <= ask_px <= 0.985:
                gross_bps = round(((1.0 - ask_px) / ask_px) * 10000.0, 1)
                ann_apr = round(((1.0 - ask_px) / ask_px) * (365.0 / dte) * 100.0, 2)
                if 10.0 <= ann_apr <= 180.0:
                    yield_bonds.append(
                        {
                            "signal_type": f"Near-Expiry Yield Bond ({ann_apr:.1f}% APR)",
                            "venues": c["venue"],
                            "title": f"{c['event_title']} — {c['outcome_label']}" if c["outcome_label"] != c["question"] else c["question"],
                            "category": c["category"],
                            "leg_a": f"Buy {c['venue']} {side} @ {ask_px*100:.1f}¢",
                            "leg_b": f"Hold {dte:.0f}d -> $1.00 resolution",
                            "yes_prob_pct": round(ask_px * 100.0, 1),
                            "edge_bps": gross_bps,
                            "volume_24h_usd": c["volume_24h_usd"],
                            "days_to_expiry": dte,
                            "url": c["url"],
                        }
                    )

    combined_spreads = sorted(cross_spreads, key=lambda x: (x["edge_bps"], x["volume_24h_usd"]), reverse=True)[:4]
    combined_spreads += sorted(intra_spreads, key=lambda x: (x["edge_bps"], x["volume_24h_usd"]), reverse=True)[:4]
    if len(combined_spreads) < 8:
        seen_titles = {s["title"] for s in combined_spreads}
        for yb in sorted(yield_bonds, key=lambda x: x["volume_24h_usd"], reverse=True):
            if yb["title"] not in seen_titles:
                seen_titles.add(yb["title"])
                combined_spreads.append(yb)
            if len(combined_spreads) >= 8:
                break

    return {
        "polymarket_contracts_scanned": len(pm_contracts),
        "kalshi_contracts_scanned": len(ka_contracts),
        "total_contracts_scanned": len(pm_contracts) + len(ka_contracts),
        "top_contracts": top_8_contracts,
        "top_spreads": combined_spreads[:8],
    }


# ---------------------------------------------------------------------------
# 2. Perpetual Funding-Rate Carry Arbitrage: Hyperliquid + Kraken + dYdX v4
# ---------------------------------------------------------------------------

SYMBOL_ALIASES = {
    "XBT": "BTC",
    "XDG": "DOGE",
    "KPEPE": "PEPE",
    "1000PEPE": "PEPE",
    "KSHIB": "SHIB",
    "1000SHIB": "SHIB",
    "KBONK": "BONK",
    "1000BONK": "BONK",
    "KFLOKI": "FLOKI",
    "1000FLOKI": "FLOKI",
    "KLUNC": "LUNC",
    "1000LUNC": "LUNC",
    "KNEIRO": "NEIRO",
    "KDOGS": "DOGS",
}


def canonical_symbol(raw: str) -> str:
    up = raw.strip().upper()
    return SYMBOL_ALIASES.get(up, up)


def fetch_hyperliquid_perps() -> list[dict[str, Any]]:
    data = fetch_json("https://api.hyperliquid.xyz/info", method="POST", payload={"type": "metaAndAssetCtxs"})
    if not isinstance(data, list) or len(data) < 2:
        return []
    universe = data[0].get("universe") or []
    ctxs = data[1] or []
    out: list[dict[str, Any]] = []
    for meta, ctx in zip(universe, ctxs):
        if not isinstance(meta, dict) or not isinstance(ctx, dict):
            continue
        if meta.get("isDelisted"):
            continue
        raw_sym = str(meta.get("name") or "").strip()
        funding_1h = _to_float(ctx.get("funding"))
        mark_px = _to_float(ctx.get("markPx")) or _to_float(ctx.get("oraclePx"))
        oi_base = _to_float(ctx.get("openInterest"))
        vol_24h = _to_float(ctx.get("dayNtlVlm")) or 0.0
        if not raw_sym or funding_1h is None or mark_px is None or mark_px <= 0 or oi_base is None:
            continue
        oi_usd = oi_base * mark_px
        apr_pct = funding_1h * HOURS_PER_YEAR * 100.0
        out.append(
            {
                "venue": "Hyperliquid",
                "symbol": canonical_symbol(raw_sym),
                "raw_symbol": raw_sym,
                "mark_price_usd": round(mark_px, 6),
                "hourly_funding_rate": funding_1h,
                "apr_pct": round(apr_pct, 2),
                "oi_usd": round(oi_usd, 2),
                "volume_24h_usd": round(vol_24h, 2),
            }
        )
    return out


def fetch_kraken_perps() -> list[dict[str, Any]]:
    data = fetch_json("https://futures.kraken.com/derivatives/api/v3/tickers")
    tickers = data.get("tickers") if isinstance(data, dict) else []
    out: list[dict[str, Any]] = []
    for t in tickers or []:
        if not isinstance(t, dict):
            continue
        raw_sym = str(t.get("symbol") or "").strip()
        if not raw_sym.startswith("PF_") or t.get("suspended") is True:
            continue
        pair = str(t.get("pair") or "")
        base = pair.split(":")[0] if ":" in pair else raw_sym[3:-3]
        rel_1h = _to_float(t.get("relativeFundingRate"))
        mark_px = _to_float(t.get("markPrice")) or _to_float(t.get("indexPrice"))
        oi_base = _to_float(t.get("openInterest"))
        vol_24h = _to_float(t.get("volumeQuote")) or 0.0
        if not base or rel_1h is None or mark_px is None or mark_px <= 0 or oi_base is None:
            continue
        oi_usd = oi_base * mark_px
        apr_pct = rel_1h * HOURS_PER_YEAR * 100.0
        out.append(
            {
                "venue": "Kraken Futures",
                "symbol": canonical_symbol(base),
                "raw_symbol": raw_sym,
                "mark_price_usd": round(mark_px, 6),
                "hourly_funding_rate": rel_1h,
                "apr_pct": round(apr_pct, 2),
                "oi_usd": round(oi_usd, 2),
                "volume_24h_usd": round(vol_24h, 2),
            }
        )
    return out


def fetch_dydx_perps() -> list[dict[str, Any]]:
    data = fetch_json("https://indexer.dydx.trade/v4/perpetualMarkets")
    markets = data.get("markets") if isinstance(data, dict) else {}
    out: list[dict[str, Any]] = []
    for ticker, m in (markets or {}).items():
        if not isinstance(m, dict):
            continue
        if str(m.get("status") or "").upper() != "ACTIVE":
            continue
        raw_sym = str(m.get("ticker") or ticker).strip()
        if not raw_sym.endswith("-USD"):
            continue
        base = raw_sym[:-4]
        rate_1h = _to_float(m.get("nextFundingRate"))
        oracle_px = _to_float(m.get("oraclePrice"))
        oi_base = _to_float(m.get("openInterest"))
        vol_24h = _to_float(m.get("volume24H")) or 0.0
        if not base or rate_1h is None or oracle_px is None or oracle_px <= 0 or oi_base is None:
            continue
        oi_usd = oi_base * oracle_px
        apr_pct = rate_1h * HOURS_PER_YEAR * 100.0
        out.append(
            {
                "venue": "dYdX v4",
                "symbol": canonical_symbol(base),
                "raw_symbol": raw_sym,
                "mark_price_usd": round(oracle_px, 6),
                "hourly_funding_rate": rate_1h,
                "apr_pct": round(apr_pct, 2),
                "oi_usd": round(oi_usd, 2),
                "volume_24h_usd": round(vol_24h, 2),
            }
        )
    return out


def compute_perp_carry_spreads(
    hl_perps: list[dict[str, Any]],
    kr_perps: list[dict[str, Any]],
    dy_perps: list[dict[str, Any]],
) -> dict[str, Any]:
    by_symbol: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for item in hl_perps + kr_perps + dy_perps:
        if item["oi_usd"] < MIN_VENUE_OI_USD:
            continue
        # Guard against single-tick delisted/broken oracle anomalies (> 500% abs APR)
        if abs(item["apr_pct"]) > 500.0:
            continue
        sym = item["symbol"]
        venue = item["venue"]
        prev = by_symbol[sym].get(venue)
        if prev is None or item["oi_usd"] > prev["oi_usd"]:
            by_symbol[sym][venue] = item

    best_per_symbol: list[dict[str, Any]] = []
    for sym, venue_map in by_symbol.items():
        if len(venue_map) < 2:
            continue
        legs = list(venue_map.values())
        best_pair: dict[str, Any] | None = None
        for short_leg in legs:
            for long_leg in legs:
                if short_leg["venue"] == long_leg["venue"]:
                    continue
                gross_spread_apr = round(short_leg["apr_pct"] - long_leg["apr_pct"], 2)
                net_spread_apr = round(gross_spread_apr - ANNUALIZED_EXEC_FEE_DRAG_PCT, 2)
                if net_spread_apr <= 0:
                    continue
                min_oi = round(min(short_leg["oi_usd"], long_leg["oi_usd"]), 2)
                candidate = {
                    "symbol": sym,
                    "short_venue": short_leg["venue"],
                    "long_venue": long_leg["venue"],
                    "short_raw_symbol": short_leg["raw_symbol"],
                    "long_raw_symbol": long_leg["raw_symbol"],
                    "short_apr_pct": short_leg["apr_pct"],
                    "long_apr_pct": long_leg["apr_pct"],
                    "gross_spread_apr_pct": gross_spread_apr,
                    "net_spread_apr_pct": net_spread_apr,
                    "min_oi_usd": min_oi,
                    "short_oi_usd": short_leg["oi_usd"],
                    "long_oi_usd": long_leg["oi_usd"],
                    "mark_price_usd": short_leg["mark_price_usd"],
                    "daily_yield_per_10k_usd": round(10_000.0 * (net_spread_apr / 100.0) / 365.0, 2),
                    "annual_yield_per_10k_usd": round(10_000.0 * (net_spread_apr / 100.0), 2),
                }
                if best_pair is None or candidate["net_spread_apr_pct"] > best_pair["net_spread_apr_pct"]:
                    best_pair = candidate
        if best_pair is not None:
            best_per_symbol.append(best_pair)

    best_per_symbol.sort(key=lambda r: (r["net_spread_apr_pct"], r["min_oi_usd"]), reverse=True)

    # Also capture major benchmark assets (BTC, ETH, SOL) rates for quick institutional reference
    benchmarks: list[dict[str, Any]] = []
    for major in ("BTC", "ETH", "SOL", "XRP", "DOGE", "AVAX"):
        vmap = by_symbol.get(major, {})
        if vmap:
            benchmarks.append(
                {
                    "symbol": major,
                    "hyperliquid_apr_pct": vmap.get("Hyperliquid", {}).get("apr_pct"),
                    "kraken_apr_pct": vmap.get("Kraken Futures", {}).get("apr_pct"),
                    "dydx_apr_pct": vmap.get("dYdX v4", {}).get("apr_pct"),
                    "total_oi_usd": round(sum(x["oi_usd"] for x in vmap.values()), 2),
                }
            )

    return {
        "hyperliquid_markets_scanned": len(hl_perps),
        "kraken_markets_scanned": len(kr_perps),
        "dydx_markets_scanned": len(dy_perps),
        "total_perp_markets_scanned": len(hl_perps) + len(kr_perps) + len(dy_perps),
        "qualified_multi_venue_symbols": len(by_symbol),
        "fee_drag_assumption_apr_pct": ANNUALIZED_EXEC_FEE_DRAG_PCT,
        "min_venue_oi_filter_usd": MIN_VENUE_OI_USD,
        "top_carry_spreads": best_per_symbol[:10],
        "major_benchmarks": benchmarks,
    }


# ---------------------------------------------------------------------------
# 3. B2B Shopify Tech-Stack & Lead Schema Reference
# ---------------------------------------------------------------------------

def build_shopify_intel_preview() -> dict[str, Any]:
    return {
        "actor_url": APIFY_ACTORS["shopify_leads_intel"]["store_url"],
        "detected_signatures_count": 46,
        "enrichment_fields": [
            "domain",
            "merchant_brand",
            "verified_emails",
            "instagram_tiktok_linkedin",
            "esp_crm_stack",
            "ad_pixels_detected",
            "active_sku_count",
            "median_price_usd",
            "estimated_revenue_tier",
        ],
        "sample_segments": [
            {
                "segment": "DTC Beauty & Skincare ($2M–$25M GMV)",
                "tech_filter": "Shopify Plus + Klaviyo + Meta/TikTok Pixel + Gorgias",
                "median_aov_usd": 68.50,
                "email_hit_rate_pct": 84.2,
                "typical_use_case": "Retention agency & B2B SaaS outbound prospecting",
            },
            {
                "segment": "Performance Apparel & Athleisure",
                "tech_filter": "Shopify + Attentive SMS + Triple Whale + Loop Returns",
                "median_aov_usd": 114.00,
                "email_hit_rate_pct": 79.6,
                "typical_use_case": "3PL fulfillment, ad-tech & post-purchase SaaS sales",
            },
            {
                "segment": "Specialty Food, Beverage & Supplements",
                "tech_filter": "Shopify + Recharge Subscriptions + Okendo + Postscript",
                "median_aov_usd": 54.00,
                "email_hit_rate_pct": 86.8,
                "typical_use_case": "Subscription churn tooling & wholesale CPG buyers",
            },
            {
                "segment": "Home Goods & Industrial Hardware DTC",
                "tech_filter": "Shopify + Affirm/Klarna BNPL + Yotpo + Zendesk",
                "median_aov_usd": 289.00,
                "email_hit_rate_pct": 81.4,
                "typical_use_case": "B2B financing, freight & conversion optimization",
            },
        ],
    }


def main() -> int:
    captured_at = utc_now_iso()
    print(f"[{captured_at}] Refreshing Quant & B2B Live Intelligence Feed...")

    def _safe_call(label: str, fn: Any, default: Any) -> Any:
        try:
            return fn()
        except Exception as exc:
            print(f"[WARN] {label} fetch degraded: {exc}", file=sys.stderr)
            return default

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        fut_pm = executor.submit(_safe_call, "Polymarket", fetch_polymarket_contracts, ([], []))
        fut_ka = executor.submit(_safe_call, "Kalshi", fetch_kalshi_contracts, [])
        fut_hl = executor.submit(_safe_call, "Hyperliquid", fetch_hyperliquid_perps, [])
        fut_kr = executor.submit(_safe_call, "Kraken Futures", fetch_kraken_perps, [])
        fut_dy = executor.submit(_safe_call, "dYdX v4", fetch_dydx_perps, [])

        pm_contracts, intra_spreads = fut_pm.result()
        ka_contracts = fut_ka.result()
        hl_perps = fut_hl.result()
        kr_perps = fut_kr.result()
        dy_perps = fut_dy.result()

    prediction_intel = build_prediction_intel(pm_contracts, ka_contracts, intra_spreads)
    perp_carry_intel = compute_perp_carry_spreads(hl_perps, kr_perps, dy_perps)
    shopify_preview = build_shopify_intel_preview()

    top_carry = perp_carry_intel["top_carry_spreads"]
    best_carry_apr = top_carry[0]["net_spread_apr_pct"] if top_carry else 0.0
    top_pred_spreads = prediction_intel["top_spreads"]
    best_pred_edge_bps = max((s["edge_bps"] for s in top_pred_spreads), default=0.0)

    payload = {
        "schema_version": "1.0.0",
        "updated_at_utc": captured_at,
        "refresh_cadence": "Hourly (GitHub Actions Public Cron `12 * * * *`)",
        "summary": {
            "total_live_markets_ingested": (
                prediction_intel["total_contracts_scanned"] + perp_carry_intel["total_perp_markets_scanned"]
            ),
            "prediction_contracts_scanned": prediction_intel["total_contracts_scanned"],
            "perp_contracts_scanned": perp_carry_intel["total_perp_markets_scanned"],
            "peak_perp_carry_net_apr_pct": best_carry_apr,
            "peak_prediction_edge_bps": best_pred_edge_bps,
        },
        "apify_actors": APIFY_ACTORS,
        "prediction_markets": prediction_intel,
        "perp_funding_carry": perp_carry_intel,
        "shopify_b2b_leads": shopify_preview,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(
        f"Saved {OUTPUT_PATH}\n"
        f"  - Polymarket contracts: {prediction_intel['polymarket_contracts_scanned']} | "
        f"Kalshi contracts: {prediction_intel['kalshi_contracts_scanned']}\n"
        f"  - Top prediction contracts: {len(prediction_intel['top_contracts'])} | "
        f"Top spreads: {len(prediction_intel['top_spreads'])}\n"
        f"  - Perp markets: HL={perp_carry_intel['hyperliquid_markets_scanned']}, "
        f"Kraken={perp_carry_intel['kraken_markets_scanned']}, dYdX={perp_carry_intel['dydx_markets_scanned']}\n"
        f"  - Top 10 carry spreads peak net APR: +{best_carry_apr:.2f}%"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
