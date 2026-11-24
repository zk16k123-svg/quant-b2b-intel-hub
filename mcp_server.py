#!/usr/bin/env python3
"""Quant & B2B Intelligence MCP Server (`quant-b2b-intel-mcp`).

Connects Claude Desktop, Cursor, Windsurf, Cline, and custom AI Agents directly to:
1. `conscious_chuckwalla/prediction-market-arb-intel` (Polymarket & Kalshi L2 Odds & Arbitrage)
2. `conscious_chuckwalla/crypto-funding-rate-carry-arb` (Cross-Venue Perp Funding Carry Yields)
3. `conscious_chuckwalla/shopify-leads-tech-stack-intel` (Shopify Store Leads, Emails & Tech Stack)

Execution Modes:
- Full Cloud Execution (`APIFY_TOKEN` set): Invokes Apify `run-sync-get-dataset-items` in real time
  for deep multi-thousand-market scans, L2 orderbook slippage, and live B2B lead enrichment.
- Zero-Auth Public Preview (`APIFY_TOKEN` unset): Falls back seamlessly to the hourly refreshed
  public `live_feed.json` snapshot and provides one-step instructions to unlock full Apify runs.

Supports both `FastMCP` (`mcp` package) and zero-dependency Python 3.10+ stdio JSON-RPC 2.0!
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

SERVER_NAME = "quant-b2b-intel-mcp"
SERVER_VERSION = "1.0.0"

PUBLIC_FEED_URL = "https://zk16k123-svg.github.io/quant-b2b-intel-hub/data/live_feed.json"
LOCAL_FEED_PATH = Path(__file__).resolve().parent / "docs" / "data" / "live_feed.json"

ACTORS = {
    "prediction": {
        "slug": "conscious_chuckwalla~prediction-market-arb-intel",
        "store_url": "https://apify.com/conscious_chuckwalla/prediction-market-arb-intel",
        "title": "Polymarket & Kalshi Odds, Orderbook & Arbitrage Intel",
    },
    "funding": {
        "slug": "conscious_chuckwalla~crypto-funding-rate-carry-arb",
        "store_url": "https://apify.com/conscious_chuckwalla/crypto-funding-rate-carry-arb",
        "title": "Crypto Perp Funding Rate, OI & Delta-Neutral Carry Arbitrage",
    },
    "shopify": {
        "slug": "conscious_chuckwalla~shopify-leads-tech-stack-intel",
        "store_url": "https://apify.com/conscious_chuckwalla/shopify-leads-tech-stack-intel",
        "title": "Shopify Store Leads, Emails, Socials & Tech Stack Enricher",
    },
}


def _get_apify_token() -> str:
    return (os.environ.get("APIFY_TOKEN") or os.environ.get("APIFY_API_TOKEN") or "").strip()


def _run_apify_actor_sync(actor_slug: str, actor_input: dict[str, Any], token: str, timeout: float = 120.0) -> Any:
    qs = urllib.parse.urlencode({"token": token, "clean": "true"})
    url = f"https://api.apify.com/v2/acts/{actor_slug}/run-sync-get-dataset-items?{qs}"
    body = json.dumps(actor_input).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": f"{SERVER_NAME}/{SERVER_VERSION}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _load_public_preview_feed() -> dict[str, Any]:
    try:
        req = urllib.request.Request(
            PUBLIC_FEED_URL,
            headers={"Accept": "application/json", "User-Agent": f"{SERVER_NAME}/{SERVER_VERSION}"},
        )
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass

    if LOCAL_FEED_PATH.exists():
        return json.loads(LOCAL_FEED_PATH.read_text(encoding="utf-8"))
    return {"error": "Live preview feed temporarily unavailable."}


def _upgrade_notice(actor_key: str) -> dict[str, str]:
    meta = ACTORS[actor_key]
    return {
        "execution_tier": "FREE_HOURLY_PREVIEW_SNAPSHOT",
        "unlock_full_realtime_depth": (
            f"Set APIFY_TOKEN in your MCP environment to run `{meta['title']}` live on Apify Cloud "
            f"with full market coverage, L2 orderbook slippage, and custom filters."
        ),
        "actor_store_url": meta["store_url"],
        "get_apify_token_url": "https://console.apify.com/settings/integrations",
    }


# ---------------------------------------------------------------------------
# Tool Implementations
# ---------------------------------------------------------------------------

def tool_scan_prediction_market_arbitrage(
    mode: str = "all",
    search_query: str = "",
    max_results: int = 25,
    include_orderbook_depth: bool = True,
) -> dict[str, Any]:
    """Scan Polymarket & Kalshi odds, L2 orderbook depth, cross-venue arbitrage & yield bonds."""
    token = _get_apify_token()
    if token:
        actor_input = {
            "mode": mode if mode in {"all", "arbitrage", "markets", "yield_bonds"} else "all",
            "searchQuery": search_query,
            "maxResults": max(1, min(int(max_results), 500)),
            "includeOrderbookDepth": bool(include_orderbook_depth),
        }
        try:
            items = _run_apify_actor_sync(ACTORS["prediction"]["slug"], actor_input, token)
            return {
                "execution_tier": "APIFY_CLOUD_LIVE_RUN",
                "actor": ACTORS["prediction"]["store_url"],
                "records_returned": len(items) if isinstance(items, list) else 1,
                "items": items,
            }
        except urllib.error.HTTPError as exc:
            err_detail = exc.read().decode("utf-8", errors="replace")[:400]
            return {
                "warning": f"Apify live run returned HTTP {exc.code}; serving public preview snapshot.",
                "apify_detail": err_detail,
                **_preview_prediction(search_query),
            }
        except Exception as exc:
            return {
                "warning": f"Apify live run error ({exc}); serving public preview snapshot.",
                **_preview_prediction(search_query),
            }

    return _preview_prediction(search_query)


def _preview_prediction(search_query: str = "") -> dict[str, Any]:
    feed = _load_public_preview_feed()
    pm_block = feed.get("prediction_markets", {})
    q = search_query.strip().lower()
    contracts = pm_block.get("top_contracts", [])
    spreads = pm_block.get("top_spreads", [])
    if q:
        contracts = [c for c in contracts if q in json.dumps(c).lower()]
        spreads = [s for s in spreads if q in json.dumps(s).lower()]
    return {
        **_upgrade_notice("prediction"),
        "snapshot_updated_at_utc": feed.get("updated_at_utc"),
        "total_contracts_scanned_in_preview": pm_block.get("total_contracts_scanned", 0),
        "top_live_spreads": spreads,
        "top_live_contracts": contracts,
    }


def tool_scan_crypto_funding_carry(
    min_net_apr_pct: float = 5.0,
    min_oi_usd: float = 75000.0,
    notional_usd: float = 10000.0,
    symbol_filter: str = "",
    max_results: int = 25,
) -> dict[str, Any]:
    """Scan delta-neutral perpetual funding-rate carry spreads across Hyperliquid, Kraken & dYdX."""
    token = _get_apify_token()
    if token:
        actor_input = {
            "minNetSpreadAprPct": float(min_net_apr_pct),
            "minOpenInterestUsd": float(min_oi_usd),
            "notionalUsd": float(notional_usd),
            "symbols": [s.strip().upper() for s in symbol_filter.split(",") if s.strip()] if symbol_filter else [],
            "maxResults": max(1, min(int(max_results), 500)),
        }
        try:
            items = _run_apify_actor_sync(ACTORS["funding"]["slug"], actor_input, token)
            return {
                "execution_tier": "APIFY_CLOUD_LIVE_RUN",
                "actor": ACTORS["funding"]["store_url"],
                "records_returned": len(items) if isinstance(items, list) else 1,
                "items": items,
            }
        except Exception as exc:
            return {
                "warning": f"Apify live run fallback ({exc}); serving hourly preview snapshot.",
                **_preview_funding(min_net_apr_pct, min_oi_usd, notional_usd, symbol_filter),
            }

    return _preview_funding(min_net_apr_pct, min_oi_usd, notional_usd, symbol_filter)


def _preview_funding(
    min_net_apr_pct: float,
    min_oi_usd: float,
    notional_usd: float,
    symbol_filter: str,
) -> dict[str, Any]:
    feed = _load_public_preview_feed()
    block = feed.get("perp_funding_carry", {})
    sym_q = symbol_filter.strip().upper()
    rows = []
    for r in block.get("top_carry_spreads", []):
        if r.get("net_spread_apr_pct", 0.0) < min_net_apr_pct:
            continue
        if r.get("min_oi_usd", 0.0) < min_oi_usd:
            continue
        if sym_q and sym_q not in str(r.get("symbol", "")).upper():
            continue
        apr = float(r.get("net_spread_apr_pct", 0.0))
        enriched = dict(r)
        enriched["notional_usd"] = round(float(notional_usd), 2)
        enriched["est_daily_yield_usd"] = round(float(notional_usd) * (apr / 100.0) / 365.0, 2)
        enriched["est_annual_yield_usd"] = round(float(notional_usd) * (apr / 100.0), 2)
        rows.append(enriched)

    return {
        **_upgrade_notice("funding"),
        "snapshot_updated_at_utc": feed.get("updated_at_utc"),
        "total_perp_markets_scanned_in_preview": block.get("total_perp_markets_scanned", 0),
        "carry_opportunities": rows,
        "major_benchmarks": block.get("major_benchmarks", []),
    }


def tool_enrich_shopify_b2b_leads(
    niche_keyword: str = "skincare",
    domains: list[str] | None = None,
    required_tech: list[str] | None = None,
    max_leads: int = 25,
) -> dict[str, Any]:
    """Discover & enrich Shopify stores with verified emails, social links, SKU stats & 40+ tech signatures."""
    token = _get_apify_token()
    if token:
        actor_input = {
            "searchQuery": niche_keyword,
            "domains": domains or [],
            "requiredTechStack": required_tech or [],
            "maxResults": max(1, min(int(max_leads), 500)),
        }
        try:
            items = _run_apify_actor_sync(ACTORS["shopify"]["slug"], actor_input, token)
            return {
                "execution_tier": "APIFY_CLOUD_LIVE_RUN",
                "actor": ACTORS["shopify"]["store_url"],
                "records_returned": len(items) if isinstance(items, list) else 1,
                "items": items,
            }
        except Exception as exc:
            return {
                "warning": f"Apify live run fallback ({exc}); serving schema & benchmark preview.",
                **_preview_shopify(niche_keyword),
            }

    return _preview_shopify(niche_keyword)


def _preview_shopify(niche_keyword: str) -> dict[str, Any]:
    feed = _load_public_preview_feed()
    block = feed.get("shopify_b2b_leads", {})
    return {
        **_upgrade_notice("shopify"),
        "requested_niche": niche_keyword,
        "enrichment_capabilities": block,
    }


def tool_get_live_terminal_summary() -> dict[str, Any]:
    """Return high-level summary of the hourly Quant & B2B Intelligence Hub snapshot and Apify endpoints."""
    feed = _load_public_preview_feed()
    return {
        "apify_token_configured": bool(_get_apify_token()),
        "updated_at_utc": feed.get("updated_at_utc"),
        "summary": feed.get("summary", {}),
        "apify_actors": feed.get("apify_actors", ACTORS),
        "live_terminal_url": "https://zk16k123-svg.github.io/quant-b2b-intel-hub/",
    }


# ---------------------------------------------------------------------------
# Dual-Mode Transport: FastMCP (if installed) OR Stdlib JSON-RPC 2.0 MCP
# ---------------------------------------------------------------------------

TOOL_SPECS = [
    {
        "name": "scan_prediction_market_arbitrage",
        "description": (
            "Scan Polymarket & Kalshi prediction market odds, L2 orderbook depth, "
            "cross-venue arbitrage spreads, NegRisk basket mispricings, and near-expiry yield bonds."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "enum": ["all", "arbitrage", "markets", "yield_bonds"],
                    "default": "all",
                    "description": "Output intelligence mode.",
                },
                "search_query": {
                    "type": "string",
                    "default": "",
                    "description": "Optional topic filter (e.g., 'Fed', 'Bitcoin', 'NBA', 'CPI').",
                },
                "max_results": {
                    "type": "integer",
                    "default": 25,
                    "description": "Max dataset items when running on Apify Cloud.",
                },
                "include_orderbook_depth": {
                    "type": "boolean",
                    "default": True,
                    "description": "Enrich live CLOB/L2 orderbook depth and $1k VWAP slippage.",
                },
            },
        },
    },
    {
        "name": "scan_crypto_funding_carry",
        "description": (
            "Compute live cross-venue delta-neutral perpetual funding-rate carry spreads "
            "(Hyperliquid, Kraken Futures, dYdX v4, and more via Apify) with USD yield estimates."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "min_net_apr_pct": {
                    "type": "number",
                    "default": 5.0,
                    "description": "Minimum annualized net carry spread (%).",
                },
                "min_oi_usd": {
                    "type": "number",
                    "default": 75000.0,
                    "description": "Minimum open interest per leg in USD.",
                },
                "notional_usd": {
                    "type": "number",
                    "default": 10000.0,
                    "description": "Capital notional in USD for daily/annual yield calculation.",
                },
                "symbol_filter": {
                    "type": "string",
                    "default": "",
                    "description": "Optional asset ticker filter (e.g., 'BTC', 'ETH', 'SOL').",
                },
                "max_results": {
                    "type": "integer",
                    "default": 25,
                },
            },
        },
    },
    {
        "name": "enrich_shopify_b2b_leads",
        "description": (
            "Discover and enrich Shopify DTC stores with verified emails, social links, "
            "product catalog metrics, and 40+ detected SaaS/ad-pixel tech stack signatures."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "niche_keyword": {
                    "type": "string",
                    "default": "skincare",
                    "description": "Store niche or category keyword.",
                },
                "domains": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional explicit Shopify store domains to enrich.",
                },
                "required_tech": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional tech filters (e.g., ['Klaviyo', 'Meta Pixel', 'Recharge']).",
                },
                "max_leads": {
                    "type": "integer",
                    "default": 25,
                },
            },
        },
    },
    {
        "name": "get_live_terminal_summary",
        "description": "Get live market counts, peak spreads/APRs, and Apify Cloud Actor endpoints.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]

TOOL_DISPATCH = {
    "scan_prediction_market_arbitrage": tool_scan_prediction_market_arbitrage,
    "scan_crypto_funding_carry": tool_scan_crypto_funding_carry,
    "enrich_shopify_b2b_leads": tool_enrich_shopify_b2b_leads,
    "get_live_terminal_summary": tool_get_live_terminal_summary,
}


def _run_stdlib_stdio_mcp() -> None:
    """Native line-delimited JSON-RPC 2.0 MCP server over stdin/stdout (zero external dependencies)."""
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue

        method = msg.get("method")
        msg_id = msg.get("id")
        params = msg.get("params") or {}

        # Notifications have no id and expect no response
        if msg_id is None:
            continue

        if method == "initialize":
            client_proto = params.get("protocolVersion") or "2024-11-05"
            reply = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "protocolVersion": client_proto,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                },
            }
        elif method == "ping":
            reply = {"jsonrpc": "2.0", "id": msg_id, "result": {}}
        elif method == "tools/list":
            reply = {"jsonrpc": "2.0", "id": msg_id, "result": {"tools": TOOL_SPECS}}
        elif method == "tools/call":
            t_name = params.get("name", "")
            t_args = params.get("arguments") or {}
            fn = TOOL_DISPATCH.get(t_name)
            if fn is None:
                reply = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "error": {"code": -32601, "message": f"Unknown tool: {t_name}"},
                }
            else:
                try:
                    data = fn(**t_args)
                    reply = {
                        "jsonrpc": "2.0",
                        "id": msg_id,
                        "result": {
                            "content": [{"type": "text", "text": json.dumps(data, indent=2, ensure_ascii=False)}],
                            "isError": False,
                        },
                    }
                except Exception as exc:
                    reply = {
                        "jsonrpc": "2.0",
                        "id": msg_id,
                        "result": {
                            "content": [{"type": "text", "text": json.dumps({"error": str(exc)})}],
                            "isError": True,
                        },
                    }
        else:
            reply = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {"code": -32601, "message": f"Method not supported: {method}"},
            }

        sys.stdout.write(json.dumps(reply, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def main() -> None:
    # Prefer FastMCP if installed unless `--stdlib` is passed; fall back cleanly to stdlib JSON-RPC
    if "--stdlib" not in sys.argv:
        try:
            from mcp.server.fastmcp import FastMCP  # type: ignore

            mcp = FastMCP(SERVER_NAME)
            mcp.tool(name="scan_prediction_market_arbitrage")(tool_scan_prediction_market_arbitrage)
            mcp.tool(name="scan_crypto_funding_carry")(tool_scan_crypto_funding_carry)
            mcp.tool(name="enrich_shopify_b2b_leads")(tool_enrich_shopify_b2b_leads)
            mcp.tool(name="get_live_terminal_summary")(tool_get_live_terminal_summary)
            mcp.run(transport="stdio")
            return
        except ImportError:
            pass

    _run_stdlib_stdio_mcp()


if __name__ == "__main__":
    main()
