# Quant & B2B Intelligence Hub — Live Terminal & MCP Server

[![Refresh Live Quant & B2B Intel Feed](https://github.com/zk16k123-svg/quant-b2b-intel-hub/actions/workflows/refresh-live-intel.yml/badge.svg)](https://github.com/zk16k123-svg/quant-b2b-intel-hub/actions/workflows/refresh-live-intel.yml)
[![Live Terminal](https://img.shields.io/badge/Live_Terminal-GitHub_Pages-16A34A)](https://zk16k123-svg.github.io/quant-b2b-intel-hub/)
[![MCP Compatible](https://img.shields.io/badge/MCP-Claude_%7C_Cursor_%7C_Windsurf_%7C_Cline-0B0C0E)](https://github.com/zk16k123-svg/quant-b2b-intel-hub#mcp-server-setup-claude-desktop--cursor--windsurf--cline)

**Live Public Terminal (Refreshed Hourly 24/7):**  
**[https://zk16k123-svg.github.io/quant-b2b-intel-hub/](https://zk16k123-svg.github.io/quant-b2b-intel-hub/)**

Real-time institutional market intelligence terminal and **Model Context Protocol (MCP)** server connecting AI Agents, quantitative researchers, and growth engineers to three production **Apify Cloud Actors**:

| Intelligence Stream | Apify Cloud Actor | Full Cloud Scope |
|---|---|---|
| **01. Prediction Market Arbitrage** | [`conscious_chuckwalla/prediction-market-arb-intel`](https://apify.com/conscious_chuckwalla/prediction-market-arb-intel) | **3,400+ active Polymarket & Kalshi contracts**, L2 CLOB orderbooks, `$1,000` VWAP slippage, cross-venue arbitrage, NegRisk sum-to-one baskets & near-expiry yield bonds |
| **02. Crypto Perp Funding Carry** | [`conscious_chuckwalla/crypto-funding-rate-carry-arb`](https://apify.com/conscious_chuckwalla/crypto-funding-rate-carry-arb) | **650+ perpetual markets** (Hyperliquid, Kraken Futures, dYdX v4 & more), OI-filtered delta-neutral carry spreads, realized stability & notional USD yield sizing |
| **03. B2B Shopify Lead & Tech Stack** | [`conscious_chuckwalla/shopify-leads-tech-stack-intel`](https://apify.com/conscious_chuckwalla/shopify-leads-tech-stack-intel) | **Verified DTC merchant emails**, social profiles, SKU/AOV catalog pricing & **40+ SaaS / pixel signatures** (Klaviyo, Attentive, Recharge, Gorgias, Meta/TikTok Pixel) |

---

## Architecture: Hourly Public Preview + On-Demand Full Depth

1. **7×24 Automated Public Snapshot (`docs/data/live_feed.json`)**:
   - Updated every hour (`12 * * * *` UTC) via `.github/workflows/refresh-live-intel.yml` using zero-dependency Python 3.11 stdlib (`scripts/update_live_preview.py`).
   - Queries official public read-only endpoints across **Polymarket Gamma**, **Kalshi Trade API v2**, **Hyperliquid**, **Kraken Futures**, and **dYdX v4**.
2. **Interactive Web Terminal (`docs/index.html`)**:
   - Live cross-venue arbitrage & perpetual carry matrix, interactive notional yield calculator (`$10k` / `$50k` / `$100k` / custom), B2B Shopify lead segment explorer, and one-click MCP / Python / cURL snippets.
3. **Dual-Tier MCP Server (`mcp_server.py`)**:
   - **With `APIFY_TOKEN` set**: Executes synchronous on-demand cloud runs (`run-sync-get-dataset-items`) against `conscious_chuckwalla/*` for full market depth, custom keyword/tech filters, and L2 orderbook slippage.
   - **Without `APIFY_TOKEN`**: Zero-auth fallback to the hourly `live_feed.json` preview so agents work immediately on first install.

---

## MCP Server Setup (Claude Desktop / Cursor / Windsurf / Cline)

Grab your free Apify API token at **[console.apify.com/settings/integrations](https://console.apify.com/settings/integrations)** to unlock full real-time dataset runs across all 3,400+ prediction contracts, 650+ perpetuals, and live Shopify store leads.

### Option A: Zero-Install via `uvx` (Recommended)

Add to `claude_desktop_config.json` or `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "quant-b2b-intel": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/zk16k123-svg/quant-b2b-intel-hub",
        "quant-b2b-intel-mcp"
      ],
      "env": {
        "APIFY_TOKEN": "apify_api_YOUR_TOKEN_HERE"
      }
    }
  }
}
```

### Option B: Direct Python Execution (Zero External Dependencies Required)

`mcp_server.py` includes a built-in Python 3.10+ stdlib JSON-RPC 2.0 stdio transport—you can run it directly after cloning without installing a single package:

```bash
git clone https://github.com/zk16k123-svg/quant-b2b-intel-hub.git
cd quant-b2b-intel-hub
python mcp_server.py
```

```json
{
  "mcpServers": {
    "quant-b2b-intel": {
      "command": "python",
      "args": ["/absolute/path/to/quant-b2b-intel-hub/mcp_server.py"],
      "env": {
        "APIFY_TOKEN": "apify_api_YOUR_TOKEN_HERE"
      }
    }
  }
}
```

### Exposed MCP Tools

- `scan_prediction_market_arbitrage(mode, search_query, max_results, include_orderbook_depth)`
- `scan_crypto_funding_carry(min_net_apr_pct, min_oi_usd, notional_usd, symbol_filter, max_results)`
- `enrich_shopify_b2b_leads(niche_keyword, domains, required_tech, max_leads)`
- `get_live_terminal_summary()`

---

## Direct REST API & Python Quickstart

### 1. Polymarket & Kalshi Arbitrage + L2 Orderbook Slippage

```bash
curl -X POST "https://api.apify.com/v2/acts/conscious_chuckwalla~prediction-market-arb-intel/run-sync-get-dataset-items?token=$APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "all",
    "venues": ["polymarket", "kalshi"],
    "minVolume24h": 1000,
    "includeOrderbookDepth": true,
    "maxResults": 100
  }'
```

### 2. Crypto Perpetual Funding-Rate Delta-Neutral Carry

```bash
curl -X POST "https://api.apify.com/v2/acts/conscious_chuckwalla~crypto-funding-rate-carry-arb/run-sync-get-dataset-items?token=$APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "minNetSpreadAprPct": 10.0,
    "minOpenInterestUsd": 100000,
    "notionalUsd": 50000,
    "maxResults": 50
  }'
```

### 3. Shopify Store Leads, Verified Emails & Tech Stack Enricher

```bash
curl -X POST "https://api.apify.com/v2/acts/conscious_chuckwalla~shopify-leads-tech-stack-intel/run-sync-get-dataset-items?token=$APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "searchQuery": "specialty coffee",
    "requiredTechStack": ["Klaviyo"],
    "maxResults": 50
  }'
```

---

## Refreshing the Public Feed Locally

```bash
python scripts/update_live_preview.py
```

Writes fresh market telemetry to `docs/data/live_feed.json` using only the Python 3.11 standard library.

## License

MIT — Data provided strictly for quantitative research, engineering, and informational workflows. Not financial advice.
