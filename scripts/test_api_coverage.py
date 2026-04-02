#!/usr/bin/env python3
"""
Test Finnhub and API Ninjas ETF holdings endpoints on 10 UCITS European ETFs.

Compares: number of holdings returned, coverage vs expected, ISIN availability,
weight availability. Tries multiple ticker suffixes (.L, .DE, .PA, .AS) per ETF.

API keys read from env vars FINNHUB_API_KEY and API_NINJAS_KEY (loads .env if present).
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Load .env if present
# ---------------------------------------------------------------------------
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip("'\"")
        os.environ.setdefault(key.strip(), value)

FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "")
NINJAS_KEY = os.environ.get("API_NINJAS_KEY", "")

if not FINNHUB_KEY:
    print("WARNING: FINNHUB_API_KEY not set — Finnhub tests will be skipped.")
if not NINJAS_KEY:
    print("WARNING: API_NINJAS_KEY not set — API Ninjas tests will be skipped.")

# ---------------------------------------------------------------------------
# ETF universe
# ---------------------------------------------------------------------------
ETF_UNIVERSE: list[dict[str, Any]] = [
    {"name": "SWDA", "isin": "IE00B4L5Y983", "issuer": "iShares",   "expected_holdings": 1400},
    {"name": "CSPX", "isin": "IE00B5BMR087", "issuer": "iShares",   "expected_holdings": 503},
    {"name": "VWCE", "isin": "IE00BK5BQT80", "issuer": "Vanguard",  "expected_holdings": 3793},
    {"name": "VUSA", "isin": "IE00B3XXRP09", "issuer": "Vanguard",  "expected_holdings": 503},
    {"name": "CW8",  "isin": "LU1681043599", "issuer": "Amundi",    "expected_holdings": 1400},
    {"name": "XDWD", "isin": "IE00BK1PV551", "issuer": "Xtrackers", "expected_holdings": 1400},
    {"name": "EIMI", "isin": "IE00BKM4GZ66", "issuer": "iShares",   "expected_holdings": 3000},
    {"name": "VHYL", "isin": "IE00B8GKDB10", "issuer": "Vanguard",  "expected_holdings": 1800},
    {"name": "MEUD", "isin": "LU1089763773", "issuer": "Amundi",    "expected_holdings": 600},
    {"name": "SPYD", "isin": "IE00B6YX5D40", "issuer": "SPDR",      "expected_holdings": 80},
]

EXCHANGE_SUFFIXES = [".L", ".DE", ".PA", ".AS"]

# ---------------------------------------------------------------------------
# Finnhub
# ---------------------------------------------------------------------------

def finnhub_get_holdings(ticker: str) -> dict[str, Any]:
    """Call Finnhub ETF holdings endpoint. Returns parsed JSON with status info."""
    url = "https://finnhub.io/api/v1/etf/holdings"
    params = {"symbol": ticker, "token": FINNHUB_KEY}
    try:
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()
        if resp.status_code == 403 or data.get("error"):
            return {"_status": "access_denied", "_error": data.get("error", f"HTTP {resp.status_code}")}
        if resp.status_code == 200 and data.get("holdings"):
            data["_status"] = "ok"
            return data
        return {"_status": "empty", "_error": f"HTTP {resp.status_code}, no holdings in response"}
    except requests.RequestException as e:
        return {"_status": "request_error", "_error": str(e)}


def test_finnhub(etf: dict[str, Any]) -> dict[str, Any]:
    """Try Finnhub with multiple suffixes, return best result."""
    result: dict[str, Any] = {
        "api": "finnhub",
        "etf": etf["name"],
        "isin": etf["isin"],
        "expected_holdings": etf["expected_holdings"],
        "ticker_tried": [],
        "best_ticker": None,
        "holdings_returned": 0,
        "coverage_pct": 0.0,
        "has_isin": False,
        "has_weight": False,
        "error": None,
    }

    if not FINNHUB_KEY:
        result["error"] = "FINNHUB_API_KEY not set"
        return result

    # Also try the ISIN directly — Finnhub sometimes accepts it
    candidates = [etf["name"] + s for s in EXCHANGE_SUFFIXES] + [etf["isin"]]
    best_data = None
    best_count = 0
    best_ticker = None

    access_denied = False
    for ticker in candidates:
        result["ticker_tried"].append(ticker)
        data = finnhub_get_holdings(ticker)
        status = data.get("_status")
        if status == "access_denied":
            access_denied = True
        elif status == "ok" and len(data.get("holdings", [])) > best_count:
            best_data = data
            best_count = len(data["holdings"])
            best_ticker = ticker
        # Finnhub free tier: 60 calls/min — small pause to stay safe
        time.sleep(1.1)

    if best_data:
        holdings = best_data["holdings"]
        result["best_ticker"] = best_ticker
        result["holdings_returned"] = best_count
        result["coverage_pct"] = round(best_count / etf["expected_holdings"] * 100, 1)
        # Check first holding for ISIN and weight fields
        sample = holdings[0] if holdings else {}
        result["has_isin"] = bool(sample.get("isin"))
        result["has_weight"] = any(
            sample.get(k) is not None for k in ("percent", "share", "weight")
        )
    elif access_denied:
        result["error"] = "PREMIUM REQUIRED — ETF holdings endpoint needs paid plan"
    else:
        result["error"] = "No holdings returned for any ticker variant"

    return result


# ---------------------------------------------------------------------------
# API Ninjas
# ---------------------------------------------------------------------------

def ninjas_get_etf(ticker: str) -> dict[str, Any]:
    """Call API Ninjas ETF endpoint. Returns parsed response with status info."""
    url = "https://api.api-ninjas.com/v1/etf"
    headers = {"X-Api-Key": NINJAS_KEY}
    params = {"ticker": ticker}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        data = resp.json()
        if resp.status_code != 200:
            return {"_status": "error", "_error": data.get("error", f"HTTP {resp.status_code}")}

        # API Ninjas returns a dict with etf_ticker, holdings, num_holdings, etc.
        # Free tier returns "This data is for premium users only." for holdings
        holdings_val = data.get("holdings", "")
        num_holdings_val = data.get("num_holdings", "")

        if isinstance(holdings_val, str) and "premium" in holdings_val.lower():
            data["_status"] = "premium_required"
            data["_error"] = "Holdings data requires premium plan"
            return data

        if isinstance(holdings_val, list) and holdings_val:
            data["_status"] = "ok"
            return data

        # etf_name present means the ticker was recognized even if no holdings
        if data.get("etf_name"):
            data["_status"] = "recognized_no_holdings"
            data["_error"] = "ETF recognized but no holdings list returned"
            return data

        return {"_status": "not_found", "_error": "ETF not found"}
    except requests.RequestException as e:
        return {"_status": "request_error", "_error": str(e)}


def test_ninjas(etf: dict[str, Any]) -> dict[str, Any]:
    """Try API Ninjas with multiple suffixes, return best result."""
    result: dict[str, Any] = {
        "api": "api_ninjas",
        "etf": etf["name"],
        "isin": etf["isin"],
        "expected_holdings": etf["expected_holdings"],
        "ticker_tried": [],
        "best_ticker": None,
        "holdings_returned": 0,
        "coverage_pct": 0.0,
        "has_isin": False,
        "has_weight": False,
        "etf_recognized": False,
        "premium_required": False,
        "ninjas_metadata": {},
        "error": None,
    }

    if not NINJAS_KEY:
        result["error"] = "API_NINJAS_KEY not set"
        return result

    # API Ninjas uses bare tickers (no suffix) — try bare first, then with suffixes
    candidates = [etf["name"]] + [etf["name"] + s for s in EXCHANGE_SUFFIXES]
    best_data: dict[str, Any] | None = None
    best_count = 0
    best_ticker = None
    premium_blocked = False
    recognized = False

    for ticker in candidates:
        result["ticker_tried"].append(ticker)
        data = ninjas_get_etf(ticker)
        status = data.get("_status")

        if status == "premium_required":
            premium_blocked = True
            recognized = True
            # Store metadata even if holdings are gated
            if not best_data:
                best_data = data
                best_ticker = ticker
        elif status == "ok":
            holdings = data.get("holdings", [])
            if isinstance(holdings, list) and len(holdings) > best_count:
                best_data = data
                best_count = len(holdings)
                best_ticker = ticker
                recognized = True
        elif status == "recognized_no_holdings":
            recognized = True
            if not best_data:
                best_data = data
                best_ticker = ticker
        # Polite pause
        time.sleep(1.1)

    result["etf_recognized"] = recognized
    result["premium_required"] = premium_blocked

    if best_data and best_count > 0:
        holdings = best_data["holdings"]
        result["best_ticker"] = best_ticker
        result["holdings_returned"] = best_count
        result["coverage_pct"] = round(best_count / etf["expected_holdings"] * 100, 1)
        sample = holdings[0] if holdings else {}
        result["has_isin"] = bool(sample.get("isin"))
        result["has_weight"] = any(
            sample.get(k) is not None
            for k in ("weight", "percent", "weight_pct", "percentage")
        )
    elif premium_blocked:
        result["best_ticker"] = best_ticker
        # Capture available free metadata
        result["ninjas_metadata"] = {
            k: v for k, v in (best_data or {}).items()
            if not k.startswith("_") and not isinstance(v, str) or
            (isinstance(v, str) and "premium" not in v.lower())
        }
        result["error"] = "PREMIUM REQUIRED — holdings data needs paid plan"
    elif recognized:
        result["best_ticker"] = best_ticker
        result["error"] = "ETF recognized but no holdings data returned"
    else:
        result["error"] = "ETF not found for any ticker variant"

    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_table(results: list[dict[str, Any]]) -> None:
    """Print a formatted comparison table."""
    header = (
        f"{'ETF':<6} {'API':<12} {'Best Ticker':<14} {'Holdings':<10} "
        f"{'Expected':<10} {'Cov%':<8} {'ISIN?':<6} {'Wt%?':<6} {'Status'}"
    )
    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))

    for r in sorted(results, key=lambda x: (x["etf"], x["api"])):
        print(
            f"{r['etf']:<6} {r['api']:<12} {(r['best_ticker'] or '-'):<14} "
            f"{r['holdings_returned']:<10} {r['expected_holdings']:<10} "
            f"{r['coverage_pct']:<8} {'Y' if r['has_isin'] else 'N':<6} "
            f"{'Y' if r['has_weight'] else 'N':<6} "
            f"{r.get('error') or 'OK — ' + str(r['holdings_returned']) + ' holdings'}"
        )

    print("=" * len(header))


def compute_verdict(results: list[dict[str, Any]]) -> str:
    """Return a verdict string comparing the two APIs."""
    finnhub = [r for r in results if r["api"] == "finnhub"]
    ninjas = [r for r in results if r["api"] == "api_ninjas"]

    def _score(group: list[dict[str, Any]]) -> dict[str, Any]:
        etfs_with_data = sum(1 for r in group if r["holdings_returned"] > 0)
        premium_blocked = sum(1 for r in group if "PREMIUM" in (r.get("error") or ""))
        avg_cov = (
            sum(r["coverage_pct"] for r in group) / len(group) if group else 0
        )
        isins = sum(1 for r in group if r["has_isin"])
        weights = sum(1 for r in group if r["has_weight"])
        return {
            "etfs_with_data": etfs_with_data,
            "premium_blocked": premium_blocked,
            "avg_coverage": round(avg_cov, 1),
            "isin_count": isins,
            "weight_count": weights,
        }

    fh = _score(finnhub)
    nn = _score(ninjas)

    lines = [
        "\n===== VERDICT =====",
        "",
        "Finnhub:",
        f"  ETFs with holdings data: {fh['etfs_with_data']}/10",
        f"  Premium-gated (no data on free tier): {fh['premium_blocked']}/10",
        f"  Avg coverage: {fh['avg_coverage']}%",
        f"  ISIN per holding: {fh['isin_count']}/10",
        f"  Weight % per holding: {fh['weight_count']}/10",
        "",
        "API Ninjas:",
        f"  ETFs with holdings data: {nn['etfs_with_data']}/10",
        f"  Premium-gated (no data on free tier): {nn['premium_blocked']}/10",
        f"  Avg coverage: {nn['avg_coverage']}%",
        f"  ISIN per holding: {nn['isin_count']}/10",
        f"  Weight % per holding: {nn['weight_count']}/10",
        "",
    ]

    # Check if both are fully premium-gated
    both_gated = (fh["etfs_with_data"] == 0 and nn["etfs_with_data"] == 0)

    if both_gated:
        lines.append("RESULT: NEITHER API provides ETF holdings on free tier.")
        lines.append("")
        if fh["premium_blocked"] > 0 and nn["premium_blocked"] > 0:
            lines.append("Both APIs gate holdings behind premium plans.")
        if fh["premium_blocked"] > 0:
            lines.append("  Finnhub: ETF holdings endpoint requires paid subscription.")
        if nn["premium_blocked"] > 0:
            lines.append("  API Ninjas: Holdings field returns 'premium users only'.")
        lines.append("")
        lines.append("RECOMMENDATION for UCITS ETF holdings:")
        lines.append("  - Neither free tier is viable for production use.")
        lines.append("  - Consider alternatives: direct issuer CSV downloads (iShares, Vanguard),")
        lines.append("    OpenFIGI for security resolution, or JustETF scraping.")
        lines.append("  - If budget allows, evaluate Finnhub premium vs API Ninjas premium")
        lines.append("    specifically for UCITS/European ETF coverage.")
    else:
        # Original scoring logic
        fh_score = fh["etfs_with_data"] * 3 + fh["avg_coverage"] * 0.02 + fh["isin_count"] * 2 + fh["weight_count"]
        nn_score = nn["etfs_with_data"] * 3 + nn["avg_coverage"] * 0.02 + nn["isin_count"] * 2 + nn["weight_count"]

        if fh_score > nn_score * 1.1:
            winner = "Finnhub"
        elif nn_score > fh_score * 1.1:
            winner = "API Ninjas"
        else:
            winner = "Roughly equivalent — consider pricing/rate-limits"

        lines.append(f"WINNER for UCITS ETF holdings: {winner}")
        lines.append(
            "(Criteria: data availability for EU tickers, coverage depth, "
            "ISIN per holding, weight percentages)"
        )

    verdict = "\n".join(lines)
    print(verdict)
    return verdict


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("ETF Holdings API Coverage Test")
    print(f"Testing {len(ETF_UNIVERSE)} UCITS European ETFs")
    print(f"Suffixes to try: {EXCHANGE_SUFFIXES}")
    print("=" * 60)

    all_results: list[dict[str, Any]] = []

    for i, etf in enumerate(ETF_UNIVERSE, 1):
        print(f"\n[{i}/{len(ETF_UNIVERSE)}] Testing {etf['name']} ({etf['isin']}, {etf['issuer']})...")

        if FINNHUB_KEY:
            print(f"  Finnhub...", end=" ", flush=True)
            fh_result = test_finnhub(etf)
            all_results.append(fh_result)
            print(f"{fh_result['holdings_returned']} holdings" if not fh_result["error"] else fh_result["error"])

        if NINJAS_KEY:
            print(f"  API Ninjas...", end=" ", flush=True)
            nn_result = test_ninjas(etf)
            all_results.append(nn_result)
            print(f"{nn_result['holdings_returned']} holdings" if not nn_result["error"] else nn_result["error"])

    # Print comparison table
    print_table(all_results)

    # Verdict
    verdict = compute_verdict(all_results)

    # Save results
    output_path = Path(__file__).resolve().parent / "api_test_results.json"
    output = {
        "etfs_tested": [e["name"] for e in ETF_UNIVERSE],
        "suffixes_tried": EXCHANGE_SUFFIXES,
        "results": all_results,
    }
    output_path.write_text(json.dumps(output, indent=2, default=str))
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
