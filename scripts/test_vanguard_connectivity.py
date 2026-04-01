#!/usr/bin/env python3
"""Vanguard connectivity diagnostic script.

Tests reachability of all Vanguard endpoints from the current network.
Designed to run both locally and on Streamlit Cloud (AWS US) to compare.

Usage:
    python scripts/test_vanguard_connectivity.py
"""

from __future__ import annotations

import re
import socket
import sys
import time
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TIMEOUT = 10  # seconds per request
MAX_RETRIES = 3

ENDPOINTS = {
    "eds.ecs.gisp.c1.vanguard.com": {
        "description": "Internal API used by etf-scraper for US-listed Vanguard ETFs",
        "urls": [
            "https://eds.ecs.gisp.c1.vanguard.com/eds-eip-gatekeeper-svc/api/product/EQUITY_ETF",
        ],
    },
    "www.vanguard.co.uk": {
        "description": "Vanguard UK site — potential UCITS holdings source",
        "urls": [
            "https://www.vanguard.co.uk/professional/product/etf/equity/9679/ftse-all-world-ucits-etf-usd-accumulating",
        ],
    },
    "global.vanguard.com": {
        "description": "Vanguard international site",
        "urls": [
            "https://global.vanguard.com/portal/site/loadPDF?country=global&docId=31337",
        ],
    },
    "fund-docs.vanguard.com": {
        "description": "Vanguard fund documents (factsheets, reports)",
        "urls": [
            "https://fund-docs.vanguard.com/",
        ],
    },
}

# VWCE product page + potential holdings download patterns
VWCE_ISIN = "IE00BK5BQT80"
VWCE_PRODUCT_ID = "9679"
VWCE_URLS_TO_PROBE = [
    # Product page
    "https://www.vanguard.co.uk/professional/product/etf/equity/9679/ftse-all-world-ucits-etf-usd-accumulating",
    # Possible CSV/Excel download patterns
    "https://www.vanguard.co.uk/professional/product/etf/equity/9679/ftse-all-world-ucits-etf-usd-accumulating/portfolio-holding",
    "https://fund-docs.vanguard.com/holdings-detail-international-etf-VWCE.csv",
    "https://global.vanguard.com/portal/site/loadPDF?country=ie&docId=31337&fundId=9679",
]

SESSION = requests.Session()
SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def print_header(text: str) -> None:
    width = 72
    print()
    print("=" * width)
    print(f"  {text}")
    print("=" * width)


def print_sub(text: str) -> None:
    print(f"\n--- {text} ---")


def dns_resolve(hostname: str) -> str | None:
    """Resolve hostname to IP. Returns IP string or None."""
    try:
        ip = socket.gethostbyname(hostname)
        return ip
    except socket.gaierror as exc:
        return None


def http_get(url: str, attempt: int = 1) -> dict:
    """GET with retry. Returns result dict."""
    result = {
        "url": url,
        "status": None,
        "content_type": None,
        "content_length": None,
        "preview": None,
        "error": None,
        "elapsed_ms": None,
        "redirect_url": None,
    }

    try:
        start = time.monotonic()
        resp = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
        elapsed = (time.monotonic() - start) * 1000
        result["status"] = resp.status_code
        result["content_type"] = resp.headers.get("Content-Type", "")
        result["content_length"] = len(resp.content)
        result["elapsed_ms"] = round(elapsed)
        if resp.url != url:
            result["redirect_url"] = resp.url
        # Preview: first 200 chars of text, or note binary
        ct = result["content_type"]
        if "text" in ct or "json" in ct or "xml" in ct:
            result["preview"] = resp.text[:200]
        else:
            result["preview"] = f"[binary: {len(resp.content)} bytes]"
    except requests.exceptions.ConnectionError as exc:
        result["error"] = f"ConnectionError: {exc}"
    except requests.exceptions.Timeout:
        result["error"] = f"Timeout after {TIMEOUT}s"
    except requests.exceptions.RequestException as exc:
        result["error"] = f"RequestError: {exc}"

    return result


def print_result(r: dict) -> None:
    if r["error"]:
        print(f"  FAILED: {r['error']}")
    else:
        print(f"  Status: {r['status']}")
        print(f"  Content-Type: {r['content_type']}")
        print(f"  Size: {r['content_length']} bytes")
        print(f"  Elapsed: {r['elapsed_ms']}ms")
        if r["redirect_url"]:
            print(f"  Redirected to: {r['redirect_url']}")
        print(f"  Preview: {r['preview'][:200]}")


def search_download_links(html: str) -> list[str]:
    """Extract potential holdings download links from HTML."""
    patterns = [
        r'href="([^"]*(?:holdings|download|csv|xlsx|export)[^"]*)"',
        r'href="([^"]*\.csv[^"]*)"',
        r'href="([^"]*\.xlsx?[^"]*)"',
        r'data-url="([^"]*(?:holdings|download)[^"]*)"',
    ]
    links = []
    for pat in patterns:
        links.extend(re.findall(pat, html, re.IGNORECASE))
    return list(dict.fromkeys(links))  # dedupe, preserve order


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print_header("VANGUARD CONNECTIVITY DIAGNOSTIC")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"Python: {sys.version}")

    # Try to detect if we're on Streamlit Cloud
    try:
        import streamlit
        print(f"Streamlit version: {streamlit.__version__}")
    except ImportError:
        print("Streamlit: not installed (running standalone)")

    # External IP for network identification
    try:
        ip_resp = SESSION.get("https://httpbin.org/ip", timeout=5)
        print(f"External IP: {ip_resp.json().get('origin', 'unknown')}")
    except Exception:
        print("External IP: could not determine")

    # -----------------------------------------------------------------------
    # Step 1: DNS resolution for all endpoints
    # -----------------------------------------------------------------------
    print_header("STEP 1: DNS Resolution")

    dns_results: dict[str, str | None] = {}
    for hostname, info in ENDPOINTS.items():
        ip = dns_resolve(hostname)
        dns_results[hostname] = ip
        status = f"{ip}" if ip else "FAILED (DNS unreachable)"
        print(f"  {hostname:45s} -> {status}")
        print(f"    ({info['description']})")

    # -----------------------------------------------------------------------
    # Step 2: HTTP connectivity for each endpoint
    # -----------------------------------------------------------------------
    print_header("STEP 2: HTTP Connectivity")

    reachable_hosts: set[str] = set()
    for hostname, info in ENDPOINTS.items():
        print_sub(hostname)
        if not dns_results[hostname]:
            print("  SKIPPED: DNS resolution failed")
            continue

        for url in info["urls"]:
            print(f"\n  GET {url}")
            last_error = None
            for attempt in range(1, MAX_RETRIES + 1):
                result = http_get(url, attempt)
                if not result["error"]:
                    print_result(result)
                    reachable_hosts.add(hostname)
                    break
                last_error = result["error"]
                if attempt < MAX_RETRIES:
                    print(f"  Attempt {attempt}/{MAX_RETRIES} failed, retrying...")
                    time.sleep(1)
            else:
                print(f"  FAILED after {MAX_RETRIES} attempts: {last_error}")

    # -----------------------------------------------------------------------
    # Step 3: VWCE holdings discovery (if vanguard.co.uk reachable)
    # -----------------------------------------------------------------------
    print_header("STEP 3: VWCE Holdings Discovery")

    if "www.vanguard.co.uk" in reachable_hosts:
        for url in VWCE_URLS_TO_PROBE:
            print(f"\n  Probing: {url}")
            result = http_get(url)
            print_result(result)

            # Search for download links in HTML responses
            if (
                result["status"] == 200
                and result["preview"]
                and "text" in (result["content_type"] or "")
            ):
                # Need full HTML for link search
                try:
                    full_resp = SESSION.get(url, timeout=TIMEOUT)
                    links = search_download_links(full_resp.text)
                    if links:
                        print(f"\n  Found {len(links)} potential download link(s):")
                        for link in links[:10]:
                            print(f"    - {link}")
                    else:
                        print("  No download links found in HTML")
                except Exception as exc:
                    print(f"  Could not re-fetch for link search: {exc}")
    else:
        print("  SKIPPED: www.vanguard.co.uk not reachable")

    # -----------------------------------------------------------------------
    # Step 4: etf-scraper VT test (US equivalent of VWCE)
    # -----------------------------------------------------------------------
    print_header("STEP 4: etf-scraper VT Test (US equivalent of VWCE)")

    try:
        from etf_scraper import ETFScraper

        scraper = ETFScraper()
        print("  etf-scraper loaded successfully")
        print(f"  Vanguard tickers available: {sum(1 for _, r in scraper.listings_df.iterrows() if r.get('provider') == 'Vanguard')}")

        # Try fetching VT holdings
        print("\n  Fetching VT holdings...")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                start = time.monotonic()
                df = scraper.query_holdings("VT")
                elapsed = time.monotonic() - start
                print(f"  SUCCESS in {elapsed:.1f}s")
                print(f"  Holdings: {len(df)} rows")
                print(f"  Columns: {list(df.columns)}")
                print(f"  Top 5:")
                if "weight" in df.columns:
                    top5 = df.nlargest(5, "weight")
                else:
                    top5 = df.head(5)
                for _, row in top5.iterrows():
                    name = row.get("name", "?")
                    weight = row.get("weight", "?")
                    print(f"    {name:40s}  {weight}")
                break
            except Exception as exc:
                print(f"  Attempt {attempt}/{MAX_RETRIES} failed: {exc}")
                if attempt < MAX_RETRIES:
                    time.sleep(2)
        else:
            print(f"  FAILED after {MAX_RETRIES} attempts")

    except ImportError:
        print("  etf-scraper not installed — skipping")
    except Exception as exc:
        print(f"  etf-scraper init failed: {exc}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print_header("SUMMARY")

    all_hosts = list(ENDPOINTS.keys())
    for h in all_hosts:
        dns_ok = "OK" if dns_results[h] else "FAIL"
        http_ok = "OK" if h in reachable_hosts else "FAIL"
        print(f"  {h:45s}  DNS: {dns_ok:4s}  HTTP: {http_ok}")

    print()
    if "eds.ecs.gisp.c1.vanguard.com" not in reachable_hosts:
        print("  >> etf-scraper Vanguard API is NOT reachable from this network.")
        print("  >> Deploy this script on Streamlit Cloud to test from AWS US.")
    else:
        print("  >> etf-scraper Vanguard API IS reachable — fetcher should work.")

    if "www.vanguard.co.uk" in reachable_hosts:
        print("  >> Vanguard UK site is reachable — UCITS scraping may be possible.")
    else:
        print("  >> Vanguard UK site is NOT reachable.")

    print()


if __name__ == "__main__":
    main()
