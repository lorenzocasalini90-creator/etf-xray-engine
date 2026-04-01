"""Temporary network diagnostics page for Vanguard endpoint testing."""

from __future__ import annotations

import socket
import time

import requests
import streamlit as st

st.set_page_config(page_title="Network Test", page_icon="🔧", layout="wide")

st.warning("Pagina di diagnostica temporanea — rimuovere dopo il test")
st.title("Network Connectivity Test")
st.caption(
    "Testa la raggiungibilità degli endpoint Vanguard da questa rete. "
    "Utile per confrontare rete locale vs Streamlit Cloud (AWS US)."
)

TIMEOUT = 10
MAX_RETRIES = 3

ENDPOINTS = [
    ("eds.ecs.gisp.c1.vanguard.com", "https://eds.ecs.gisp.c1.vanguard.com/", "API interna etf-scraper"),
    ("www.vanguard.co.uk", "https://www.vanguard.co.uk/professional/product/etf/equity/9679/ftse-all-world-ucits-etf-usd-accumulating", "Sito UK — pagina VWCE"),
]


def dns_resolve(hostname: str) -> tuple[str, str]:
    """Resolve hostname via getaddrinfo. Returns (status, detail)."""
    try:
        results = socket.getaddrinfo(hostname, 443, socket.AF_INET, socket.SOCK_STREAM)
        ips = sorted({r[4][0] for r in results})
        return "OK", ", ".join(ips)
    except socket.gaierror as exc:
        return "FAIL", str(exc)


def http_get(url: str) -> tuple[str, str]:
    """GET with retry. Returns (status, detail)."""
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    last_error = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            ct = resp.headers.get("Content-Type", "")
            return f"{resp.status_code}", f"{len(resp.content)} bytes, {ct[:50]}"
        except requests.exceptions.ConnectionError as exc:
            last_error = f"ConnectionError: {exc}"
        except requests.exceptions.Timeout:
            last_error = f"Timeout ({TIMEOUT}s)"
        except requests.exceptions.RequestException as exc:
            last_error = str(exc)
        if attempt < MAX_RETRIES:
            time.sleep(1)
    return "FAIL", f"After {MAX_RETRIES} attempts: {last_error[:120]}"


def test_etf_scraper_vt() -> tuple[bool, str, object]:
    """Try fetching VT holdings via etf-scraper. Returns (ok, message, df_or_none)."""
    try:
        from etf_scraper import ETFScraper
    except ImportError:
        return False, "etf-scraper non installato", None

    try:
        scraper = ETFScraper()
    except Exception as exc:
        return False, f"ETFScraper init failed: {exc}", None

    last_error = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = scraper.query_holdings("VT")
            return True, f"{len(df)} holdings scaricate", df
        except Exception as exc:
            last_error = str(exc)
        if attempt < MAX_RETRIES:
            time.sleep(2)
    return False, f"Fallito dopo {MAX_RETRIES} tentativi: {last_error[:200]}", None


if st.button("Run Test", type="primary"):
    # --- External IP ---
    with st.spinner("Rilevamento IP esterno..."):
        try:
            ip_resp = requests.get("https://httpbin.org/ip", timeout=5)
            ext_ip = ip_resp.json().get("origin", "unknown")
        except Exception:
            ext_ip = "non determinabile"
    st.info(f"IP esterno: **{ext_ip}**")

    # --- DNS + HTTP tests ---
    rows = []
    progress = st.progress(0, text="Testing endpoints...")

    for i, (hostname, url, desc) in enumerate(ENDPOINTS):
        progress.progress((i + 1) / (len(ENDPOINTS) + 1), text=f"Testing {hostname}...")

        dns_status, dns_detail = dns_resolve(hostname)
        if dns_status == "OK":
            http_status, http_detail = http_get(url)
        else:
            http_status, http_detail = "SKIP", "DNS non risolto"

        rows.append({
            "Endpoint": hostname,
            "Descrizione": desc,
            "DNS": dns_status,
            "DNS Detail": dns_detail,
            "HTTP": http_status,
            "HTTP Detail": http_detail,
        })

    progress.progress(1.0, text="Done")

    st.subheader("Risultati connettività")
    st.dataframe(rows, use_container_width=True)

    # --- Highlight key result ---
    eds_row = rows[0]
    if eds_row["DNS"] == "OK" and eds_row["HTTP"] != "FAIL":
        st.success("eds.ecs.gisp.c1.vanguard.com è raggiungibile — etf-scraper dovrebbe funzionare!")
    else:
        st.error(
            "eds.ecs.gisp.c1.vanguard.com NON raggiungibile da questa rete. "
            "Il fetcher Vanguard US (via etf-scraper) non funzionerà."
        )

    # --- etf-scraper VT test ---
    eds_dns_ok = eds_row["DNS"] == "OK"
    if eds_dns_ok:
        st.subheader("Test etf-scraper: VT (Vanguard Total World)")
        with st.spinner("Scaricamento holdings VT..."):
            ok, message, df = test_etf_scraper_vt()

        if ok and df is not None:
            st.success(message)
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Holdings totali", len(df))
            with col2:
                cols = [c for c in df.columns if "weight" in c.lower()]
                if cols:
                    st.metric("Peso top-1", f"{df[cols[0]].max():.2f}%")
            st.caption("Prime 5 righe:")
            st.dataframe(df.head(5), use_container_width=True)
        else:
            st.error(message)
    else:
        st.info(
            "Test etf-scraper VT saltato: il DNS di eds.ecs.gisp.c1.vanguard.com "
            "non risolve da questa rete."
        )
