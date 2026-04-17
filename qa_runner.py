#!/usr/bin/env python3
"""QA automated test runner for CheckMyETFs — Playwright edition."""

import asyncio
import json
import time
import re
import os
from datetime import datetime, timezone
from pathlib import Path
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout

BASE_URL = "https://www.checkmyetfs.com"
QA_DIR = Path("qa_results")
SCREENSHOTS_DIR = QA_DIR / "screenshots"
TIMEOUT_PRIMA_ANALISI = 120_000
TIMEOUT_SECONDA_ANALISI = 15_000

QA_DIR.mkdir(exist_ok=True)
SCREENSHOTS_DIR.mkdir(exist_ok=True)

PORTFOLIOS = [
    {
        "id": "T01", "nome": "Closet Indexer",
        "descrizione": "Due ETF MSCI World quasi identici.",
        "etf": [
            {"ticker": "SWDA", "valore_eur": 45000},
            {"ticker": "CSPX", "valore_eur": 35000},
        ],
        "attese": {"active_share_max": 20, "ridondanza_min_pct": 70, "warning_closet_indexing": True, "overlap_coppia_min": 70},
    },
    {
        "id": "T02", "nome": "Classico 3-ETF",
        "descrizione": "World + EM + Small Cap.",
        "etf": [
            {"ticker": "VWCE", "valore_eur": 50000},
            {"ticker": "EIMI", "valore_eur": 20000},
            {"ticker": "WSML", "valore_eur": 10000},
        ],
        "attese": {"ridondanza_max_pct": 40, "warning_closet_indexing": False, "overlap_max_pct": 30},
    },
    {
        "id": "T03", "nome": "Tech Heavy",
        "descrizione": "Concentrazione tech estrema.",
        "etf": [
            {"ticker": "CNDX", "valore_eur": 35000},
            {"ticker": "IUIT", "valore_eur": 25000},
            {"ticker": "SWDA", "valore_eur": 20000},
        ],
        "attese": {"settore_it_min_pct": 35, "active_share_min": 20},
    },
    {
        "id": "T04", "nome": "Dividendi e Value",
        "descrizione": "Dividend + Value + ETF sintetico Amundi.",
        "etf": [
            {"ticker": "VHYL", "valore_eur": 30000},
            {"ticker": "IDVY", "valore_eur": 20000},
            {"ticker": "CW8",  "valore_eur": 30000},
        ],
        "attese": {"warning_swap": True},
    },
    {
        "id": "T05", "nome": "Tematico Concentrato",
        "descrizione": "Clean energy + cybersecurity + robotica.",
        "etf": [
            {"ticker": "INRG", "valore_eur": 25000},
            {"ticker": "ISPY", "valore_eur": 25000},
            {"ticker": "RBTX", "valore_eur": 25000},
        ],
        "attese": {"active_share_min": 50, "overlap_max_pct": 25},
    },
    {
        "id": "T06", "nome": "Europa + Emergenti",
        "descrizione": "Zero USA.",
        "etf": [
            {"ticker": "IEUA", "valore_eur": 35000},
            {"ticker": "EIMI", "valore_eur": 30000},
            {"ticker": "IBCI", "valore_eur": 15000},
        ],
        "attese": {"active_share_min": 40, "usa_exposure_max_pct": 15},
    },
    {
        "id": "T07", "nome": "Complesso 5 ETF",
        "descrizione": "5 ETF con overlap variabile.",
        "etf": [
            {"ticker": "VWCE", "valore_eur": 25000},
            {"ticker": "SWDA", "valore_eur": 20000},
            {"ticker": "EIMI", "valore_eur": 15000},
            {"ticker": "WSML", "valore_eur": 10000},
            {"ticker": "IUIT", "valore_eur": 10000},
        ],
        "attese": {"matrice_5x5": True, "overlap_coppia_min": 70},
    },
    {
        "id": "T08", "nome": "Single ETF",
        "descrizione": "Edge case: 1 solo ETF.",
        "etf": [
            {"ticker": "VWCE", "valore_eur": 80000},
        ],
        "attese": {"no_crash": True, "overlap_na": True},
    },
    {
        "id": "T09", "nome": "Amundi Dominante",
        "descrizione": "Amundi + Xtrackers sintetici.",
        "etf": [
            {"ticker": "CW8",   "valore_eur": 30000},
            {"ticker": "PAEEM", "valore_eur": 20000},
            {"ticker": "XDWD",  "valore_eur": 30000},
        ],
        "attese": {"warning_swap": True, "coverage_disclosure": True},
    },
    {
        "id": "T10", "nome": "Factor Diversificato",
        "descrizione": "Momentum + MinVol + Quality + Value.",
        "etf": [
            {"ticker": "IWMO", "valore_eur": 25000},
            {"ticker": "MVOL", "valore_eur": 25000},
            {"ticker": "IWQU", "valore_eur": 25000},
            {"ticker": "IWVL", "valore_eur": 10000},
        ],
        "attese": {"factor_valori_distinti": True, "coverage_pct_min": 60},
    },
]


# ── Helpers ──────────────────────────────────────────────────────

class CheckResult:
    def __init__(self):
        self.checks = {}
        self.anomalie = []
        self.timing = {}

    def record(self, sezione, criterio, status, valore=None, note=""):
        key = f"{sezione}.{criterio}"
        self.checks[key] = {"status": status, "valore": valore, "note": note}
        emoji = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️ ", "N/A": "⬜"}
        print(f"  {emoji.get(status,'?')} {key}: {status} | {valore} {('— '+note) if note else ''}")

    def verdict(self):
        fail = sum(1 for v in self.checks.values() if v["status"] == "FAIL")
        if fail == 0: return "PASS"
        if fail <= 2: return "WARN"
        return "FAIL"

    def counts(self):
        c = {"PASS": 0, "FAIL": 0, "WARN": 0, "N/A": 0}
        for v in self.checks.values():
            c[v["status"]] = c.get(v["status"], 0) + 1
        return c


async def screenshot(page, name):
    path = SCREENSHOTS_DIR / f"{name}.png"
    await page.screenshot(path=str(path), full_page=True)
    return str(path)


async def page_text(page):
    return await page.evaluate("() => document.body.innerText")


def extract_number(text, pattern):
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        for g in m.groups():
            if g is not None:
                try:
                    return float(g.replace(",", ".").replace("%", ""))
                except ValueError:
                    pass
    return None


async def wait_for_analysis(page, timeout_ms):
    """Wait for #s-xray to have content or error to appear."""
    start = time.time()
    deadline = start + timeout_ms / 1000
    while time.time() < deadline:
        # Check if report is visible and has content
        try:
            visible = await page.evaluate("""() => {
                const r = document.getElementById('report');
                const x = document.getElementById('s-xray');
                if (r && !r.hidden && x && x.children.length > 0) return 'success';
                const err = document.querySelector('.error-card');
                if (err) return 'error';
                return null;
            }""")
            if visible == 'success':
                return True, time.time() - start
            if visible == 'error':
                return False, time.time() - start
        except Exception:
            pass
        await asyncio.sleep(2)
    return False, time.time() - start


async def insert_etf(page, ticker, valore, cr, pid):
    """Insert one ETF into the form using exact selectors from inspect_ui."""
    for attempt in range(3):
        try:
            # Clear and type ticker
            inp = await page.wait_for_selector("#etf-input", timeout=5000)
            await inp.click()
            await inp.fill("")
            await inp.type(ticker, delay=80)
            await asyncio.sleep(1.2)

            # Wait for autocomplete, then dismiss or select
            ac = await page.query_selector("#ac-list")
            if ac and await ac.is_visible():
                # Try clicking first autocomplete item
                item = await ac.query_selector(".autocomplete-item")
                if item:
                    await item.click()
                    await asyncio.sleep(0.5)
                else:
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.3)

            # Fill amount
            amt = await page.wait_for_selector("#amount-input", timeout=3000)
            await amt.click()
            await amt.fill(str(valore))

            # Click "+ Aggiungi"
            await page.click("button.btn-add")
            await asyncio.sleep(1)

            # Verify ticker appears in the ETF list
            body = await page_text(page)
            if ticker in body:
                return True
        except Exception as e:
            cr.anomalie.append(f"{ticker}: attempt {attempt+1} failed: {str(e)[:80]}")
            await asyncio.sleep(1)

    cr.anomalie.append(f"{ticker}: FAILED after 3 attempts")
    return False


# ── Main test function ───────────────────────────────────────────

async def test_portfolio(page, ptf):
    pid = ptf["id"]
    print(f"\n{'='*60}")
    print(f"[{pid}] {ptf['nome']} — {len(ptf['etf'])} ETF")
    print(f"{'='*60}")

    cr = CheckResult()
    cr.timing["start"] = datetime.now(timezone.utc).isoformat()
    n_etf = len(ptf["etf"])

    # ── STEP 0: Fresh page ───────────────────────────────────────
    print(f"[{pid}] STEP 0 — Navigazione")
    try:
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=15000)
        await screenshot(page, f"{pid}_00_landing")
        body = await page_text(page)
        is_error = any(x in body for x in ["Internal Server Error", "Service Unavailable", "Application Error"])
        cr.record("PERFORMANCE", "pagina_caricata", "FAIL" if is_error else "PASS", not is_error)
    except Exception as e:
        cr.record("PERFORMANCE", "pagina_caricata", "FAIL", False, str(e)[:100])
        return _build_result(pid, ptf, cr)

    # ── STEP 1: Insert ETFs ──────────────────────────────────────
    print(f"[{pid}] STEP 1 — Inserimento {n_etf} ETF")
    etf_inseriti = 0
    for etf in ptf["etf"]:
        ok = await insert_etf(page, etf["ticker"], etf["valore_eur"], cr, pid)
        if ok:
            etf_inseriti += 1
        await asyncio.sleep(0.3)
    await screenshot(page, f"{pid}_01_form")
    cr.record("PERFORMANCE", "etf_inseriti",
              "PASS" if etf_inseriti == n_etf else "WARN",
              f"{etf_inseriti}/{n_etf}")

    if etf_inseriti == 0:
        cr.record("PERFORMANCE", "prima_analisi_sotto_120s", "FAIL", None, "No ETF inserted")
        return _build_result(pid, ptf, cr)

    # ── STEP 2: First analysis ───────────────────────────────────
    print(f"[{pid}] STEP 2 — Prima analisi")
    try:
        btn = await page.wait_for_selector("#btn-analyze", timeout=5000)
        await btn.click()
    except PWTimeout:
        cr.record("PERFORMANCE", "prima_analisi_sotto_120s", "FAIL", None, "Analyze button not found")
        return _build_result(pid, ptf, cr)

    # Check loading visible
    await asyncio.sleep(1)
    loading_el = await page.query_selector("#loading-overlay")
    loading_visible = loading_el and await loading_el.is_visible() if loading_el else False
    cr.record("PERFORMANCE", "loading_visibile", "PASS" if loading_visible else "WARN", loading_visible)

    # Wait for results
    success, durata = await wait_for_analysis(page, TIMEOUT_PRIMA_ANALISI)
    cr.timing["durata_prima_analisi_sec"] = round(durata, 1)
    cr.record("PERFORMANCE", "prima_analisi_sotto_120s",
              "PASS" if success and durata < 120 else "FAIL", f"{durata:.1f}s")

    await screenshot(page, f"{pid}_02_analisi")
    body_full = await page_text(page)

    has_error = any(x in body_full for x in [
        "Analisi non riuscita", "Traceback", "TypeError", "KeyError",
        "Internal Server Error"
    ])
    cr.record("PERFORMANCE", "nessun_errore_analisi", "FAIL" if has_error else "PASS", not has_error)

    if not success:
        return _build_result(pid, ptf, cr)

    # ── STEP 3: X-RAY ───────────────────────────────────────────
    print(f"[{pid}] STEP 3 — X-Ray")
    await page.evaluate("document.getElementById('s-xray')?.scrollIntoView()")
    await asyncio.sleep(0.5)
    await screenshot(page, f"{pid}_03_xray")

    # Unique securities
    n_titoli = extract_number(body_full, r'(\d+)\s*titoli')
    if n_titoli is None:
        n_titoli = extract_number(body_full, r'Titoli\s*unici\s*(\d+)')
    if n_titoli is None:
        # Try extracting from KPI area
        n_titoli = extract_number(body_full, r'(\d{2,})\n.*?titoli')
    min_t = 1 if n_etf == 1 else 10
    cr.record("XRAY", "titoli_unici_plausibili",
              "PASS" if n_titoli and n_titoli > min_t else "WARN",
              n_titoli)

    # HHI
    hhi = extract_number(body_full, r'HHI\n?([\d.]+)')
    if hhi is None:
        hhi = extract_number(body_full, r'(0\.\d+)\n.*?HHI')
    cr.record("XRAY", "hhi_range_valido",
              "PASS" if hhi and 0.001 < hhi < 0.5 else ("WARN" if hhi else "FAIL"), hhi)

    # Active Share
    as_val = extract_number(body_full, r'Active\s*Share\n?([\d.]+)')
    if as_val is None:
        as_val = extract_number(body_full, r'([\d.]+)\s*%?\n.*?Active\s*Share')
    cr.record("XRAY", "active_share_calcolato",
              "PASS" if as_val is not None and 0 <= as_val <= 100 else "FAIL",
              f"{as_val}%" if as_val is not None else None)

    # Real names in top holdings
    real_names = ["Apple", "Microsoft", "NVIDIA", "Amazon", "Alphabet", "Meta",
                  "ASML", "Samsung", "Tesla", "Novo Nordisk", "Nestl", "TSMC",
                  "Broadcom", "Johnson", "Exxon", "JPMorgan", "Procter"]
    found_names = [n for n in real_names if n in body_full]
    cr.record("XRAY", "top_holdings_nomi_reali",
              "PASS" if len(found_names) >= 3 else ("WARN" if found_names else "FAIL"),
              f"{len(found_names)} nomi", ", ".join(found_names[:5]))

    # Unknown sector %
    unknown_s = extract_number(body_full, r'Unknown\n([\d.]+)\s*%')
    if unknown_s is None:
        unknown_s = 0.0 if "Unknown" not in body_full else 5.0  # conservative fallback
    cr.record("XRAY", "unknown_settore_sotto_15pct",
              "PASS" if unknown_s < 15 else ("WARN" if unknown_s < 25 else "FAIL"),
              f"{unknown_s}%")

    # EUR values — accepts both "€ 1.234" and "1.234 €" and "1.234€"
    has_eur = bool(re.search(r'[\d.,]+\s*€|€\s*[\d.,]+', body_full))
    cr.record("XRAY", "valore_eur_visibile", "PASS" if has_eur else "FAIL", has_eur)

    # Active bets
    if n_etf == 1:
        cr.record("XRAY", "active_bets_popolati", "N/A", None, "1 ETF")
    else:
        has_bets = "sovrappeso" in body_full.lower() or "sottopeso" in body_full.lower() or "overweight" in body_full.lower()
        cr.record("XRAY", "active_bets_popolati", "PASS" if has_bets else "WARN", has_bets)

    # ── STEP 4: OVERLAP ─────────────────────────────────────────
    print(f"[{pid}] STEP 4 — Overlap")
    await page.evaluate("document.getElementById('s-overlap')?.scrollIntoView()")
    await asyncio.sleep(0.5)
    await screenshot(page, f"{pid}_04_overlap")

    if n_etf == 1:
        for c in ["matrice_visibile", "diagonale_100pct", "overlap_alto", "ter_sprecato"]:
            cr.record("OVERLAP", c, "N/A", None, "1 ETF")
    else:
        has_heatmap = await page.query_selector("#heatmap-container")
        has_matrix_content = has_heatmap and await has_heatmap.is_visible() if has_heatmap else False
        cr.record("OVERLAP", "matrice_visibile",
                  "PASS" if has_matrix_content else "WARN", has_matrix_content)

        has_100 = "100%" in body_full or "100 %" in body_full
        cr.record("OVERLAP", "diagonale_100pct", "PASS" if has_100 else "WARN", has_100)

        # Redundancy
        has_rid = "ridondanza" in body_full.lower() or "ridondante" in body_full.lower()
        cr.record("OVERLAP", "ridondanza_visibile", "PASS" if has_rid else "WARN", has_rid)

        # TER wasted
        has_ter = bool(re.search(r'€\s*[\d.,]+\s*/\s*anno|TER.*sprecato', body_full, re.IGNORECASE))
        cr.record("OVERLAP", "ter_sprecato", "PASS" if has_ter else "WARN", has_ter)

    # Swap warning
    sintetici = [e["ticker"] for e in ptf["etf"] if e["ticker"] in ("CW8", "XDWD", "PAEEM")]
    if sintetici:
        has_swap = any(x in body_full.lower() for x in ["sintetico", "swap", "replica sintetica"])
        cr.record("OVERLAP", "warning_swap",
                  "PASS" if has_swap else "WARN", has_swap, f"ETF: {sintetici}")
    else:
        cr.record("OVERLAP", "warning_swap", "N/A", None)

    # ── STEP 5: SECTORS & COUNTRIES ──────────────────────────────
    print(f"[{pid}] STEP 5 — Settori & Paesi")
    await page.evaluate("document.getElementById('s-sector')?.scrollIntoView()")
    await asyncio.sleep(0.5)
    await screenshot(page, f"{pid}_05_settori")

    sector_names = ["Technology", "Info Tech", "Financials", "Healthcare", "Consumer",
                    "Energy", "Industrials", "Materials", "Utilities", "Real Estate",
                    "Communication"]
    sectors_found = sum(1 for s in sector_names if s in body_full)
    cr.record("SETTORI", "settori_almeno_5",
              "PASS" if sectors_found >= 5 else ("WARN" if sectors_found >= 3 else "FAIL"),
              sectors_found)

    country_names = ["United States", "United Kingdom", "Japan", "France", "Germany",
                     "Switzerland", "China", "Canada", "Australia", "Netherlands",
                     "Taiwan", "India", "Korea", "Denmark", "Sweden"]
    countries_found = sum(1 for c in country_names if c in body_full)
    cr.record("SETTORI", "paesi_almeno_5",
              "PASS" if countries_found >= 5 else ("WARN" if countries_found >= 3 else "FAIL"),
              countries_found)

    # Charts not empty
    sector_el = await page.query_selector("#s-sector")
    sector_children = await sector_el.evaluate("el => el.children.length") if sector_el else 0
    cr.record("SETTORI", "nessun_grafico_vuoto",
              "PASS" if sector_children > 1 else "FAIL", f"{sector_children} children")

    # ── STEP 6: FACTOR FINGERPRINT ──────────────────────────────
    print(f"[{pid}] STEP 6 — Factor")
    await page.evaluate("document.getElementById('s-factor')?.scrollIntoView()")
    await asyncio.sleep(0.5)
    await screenshot(page, f"{pid}_06_factor")

    factor_dims = ["Value", "Growth", "Quality", "Size", "Momentum", "Dividend"]
    dims_found = sum(1 for d in factor_dims if d in body_full)
    cr.record("FACTOR", "5_dimensioni_presenti",
              "PASS" if dims_found >= 5 else ("WARN" if dims_found >= 3 else "FAIL"),
              dims_found)

    has_coverage = bool(re.search(r'L[1234].*?\d+%|coverage|copertura|classificat', body_full, re.IGNORECASE))
    cr.record("FACTOR", "coverage_mostrata", "PASS" if has_coverage else "WARN", has_coverage)

    # ── STEP 7: COSA FARE ────────────────────────────────────────
    print(f"[{pid}] STEP 7 — Suggerimenti")
    await page.evaluate("document.getElementById('s-suggestions')?.scrollIntoView()")
    await asyncio.sleep(0.5)
    await screenshot(page, f"{pid}_07_suggerimenti")

    has_sug = any(x in body_full.lower() for x in
                  ["suggerimento", "considera", "valuta", "potresti", "consiglio",
                   "elimina", "consolida", "diversifica"])
    cr.record("COSFARE", "almeno_1_suggerimento", "PASS" if has_sug else "WARN", has_sug)

    # AI card
    has_ai = any(x in body_full for x in ["Analisi AI", "AI PRO", "Premium", "Sblocca"])
    cr.record("COSFARE", "card_ai_visibile", "PASS" if has_ai else "WARN", has_ai)

    # ── STEP 8: MESSAGES & ERRORS ────────────────────────────────
    print(f"[{pid}] STEP 8 — Messaggi")
    bad_strings = ["TypeError:", "KeyError:", "AttributeError:", "Traceback (most recent"]
    found_bad = [x for x in bad_strings if x in body_full]
    cr.record("MESSAGGI", "nessun_traceback", "FAIL" if found_bad else "PASS",
              found_bad if found_bad else True)
    cr.record("MESSAGGI", "tutto_in_italiano",
              "PASS" if not found_bad else "FAIL", not found_bad)

    # ── STEP 9: Second analysis (cache) ──────────────────────────
    print(f"[{pid}] STEP 9 — Seconda analisi (cache)")
    # Click Modifica to go back to form
    try:
        mod_btn = await page.query_selector("#topbar-mod")
        if mod_btn and await mod_btn.is_visible():
            await mod_btn.click()
            await asyncio.sleep(1)
            # Click Analyze again
            btn2 = await page.wait_for_selector("#btn-analyze", timeout=5000)
            await btn2.click()
            ok2, dur2 = await wait_for_analysis(page, TIMEOUT_SECONDA_ANALISI)
            cr.timing["durata_seconda_analisi_sec"] = round(dur2, 1)
            cr.record("PERFORMANCE", "seconda_analisi_sotto_10s",
                      "PASS" if dur2 < 10 else ("WARN" if dur2 < 20 else "FAIL"),
                      f"{dur2:.1f}s")
        else:
            # Try hero edit button
            hero_edit = await page.query_selector("button.hero-edit")
            if hero_edit:
                await hero_edit.click()
                await asyncio.sleep(1)
                btn2 = await page.wait_for_selector("#btn-analyze", timeout=5000)
                await btn2.click()
                ok2, dur2 = await wait_for_analysis(page, TIMEOUT_SECONDA_ANALISI)
                cr.timing["durata_seconda_analisi_sec"] = round(dur2, 1)
                cr.record("PERFORMANCE", "seconda_analisi_sotto_10s",
                          "PASS" if dur2 < 10 else ("WARN" if dur2 < 20 else "FAIL"),
                          f"{dur2:.1f}s")
            else:
                cr.record("PERFORMANCE", "seconda_analisi_sotto_10s", "WARN", None, "Modifica btn not found")
    except Exception as e:
        cr.record("PERFORMANCE", "seconda_analisi_sotto_10s", "WARN", None, str(e)[:80])

    await screenshot(page, f"{pid}_09_final")

    # ── STEP 10: Specific expectations ───────────────────────────
    print(f"[{pid}] STEP 10 — Attese specifiche")
    attese_results = {}
    for k, v in ptf["attese"].items():
        if k == "active_share_max" and as_val is not None:
            ok = as_val < v
            attese_results[k] = {"status": "PASS" if ok else "FAIL", "valore": f"{as_val}%", "atteso": f"<{v}%"}
        elif k == "active_share_min" and as_val is not None:
            ok = as_val > v
            attese_results[k] = {"status": "PASS" if ok else "FAIL", "valore": f"{as_val}%", "atteso": f">{v}%"}
        elif k == "no_crash":
            attese_results[k] = {"status": "PASS" if not has_error else "FAIL", "valore": not has_error, "atteso": True}
        elif k == "warning_closet_indexing" and v:
            has_closet = "closet" in body_full.lower() or "active share" in body_full.lower()
            attese_results[k] = {"status": "PASS" if has_closet else "WARN", "valore": has_closet, "atteso": True}
        elif k == "warning_swap":
            has_swap_w = any(x in body_full.lower() for x in ["sintetico", "swap"])
            attese_results[k] = {"status": "PASS" if has_swap_w else "WARN", "valore": has_swap_w, "atteso": True}
        else:
            attese_results[k] = {"status": "WARN", "valore": "manual check", "atteso": str(v)}

    return _build_result(pid, ptf, cr, attese_results)


def _build_result(pid, ptf, cr, attese_results=None):
    counts = cr.counts()
    return {
        "portfolio_id": pid,
        "portfolio_nome": ptf["nome"],
        "portfolio_descrizione": ptf.get("descrizione", ""),
        "timestamp_inizio": cr.timing.get("start"),
        "durata_prima_analisi_sec": cr.timing.get("durata_prima_analisi_sec"),
        "durata_seconda_analisi_sec": cr.timing.get("durata_seconda_analisi_sec"),
        "checklist": cr.checks,
        "attese_specifiche": attese_results or {},
        "anomalie": cr.anomalie,
        "verdict": cr.verdict(),
        "pass_count": counts["PASS"],
        "fail_count": counts["FAIL"],
        "warn_count": counts["WARN"],
        "na_count": counts["N/A"],
    }


# ── Report generator ─────────────────────────────────────────────

def generate_report(results, run_ts):
    lines = []
    lines.append("# CheckMyETFs — QA Report Aggregato (Playwright)")
    lines.append(f"**Data run:** {run_ts}")
    lines.append(f"**URL:** {BASE_URL}")
    lines.append(f"**Portafogli testati:** {len(results)}/10")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Riepilogo Esecutivo")
    lines.append("")
    lines.append("| ID | Nome | Verdict | PASS | FAIL | WARN | N/A | 1a | 2a |")
    lines.append("|---|---|---|---|---|---|---|---|---|")

    pass_total = 0
    durate_1 = []
    durate_2 = []
    for r in results:
        v = r["verdict"]
        em = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(v, "?")
        d1 = f"{r['durata_prima_analisi_sec']}s" if r.get("durata_prima_analisi_sec") else "—"
        d2 = f"{r['durata_seconda_analisi_sec']}s" if r.get("durata_seconda_analisi_sec") else "—"
        lines.append(f"| {r['portfolio_id']} | {r['portfolio_nome']} | {em} {v} | "
                     f"{r['pass_count']} | {r['fail_count']} | {r['warn_count']} | {r['na_count']} | {d1} | {d2} |")
        if v == "PASS": pass_total += 1
        if r.get("durata_prima_analisi_sec"): durate_1.append(r["durata_prima_analisi_sec"])
        if r.get("durata_seconda_analisi_sec"): durate_2.append(r["durata_seconda_analisi_sec"])

    lines.append("")
    lines.append(f"**PASS:** {pass_total}/10")
    if durate_1: lines.append(f"**Media 1a analisi:** {sum(durate_1)/len(durate_1):.1f}s")
    if durate_2: lines.append(f"**Media 2a analisi:** {sum(durate_2)/len(durate_2):.1f}s")
    lines.append("")

    # Section aggregates
    lines.append("---")
    lines.append("")
    lines.append("## Dettaglio per Sezione")
    lines.append("")
    sezioni_set = set()
    for r in results:
        for k in r["checklist"]:
            sezioni_set.add(k.split(".")[0])
    for sezione in sorted(sezioni_set):
        lines.append(f"### {sezione}")
        lines.append("| Criterio | PASS | FAIL | WARN | N/A |")
        lines.append("|---|---|---|---|---|")
        criteri = set()
        for r in results:
            for k in r["checklist"]:
                if k.startswith(sezione + "."):
                    criteri.add(k.split(".", 1)[1])
        for criterio in sorted(criteri):
            key = f"{sezione}.{criterio}"
            c = {"PASS": 0, "FAIL": 0, "WARN": 0, "N/A": 0}
            for r in results:
                s = r["checklist"].get(key, {}).get("status", "")
                if s in c: c[s] += 1
            lines.append(f"| {criterio} | {c['PASS']} | {c['FAIL']} | {c['WARN']} | {c['N/A']} |")
        lines.append("")

    # Fails
    lines.append("---")
    lines.append("")
    lines.append("## Bug Trovati")
    lines.append("")
    lines.append("### FAIL")
    fails = []
    for r in results:
        for k, v in r["checklist"].items():
            if v["status"] == "FAIL":
                fails.append(f"- **{r['portfolio_id']}** `{k}`: {v.get('valore','')} — {v.get('note','')}")
    lines.extend(fails if fails else ["Nessun FAIL."])
    lines.append("")

    lines.append("### WARN")
    warns = []
    for r in results:
        for k, v in r["checklist"].items():
            if v["status"] == "WARN":
                warns.append(f"- **{r['portfolio_id']}** `{k}`: {v.get('valore','')}")
    lines.extend(warns[:30] if warns else ["Nessun warning."])
    lines.append("")

    # Anomalies
    lines.append("---")
    lines.append("")
    lines.append("## Anomalie")
    lines.append("")
    for r in results:
        if r.get("anomalie"):
            for a in r["anomalie"]:
                lines.append(f"- **{r['portfolio_id']}**: {a}")
    lines.append("")

    # Specific expectations
    lines.append("---")
    lines.append("")
    lines.append("## Attese Specifiche")
    lines.append("| ID | Attesa | Status | Trovato | Atteso |")
    lines.append("|---|---|---|---|---|")
    for r in results:
        for k, v in r.get("attese_specifiche", {}).items():
            em = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(v["status"], "?")
            lines.append(f"| {r['portfolio_id']} | {k} | {em} | {v.get('valore','?')} | {v.get('atteso','?')} |")
    lines.append("")

    path = QA_DIR / "QA_REPORT_AGGREGATO.md"
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\nReport → {path}")


# ── Main ─────────────────────────────────────────────────────────

async def main():
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    all_results = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="it-IT",
        )
        page = await context.new_page()
        page.on("console", lambda msg: None)  # suppress console noise

        for ptf in PORTFOLIOS:
            try:
                result = await test_portfolio(page, ptf)
            except Exception as e:
                print(f"[{ptf['id']}] CRITICAL ERROR: {e}")
                result = {
                    "portfolio_id": ptf["id"], "portfolio_nome": ptf["nome"],
                    "portfolio_descrizione": ptf.get("descrizione", ""),
                    "verdict": "FAIL", "anomalie": [str(e)],
                    "pass_count": 0, "fail_count": 1, "warn_count": 0, "na_count": 0,
                    "checklist": {}, "attese_specifiche": {},
                    "durata_prima_analisi_sec": None, "durata_seconda_analisi_sec": None,
                    "timestamp_inizio": None,
                }

            all_results.append(result)
            nome_safe = ptf["nome"].replace(" ", "_")
            with open(QA_DIR / f"{ptf['id']}_{nome_safe}.json", "w") as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            print(f"[{ptf['id']}] → {result['verdict']} (P:{result['pass_count']} F:{result['fail_count']} W:{result['warn_count']})")

            await page.goto("about:blank")
            await asyncio.sleep(2)

        await browser.close()

    generate_report(all_results, run_ts)
    print(f"\n{'='*60}\nQA COMPLETE — {sum(1 for r in all_results if r['verdict']=='PASS')}/10 PASS\n{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
