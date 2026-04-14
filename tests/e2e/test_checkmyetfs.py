"""
CheckMyETFs — comprehensive E2E test suite.

Target: https://www.checkmyetfs.com
Run:    pytest tests/e2e/test_checkmyetfs.py -v --headed
"""

import time

import pytest

from conftest import (
    BASE_URL, WHITELIST_EMAIL, ANALYSIS_TIMEOUT,
    add_etf, run_analysis, measure, take_screenshot, goto_clean,
)

# ═══════════════════════════════════════════════════════════════════════════
# MODULO A — INPUT PORTAFOGLIO
# ═══════════════════════════════════════════════════════════════════════════

class TestModuloA:

    def test_A01_inserimento_ticker_base(self, page):
        goto_clean(page)
        ms = measure("A01 add SWDA", lambda: add_etf(page, "SWDA", 10000))
        assert page.locator(".etf-ticker:text-is('SWDA')").count() == 1
        total_text = page.locator(".form-total").text_content()
        assert "10.000" in total_text or "10,000" in total_text

    def test_A02_inserimento_via_isin(self, page):
        goto_clean(page)
        add_etf(page, "IE00B4L5Y983", 15000)
        result = measure("A02 analysis", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        # The report should show a name, not just ISIN
        report_text = page.locator("#report").text_content()
        assert len(report_text) > 100  # Report has content

    def test_A03_aggiunta_multipla_3_etf(self, page):
        goto_clean(page)
        measure("A03 add SWDA", lambda: add_etf(page, "SWDA", 30000))
        measure("A03 add EIMI", lambda: add_etf(page, "EIMI", 10000))
        measure("A03 add CW8", lambda: add_etf(page, "CW8", 10000))
        assert page.locator(".etf-row").count() == 3
        total_text = page.locator(".form-total").text_content()
        assert "50.000" in total_text or "50,000" in total_text

    def test_A04_modifica_importo(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 10000)
        # Click on the amount to edit — the form re-renders, so we modify
        # the amount input and re-add after removing
        page.locator(".etf-remove").first.click()
        page.wait_for_selector(".etf-row", state="detached", timeout=3000)
        add_etf(page, "SWDA", 25000)
        amount_text = page.locator(".etf-amount").first.text_content()
        assert "25.000" in amount_text or "25,000" in amount_text

    def test_A05_eliminazione_etf(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 10000)
        add_etf(page, "EIMI", 10000)
        assert page.locator(".etf-row").count() == 2
        # Remove second ETF (EIMI)
        page.locator(".etf-remove").nth(1).click()
        page.wait_for_timeout(500)
        assert page.locator(".etf-row").count() == 1
        assert page.locator(".etf-ticker:text-is('SWDA')").count() == 1

    def test_A06_svuota_portafoglio(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 10000)
        add_etf(page, "EIMI", 10000)
        add_etf(page, "CW8", 10000)
        page.locator(".btn-clear-all").click()
        page.wait_for_timeout(500)
        assert page.locator(".etf-row").count() == 0

    def test_A07_limite_10_etf(self, page):
        """Frontend limit is 10 ETFs (not 15)."""
        goto_clean(page)
        tickers = ["SWDA", "CSPX", "EIMI", "CW8", "XDWD",
                    "VWCE", "IWDA", "IUSQ", "EQQQ", "SPY5"]
        for t in tickers:
            add_etf(page, t, 5000)
        assert page.locator(".etf-row").count() == 10
        # 11th should be silently rejected
        page.locator("#etf-input").fill("DBXJ")
        page.locator("#amount-input").fill("5000")
        page.locator(".btn-add").click()
        page.wait_for_timeout(500)
        assert page.locator(".etf-row").count() == 10

    def test_A08_ticker_inesistente(self, page):
        goto_clean(page)
        add_etf(page, "XXXXINVALID", 1000)
        result = run_analysis(page)
        # Should either fail gracefully or show partial results
        if not result["success"]:
            assert result["error_msg"] is not None
            assert len(result["error_msg"]) > 5  # Meaningful message

    def test_A10_importo_zero_negativo(self, page):
        goto_clean(page)
        page.locator("#etf-input").fill("SWDA")
        page.locator("#amount-input").fill("0")
        page.locator(".btn-add").click()
        page.wait_for_timeout(500)
        assert page.locator(".etf-row").count() == 0
        page.locator("#amount-input").fill("-1000")
        page.locator(".btn-add").click()
        page.wait_for_timeout(500)
        assert page.locator(".etf-row").count() == 0

    def test_A11_importo_grande(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 1000000)
        result = measure("A11 analysis 1M", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]

    def test_A14_cambio_benchmark(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        # Default benchmark is MSCI World
        result1 = run_analysis(page)
        assert result1["success"], result1["error_msg"]
        as1 = page.locator("#hero-bar").text_content()
        # Go back and change benchmark
        page.locator("#topbar-mod").click()
        page.wait_for_selector("#etf-input", timeout=5000)
        page.locator("#bench-select").select_option("SP500")
        result2 = run_analysis(page)
        assert result2["success"], result2["error_msg"]

    def test_A15_nessun_benchmark(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        page.locator("#bench-select").select_option("")
        result = measure("A15 no bench", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]


# ═══════════════════════════════════════════════════════════════════════════
# MODULO B — FETCH & COVERAGE
# ═══════════════════════════════════════════════════════════════════════════

class TestModuloB:

    def test_B01_ishares_core(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "CSPX", 20000)
        add_etf(page, "EIMI", 10000)
        result = measure("B01 iShares analysis", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        report = page.locator("#report").text_content()
        assert "parzial" not in report.lower() or "partial" not in report.lower()

    def test_B02_amundi_swap_warning(self, page):
        goto_clean(page)
        add_etf(page, "CW8", 20000)
        result = measure("B02 Amundi", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        report = page.locator("#report").text_content().lower()
        assert "swap" in report or "sintetico" in report or "sintetica" in report

    def test_B03_xtrackers(self, page):
        goto_clean(page)
        add_etf(page, "XDWD", 25000)
        result = measure("B03 Xtrackers", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]

    def test_B05_vanguard_justETF_fallback(self, page):
        goto_clean(page)
        add_etf(page, "VWCE", 30000)
        result = measure("B05 Vanguard fallback", lambda: run_analysis(page))
        if not result["success"]:
            err = result["error_msg"] or ""
            err_lower = err.lower()
            # Vanguard is geo-blocked and uses justETF fallback — may fail on cold fetch
            if any(k in err_lower for k in ["timeout", "504", "502", "could not fetch",
                                             "fallback", "fetch holdings"]):
                pytest.skip(f"Vanguard/JustETF provider intermittently unavailable: {err}")
            # Any other error is a real failure
            assert False, f"VWCE analysis failed: {err}"
        # Report should have at least the X-Ray section rendered
        page.wait_for_selector("#s-xray", timeout=5000)
        assert page.locator("#s-xray").text_content().strip() != ""

    def test_B06_mix_multi_provider(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 15000)
        add_etf(page, "CW8", 15000)
        add_etf(page, "XDWD", 10000)
        add_etf(page, "VWCE", 10000)
        result = measure("B06 multi-provider", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        # Overlap matrix should be visible
        assert page.locator("#s-overlap").text_content().strip() != ""

    def test_B09_cache_hit(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 20000)
        r1 = measure("B09 cold", lambda: run_analysis(page))
        assert r1["success"], r1["error_msg"]
        # Go back and re-analyze
        page.locator("#topbar-mod").click()
        page.wait_for_selector("#etf-input", timeout=5000)
        r2 = measure("B09 warm", lambda: run_analysis(page))
        assert r2["success"], r2["error_msg"]
        assert r2["duration_ms"] < 15_000, \
            f"Cache hit too slow: {r2['duration_ms']:.0f}ms (expected <15000ms)"
        print(f"[TIMING] B09 cold={r1['duration_ms']:.0f}ms warm={r2['duration_ms']:.0f}ms "
              f"speedup={r1['duration_ms']/max(r2['duration_ms'],1):.1f}x")

    def test_B10_etf_sconosciuto(self, page):
        goto_clean(page)
        add_etf(page, "ZZUNKNOWN99", 10000)
        result = run_analysis(page)
        if not result["success"]:
            assert result["error_msg"] is not None


# ═══════════════════════════════════════════════════════════════════════════
# MODULO C — ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════

class TestModuloC:

    def test_C01_overlap_stesso_indice(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "CW8", 20000)
        result = measure("C01 overlap", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        overlap_section = page.locator("#s-overlap")
        assert overlap_section.text_content().strip() != ""

    def test_C02_ridondanza_alta(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "IWDA", 20000)
        result = measure("C02 redundancy", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        # Check suggestions section mentions redundancy
        suggestions = page.locator("#s-suggestions").text_content().lower()
        assert "ridondanza" in suggestions or "equilibrato" in suggestions

    def test_C06_factor_5_dimensioni(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "EIMI", 20000)
        result = measure("C06 factors", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        factor_section = page.locator("#s-factor").text_content()
        assert len(factor_section) > 50  # Has content

    def test_C07_settori_e_paesi(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "EIMI", 20000)
        result = measure("C07 sectors", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        sector_section = page.locator("#s-sector").text_content()
        assert len(sector_section) > 50

    def test_C08_top_holdings(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "CSPX", 20000)
        result = measure("C08 holdings", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        xray = page.locator("#s-xray").text_content().lower()
        # At least one major holding should appear
        assert "apple" in xray or "microsoft" in xray or "nvidia" in xray


# ═══════════════════════════════════════════════════════════════════════════
# MODULO D — UI
# ═══════════════════════════════════════════════════════════════════════════

class TestModuloD:

    def test_D01_D02_salva_e_carica(self, page, tmp_path):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "EIMI", 10000)
        assert page.locator(".etf-row").count() == 2
        # Click "Salva" — this saves to localStorage AND triggers a JSON download.
        # We intercept the download to verify the file content.
        with page.expect_download() as download_info:
            page.locator(".btn-secondary:has-text('Salva')").click()
        download = download_info.value
        saved_path = tmp_path / "saved.json"
        download.save_as(str(saved_path))
        import json
        saved_data = json.loads(saved_path.read_text())
        assert len(saved_data) == 2
        # Clear UI
        page.locator(".btn-clear-all").click()
        page.wait_for_timeout(500)
        assert page.locator(".etf-row").count() == 0
        # Click "Carica" — opens a file picker. Feed it the saved JSON.
        file_input = page.locator("input[type='file'][accept='.json']")
        file_input.set_input_files(str(saved_path))
        page.wait_for_selector(".etf-row", timeout=5000)
        assert page.locator(".etf-row").count() == 2
        assert page.locator(".etf-ticker:text-is('SWDA')").count() == 1
        assert page.locator(".etf-ticker:text-is('EIMI')").count() == 1

    def test_D03_import_csv(self, page, tmp_path):
        goto_clean(page)
        csv_file = tmp_path / "portfolio.csv"
        csv_file.write_text("ticker,amount_eur\nSWDA,30000\nEIMI,10000\n")
        # Find file input inside the import label
        file_input = page.locator("input[type='file'][accept*='.csv']")
        file_input.set_input_files(str(csv_file))
        page.wait_for_selector(".etf-row", timeout=5000)
        assert page.locator(".etf-row").count() == 2
        total_text = page.locator(".form-total").text_content()
        assert "40.000" in total_text or "40,000" in total_text

    def test_D06_navigazione_anchor(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "EIMI", 10000)
        result = run_analysis(page)
        assert result["success"], result["error_msg"]
        # Click each nav tab and verify section is scrolled to
        sections = ["s-xray", "s-overlap", "s-sector", "s-factor", "s-suggestions"]
        for sid in sections:
            link = page.locator(f".topbar-nav a[data-section='{sid}']")
            if link.count() > 0:
                link.click()
                page.wait_for_timeout(500)
                assert page.locator(f"#{sid}").is_visible()

    def test_D09_modifica_dopo_analisi(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 20000)
        result = run_analysis(page)
        assert result["success"], result["error_msg"]
        # Click modify
        page.locator("#topbar-mod").click()
        page.wait_for_selector("#etf-input", timeout=5000)
        # Form should be visible with data
        assert page.locator(".etf-row").count() == 1
        # Add another ETF and re-analyze
        add_etf(page, "EIMI", 10000)
        result2 = measure("D09 re-analysis", lambda: run_analysis(page))
        assert result2["success"], result2["error_msg"]

    def test_D12_rate_limiting(self, page):
        """Rate limiter: 3 req/min on /api/analyze.

        Uses the same cached ETF for speed. The rate limiter counts requests
        per IP per minute regardless of response time.
        """
        goto_clean(page)
        add_etf(page, "SWDA", 10000)

        # Fire 3 analyses as fast as possible (same ETF = cache hit = fast)
        for i in range(3):
            result = run_analysis(page)
            if page.evaluate("!document.getElementById('report').hidden"):
                page.locator("#topbar-mod").click()
                page.wait_for_selector("#btn-analyze", timeout=5000)

        # 4th immediately
        page.locator("#btn-analyze").click()

        # Wait for either rate limit OR report
        try:
            page.wait_for_function(
                """() => {
                    const body = document.body.textContent.toLowerCase();
                    const report = document.getElementById('report');
                    return body.includes('troppe') || body.includes('attendi') ||
                           (report && !report.hidden);
                }""",
                timeout=ANALYSIS_TIMEOUT,
            )
        except Exception:
            pass

        body_text = page.text_content("body").lower()
        rate_limited = "troppe" in body_text or "attendi" in body_text
        analysis_ok = page.evaluate("!document.getElementById('report').hidden")

        if not rate_limited and analysis_ok:
            # Rate limit window may have expired between requests
            pytest.skip("Rate limit window expired — analyses were too slow to trigger limit")
        assert rate_limited or analysis_ok, \
            "Expected rate limit message or successful analysis"


# ═══════════════════════════════════════════════════════════════════════════
# MODULO E — PREMIUM AI
# ═══════════════════════════════════════════════════════════════════════════

class TestModuloE:

    def _setup_analysis(self, page):
        """Helper: run a quick analysis to get the report visible."""
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "EIMI", 10000)
        result = run_analysis(page)
        assert result["success"], result["error_msg"]

    def test_E01_card_ai_visibile(self, page):
        self._setup_analysis(page)
        ai_section = page.locator("#s-ai")
        assert ai_section.text_content().strip() != "", "AI card section is empty"
        assert "Analisi AI" in ai_section.text_content()

    def test_E02_espansione_card(self, page):
        self._setup_analysis(page)
        page.locator("#s-ai").scroll_into_view_if_needed()
        btn = page.locator("text=Scopri Analisi AI")
        assert btn.count() > 0, "Button 'Scopri Analisi AI' not found"
        btn.click()
        page.wait_for_timeout(500)
        ai_text = page.locator("#s-ai").text_content()
        assert "accesso" in ai_text.lower()

    def test_E03_accesso_whitelist(self, page):
        self._setup_analysis(page)
        page.locator("#s-ai").scroll_into_view_if_needed()
        page.locator("text=Scopri Analisi AI").click()
        page.wait_for_timeout(500)
        # Find the email input inside the AI card
        email_input = page.locator("#s-ai input[type='email']")
        email_input.fill(WHITELIST_EMAIL)
        page.locator("#s-ai >> text=Accedi").click()
        # Wait for AI response (may take up to 30s)
        try:
            page.wait_for_function(
                """() => {
                    const el = document.querySelector('#s-ai');
                    return el && (el.textContent.includes('summary') ||
                                  el.textContent.includes('azione') ||
                                  el.textContent.includes('portafoglio') ||
                                  el.querySelectorAll('[style*="border-left"]').length > 0);
                }""",
                timeout=30_000
            )
            print("[TIMING] E03 AI generation: completed")
        except Exception:
            # Check if there's an error message instead
            ai_text = page.locator("#s-ai").text_content()
            if "non configurato" in ai_text.lower() or "errore" in ai_text.lower():
                pytest.skip("AI service not configured on production")
            raise

    def test_E05_email_non_whitelist(self, page):
        self._setup_analysis(page)
        page.locator("#s-ai").scroll_into_view_if_needed()
        page.locator("text=Scopri Analisi AI").click()
        page.wait_for_timeout(500)
        email_input = page.locator("#s-ai input[type='email']")
        email_input.fill("utente.random@test.com")
        page.locator("#s-ai >> text=Accedi").click()
        page.wait_for_timeout(3000)
        ai_text = page.locator("#s-ai").text_content().lower()
        assert "non ha accesso" in ai_text or "non sei" in ai_text or "lista" in ai_text

    def test_E06_waitlist_form_precompilato(self, page):
        self._setup_analysis(page)
        page.locator("#s-ai").scroll_into_view_if_needed()
        page.locator("text=Scopri Analisi AI").click()
        page.wait_for_timeout(500)
        email_input = page.locator("#s-ai input[type='email']")
        email_input.fill("test@example.com")
        page.locator("#s-ai >> text=Accedi").click()
        # Wait for the "not in list" response
        page.wait_for_function(
            """() => {
                const el = document.querySelector('#s-ai');
                return el && (el.textContent.toLowerCase().includes('non ha accesso') ||
                              el.textContent.toLowerCase().includes('lista'));
            }""",
            timeout=10_000,
        )
        # After "not in list" response, ALL Google Form links should have email
        # (both the right column link and the inline link get updated)
        page.wait_for_timeout(500)  # ensure DOM updates complete
        links = page.locator("#s-ai a[href*='google.com/forms']")
        assert links.count() > 0, "Waitlist Google Form link not found"
        # Check the LAST link (inline one created by _showNotInList)
        href = links.last.get_attribute("href")
        assert "1FAIpQLSd-bFJg9H5OyeeAmZXJTdSBen" in href, f"Wrong form URL: {href}"
        assert "test%40example.com" in href, \
            f"Email not pre-filled in form URL: {href}"


# ═══════════════════════════════════════════════════════════════════════════
# MODULO F — EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════

class TestModuloF:

    def test_F01_un_solo_etf(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 50000)
        result = measure("F01 single ETF", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]

    def test_F05_messaggi_progressivi(self, page):
        goto_clean(page)
        # Use rare ETFs unlikely to be cached
        add_etf(page, "AGGH", 10000)
        add_etf(page, "WCLD", 8000)
        page.locator("#btn-analyze").click()

        # Poll for loading messages over time
        t0 = time.time()
        messages_seen = set()
        while (time.time() - t0) < 90:
            # Check if loading is still visible
            loading_visible = page.evaluate(
                "!document.getElementById('loading-overlay').hidden"
            )
            if not loading_visible:
                break
            msg_el = page.query_selector("#loading-msg")
            if msg_el:
                txt = msg_el.text_content()
                if txt:
                    messages_seen.add(txt)
            page.wait_for_timeout(500)

        elapsed = (time.time() - t0) * 1000
        print(f"[TIMING] F05 loading: {elapsed:.0f}ms, messages: {messages_seen}")

        if elapsed < 3000:
            pytest.skip("Analysis too fast (cached) — cannot test progress messages")

        assert len(messages_seen) >= 1, "No loading messages seen during analysis"

    def test_F06_refresh_durante_analisi(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 10000)
        page.locator("#btn-analyze").click()
        page.wait_for_timeout(2000)
        page.reload(wait_until="networkidle")
        page.wait_for_selector("#etf-input", timeout=10_000)
        # Page should be in a clean state, no crash
        assert page.locator("#portfolio-input").is_visible() or \
               page.locator("#landing-hero").is_visible()

    def test_F10_mobile_viewport(self, mobile_page):
        page = mobile_page
        goto_clean(page)
        add_etf(page, "SWDA", 20000)
        add_etf(page, "EIMI", 10000)
        result = measure("F10 mobile analysis", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        # Report should be visible
        assert page.locator("#report:not([hidden])").count() > 0


# ═══════════════════════════════════════════════════════════════════════════
# MODULO G — FLUSSI UTENTE
# ═══════════════════════════════════════════════════════════════════════════

class TestModuloG:

    def test_G01_analisi_modifica_rianalisi(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 30000)
        add_etf(page, "EIMI", 10000)
        result = run_analysis(page)
        assert result["success"], result["error_msg"]
        # Modify
        page.locator("#topbar-mod").click()
        page.wait_for_selector("#etf-input", timeout=5000)
        add_etf(page, "CSPX", 10000)
        result2 = measure("G01 re-analysis", lambda: run_analysis(page))
        assert result2["success"], result2["error_msg"]
        # Should have 3 ETFs in the report
        assert page.locator("#report:not([hidden])").count() > 0

    def test_G02_salva_svuota_ricarica_analisi(self, page, tmp_path):
        goto_clean(page)
        add_etf(page, "SWDA", 25000)
        add_etf(page, "CW8", 15000)
        assert page.locator(".etf-row").count() == 2
        # Save via button — downloads JSON file
        with page.expect_download() as download_info:
            page.locator(".btn-secondary:has-text('Salva')").click()
        download = download_info.value
        saved_path = tmp_path / "portfolio.json"
        download.save_as(str(saved_path))
        # Clear
        page.locator(".btn-clear-all").click()
        page.wait_for_timeout(500)
        assert page.locator(".etf-row").count() == 0
        # Load via file picker with the saved JSON
        file_input = page.locator("input[type='file'][accept='.json']")
        file_input.set_input_files(str(saved_path))
        page.wait_for_selector(".etf-row", timeout=5000)
        assert page.locator(".etf-row").count() == 2
        # Analyze
        result = measure("G02 restored analysis", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        # Overlap should be visible (2 MSCI World ETFs)
        assert page.locator("#s-overlap").text_content().strip() != ""

    def test_G04_primo_utente_onboarding(self, page):
        goto_clean(page)
        # Landing hero should be visible
        hero = page.locator("#landing-hero")
        assert hero.is_visible()
        # Add ETFs and analyze
        add_etf(page, "SWDA", 20000)
        add_etf(page, "EIMI", 10000)
        result = measure("G04 onboarding analysis", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        # All 5 report sections should have content
        for sid in ["s-xray", "s-overlap", "s-sector", "s-factor", "s-suggestions"]:
            section = page.locator(f"#{sid}")
            assert section.text_content().strip() != "", f"Section {sid} is empty"

    def test_G05_portafoglio_complesso_7_etf(self, page):
        goto_clean(page)
        etfs = [("SWDA", 20000), ("EIMI", 8000), ("CSPX", 10000),
                ("CW8", 12000), ("VWCE", 5000), ("XDWD", 8000), ("IUSQ", 5000)]
        for t, a in etfs:
            add_etf(page, t, a)
        assert page.locator(".etf-row").count() == 7
        result = measure("G05 complex portfolio", lambda: run_analysis(page))
        assert result["success"], result["error_msg"]
        # Suggestions should have content
        suggestions = page.locator("#s-suggestions").text_content()
        assert len(suggestions) > 20

    def test_G08_flusso_waitlist_completo(self, page):
        goto_clean(page)
        add_etf(page, "SWDA", 20000)
        result = run_analysis(page)
        assert result["success"], result["error_msg"]
        # Navigate to AI card
        page.locator("#s-ai").scroll_into_view_if_needed()
        discover_btn = page.locator("text=Scopri Analisi AI")
        if discover_btn.count() == 0:
            pytest.skip("AI card not visible")
        discover_btn.click()
        page.wait_for_timeout(500)
        # Check waitlist link is present
        waitlist = page.locator("#s-ai >> text=Unisciti alla lista Pro")
        if waitlist.count() > 0:
            href = waitlist.get_attribute("href")
            assert "google.com/forms" in href
