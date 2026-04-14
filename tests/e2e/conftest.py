"""Shared fixtures and helpers for CheckMyETFs E2E tests."""

import time

import pytest

BASE_URL = "https://www.checkmyetfs.com"
WHITELIST_EMAIL = "lorenzo.casalini90@gmail.com"
ANALYSIS_TIMEOUT = 90_000  # 90s max for analysis


@pytest.fixture(scope="session")
def browser_context_args():
    return {"viewport": {"width": 1280, "height": 800}}


@pytest.fixture()
def page(browser):
    """Desktop page — fresh context per test (no localStorage bleed)."""
    ctx = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = ctx.new_page()
    yield pg
    pg.close()
    ctx.close()


@pytest.fixture()
def mobile_page(browser):
    """Mobile viewport 390x844."""
    ctx = browser.new_context(viewport={"width": 390, "height": 844})
    pg = ctx.new_page()
    yield pg
    pg.close()
    ctx.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def add_etf(page, ticker: str, amount: int) -> float:
    """Type ticker, set amount, click add. Returns elapsed ms."""
    t0 = time.time()
    inp = page.locator("#etf-input")
    inp.fill(ticker)
    page.locator("#amount-input").fill(str(amount))
    page.locator(".btn-add").click()
    # Wait for the ETF row to appear
    page.locator(f".etf-ticker:text-is('{ticker.upper()}')").wait_for(timeout=10_000)
    return (time.time() - t0) * 1000


def run_analysis(page, timeout: int = ANALYSIS_TIMEOUT) -> dict:
    """Click analyze, wait for report. Returns dict with timing info."""
    t0 = time.time()
    page.locator("#btn-analyze").click()
    # Wait for either report visible OR error card
    try:
        page.wait_for_function(
            """() => {
                const report = document.getElementById('report');
                const error = document.querySelector('.error-card');
                return (report && !report.hidden) || error;
            }""",
            timeout=timeout,
        )
    except Exception:
        elapsed = (time.time() - t0) * 1000
        return {"duration_ms": elapsed, "success": False,
                "error_msg": f"TIMEOUT: analisi non completata entro {timeout}ms"}
    elapsed = (time.time() - t0) * 1000

    # Check if report is visible or error shown
    report_visible = page.evaluate("!document.getElementById('report').hidden")
    if report_visible:
        return {"duration_ms": elapsed, "success": True, "error_msg": None}
    err_el = page.locator(".error-card p")
    err_msg = err_el.text_content() if err_el.count() > 0 else "Unknown error"
    return {"duration_ms": elapsed, "success": False, "error_msg": err_msg}


def measure(label: str, fn):
    """Execute fn, measure time, log it, return result."""
    t0 = time.time()
    result = fn()
    elapsed = (time.time() - t0) * 1000
    print(f"[TIMING] {label}: {elapsed:.0f}ms")
    return result


def take_screenshot(page, name: str):
    """Save screenshot on failure."""
    path = f"tests/e2e/screenshots/{name}.png"
    page.screenshot(path=path, full_page=True)
    return path


def goto_clean(page):
    """Navigate to BASE_URL with clean state."""
    page.goto(BASE_URL, wait_until="networkidle")
    # Clear localStorage to avoid pre-loaded portfolios
    page.evaluate("localStorage.removeItem('cmf_portfolio')")
    page.reload(wait_until="networkidle")
    page.wait_for_selector("#etf-input", timeout=10_000)
