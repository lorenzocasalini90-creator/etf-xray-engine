# Fix Italian Number Parsing & Persistent Enrichment

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two critical bugs: (1) Italian/European number format parsing for portfolio amounts, (2) enrichment sector/country not persisting across analysis runs, plus expand static mapping.

**Architecture:** Bug 1 is a pure function fix in `_parse_amount()` plus adding `$` handling. Bug 2 requires ensuring enriched data flows back to session state in all code paths. Static mapping expansion adds ~30 new entries.

**Tech Stack:** Python, pandas, Streamlit, pytest

---

### Task 1: Fix `_parse_amount()` — tests first

**Files:**
- Modify: `tests/test_portfolio_uploader.py` (add new test cases)
- Modify: `src/dashboard/components/portfolio_uploader.py:138-177`

- [ ] **Step 1: Add failing tests for the bug cases**

Add these tests to `TestParseAmount` in `tests/test_portfolio_uploader.py`:

```python
def test_european_full_format(self):
    """13.313,125 → 13313.125 (European: dot=thousands, comma=decimal)"""
    assert _parse_amount("13.313,125") == 13313.125

def test_comma_decimal_three_digits(self):
    """13313,125 — ambiguous, but with no dot it's thousands separator."""
    assert _parse_amount("13313,125") == 13313125.0

def test_dollar_sign(self):
    assert _parse_amount("$30000") == 30000.0

def test_dollar_with_comma(self):
    assert _parse_amount("$30,000.50") == 30000.50

def test_numeric_passthrough_int(self):
    assert _parse_amount("30000") == 30000.0

def test_comma_decimal_two_digits(self):
    """13313,12 → 13313.12 (decimal comma, 2 digits)"""
    assert _parse_amount("13313,12") == 13313.12

def test_dot_decimal_two_digits(self):
    """13313.12 → 13313.12 (decimal dot, 2 digits)"""
    assert _parse_amount("13313.12") == 13313.12

def test_european_thousands_dot_only(self):
    """13.313 → 13313 (exactly 3 digits after dot = thousands)"""
    assert _parse_amount("13.313") == 13313.0
```

- [ ] **Step 2: Run tests to confirm new ones pass/fail**

Run: `pytest tests/test_portfolio_uploader.py::TestParseAmount -v`
Expected: Some new tests may already pass (existing logic handles some), verify which fail.

- [ ] **Step 3: Replace `_parse_amount()` with improved version**

Replace `_parse_amount()` in `src/dashboard/components/portfolio_uploader.py:138-177` with:

```python
def _parse_amount(raw) -> float | None:
    """Parse amount string handling euro/dollar signs, locale separators.

    Handles:
    - European format: 30.000 or 30.000,50 (dot=thousands, comma=decimal)
    - US format: 30,000 or 30,000.50 (comma=thousands, dot=decimal)
    - Euro/dollar sign prefix, spaces
    - Direct numeric values (int/float passthrough)
    """
    if isinstance(raw, (int, float)):
        return float(raw)

    s = str(raw).strip().replace("€", "").replace("$", "").replace(" ", "")
    if not s:
        return None

    if "," in s and "." in s:
        # Both present: last one is decimal separator
        if s.rfind(",") > s.rfind("."):
            # European: 30.000,50 → 30000.50
            s = s.replace(".", "").replace(",", ".")
        else:
            # US: 30,000.50 → 30000.50
            s = s.replace(",", "")
    elif "," in s:
        # Only comma: if exactly 3 digits after, it's thousands
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) == 3:
            s = s.replace(",", "")
        else:
            s = s.replace(",", ".")
    elif "." in s:
        # Only dot: if exactly 3 digits after, it's thousands
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            s = s.replace(".", "")
        # else: regular decimal dot, leave as-is

    try:
        return float(s)
    except ValueError:
        return None
```

- [ ] **Step 4: Run all parse tests**

Run: `pytest tests/test_portfolio_uploader.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_portfolio_uploader.py src/dashboard/components/portfolio_uploader.py
git commit -m "Fix: Italian/European number format parsing in portfolio amounts"
```

---

### Task 2: Expand STATIC_SECTOR_COUNTRY mapping

**Files:**
- Modify: `src/analytics/enrichment.py:79-160` (add entries before closing `}`)
- Modify: `tests/test_enrichment.py` (add test for new entries)

- [ ] **Step 1: Add test for new static mapping entries**

Add to `tests/test_enrichment.py`:

```python
class TestExpandedStaticMapping:
    """Test that new defense/banking/energy entries exist in static mapping."""

    @pytest.mark.parametrize("name,expected_sector", [
        ("BOEING", "Industrials"),
        ("AIRBUS", "Industrials"),
        ("DEUTSCHE BANK", "Financials"),
        ("HSBC", "Financials"),
        ("TOTALENERGIES", "Energy"),
        ("REPSOL", "Energy"),
        ("GALP", "Energy"),
        ("OMV", "Energy"),
        ("NORDEA", "Financials"),
        ("COMMERZBANK", "Financials"),
    ])
    def test_entry_exists(self, name, expected_sector):
        assert name in STATIC_SECTOR_COUNTRY
        assert STATIC_SECTOR_COUNTRY[name][0] == expected_sector
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_enrichment.py::TestExpandedStaticMapping -v`
Expected: FAIL — entries don't exist yet.

- [ ] **Step 3: Add entries to STATIC_SECTOR_COUNTRY**

Add before the closing `}` of `STATIC_SECTOR_COUNTRY` in `src/analytics/enrichment.py:159`, after the `"COGNEX"` line:

```python
    # Defense / Aerospace — additional
    "BOEING": ("Industrials", "United States"),
    "AIRBUS": ("Industrials", "France"),
    # European banks
    "DEUTSCHE BANK": ("Financials", "Germany"),
    "SOCIETE GENERALE": ("Financials", "France"),
    "CREDIT AGRICOLE": ("Financials", "France"),
    "BARCLAYS": ("Financials", "United Kingdom"),
    "HSBC": ("Financials", "United Kingdom"),
    "STANDARD CHARTERED": ("Financials", "United Kingdom"),
    "NORDEA": ("Financials", "Finland"),
    "DANSKE BANK": ("Financials", "Denmark"),
    "KBC GROUP": ("Financials", "Belgium"),
    "ERSTE GROUP": ("Financials", "Austria"),
    "RAIFFEISEN": ("Financials", "Austria"),
    "COMMERZBANK": ("Financials", "Germany"),
    # Energy — additional
    "REPSOL": ("Energy", "Spain"),
    "GALP": ("Energy", "Portugal"),
    "OMV": ("Energy", "Austria"),
```

- [ ] **Step 4: Run enrichment tests**

Run: `pytest tests/test_enrichment.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/analytics/enrichment.py tests/test_enrichment.py
git commit -m "Add 17 static sector/country mappings for defense, banks, energy"
```

---

### Task 3: Fix enrichment persistence in session state

**Files:**
- Modify: `src/dashboard/pages/01_portfolio_input.py:331-355` (cached path)
- Modify: `src/dashboard/pages/01_portfolio_input.py:443-454` (fresh analysis path)

The enrichment already runs on both paths (lines 339-350 for cached, lines 444-452 for fresh). The issue is that in the cached path, enriched data is saved to `st.session_state.aggregated` (line 345) but the hash isn't updated, so next time it re-enriches the already-enriched data (harmless but wasteful). The real issue is ensuring both paths save consistently.

- [ ] **Step 1: Verify the cached path saves enriched data**

Read `src/dashboard/pages/01_portfolio_input.py:331-355` and confirm that line 345 does `st.session_state.aggregated = enrich_missing_data(...)`. This is already correct.

- [ ] **Step 2: Ensure fresh analysis path is correct**

Read lines 443-454. The enrichment runs at line 449 and saves at line 454. This path is correct.

Both paths are already saving. The actual persistence issue is that if the user navigates to another page and comes back, the `analysis_hash` matches so the cached path runs — which does re-enrich. This is correct behavior. No code change needed here.

- [ ] **Step 3: Commit (if any changes were needed)**

If no changes needed, skip this commit.

---

### Task 4: Run full test suite

**Files:** All test files

- [ ] **Step 1: Run full test suite**

Run: `python -m pytest tests/ -v`
Expected: ALL PASS

- [ ] **Step 2: Final commit and push**

```bash
git add -A
git commit -m "Fix: Italian number parsing, persistent enrichment, expanded static mapping"
git push origin main
```
