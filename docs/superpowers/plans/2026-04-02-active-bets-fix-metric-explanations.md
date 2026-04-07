# Active Bets Fix + Metric Explanations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the Active Bets table to display all 4 columns, and add Italian-language metric explanations for retail investors across all dashboard pages.

**Architecture:** Task 1 investigates the Active Bets display bug by running the actual analysis and inspecting the DataFrame output, then fixes the rendering. Task 2 adds `st.expander` blocks after each metric/chart across 5 dashboard pages with pre-written Italian copy.

**Tech Stack:** Streamlit (st.expander, st.markdown), pandas, existing analytics modules.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/dashboard/pages/02_xray_overview.py` | Modify | Fix Active Bets table + add KPI explanations |
| `src/dashboard/pages/03_redundancy.py` | Modify | Add redundancy/TER explanations |
| `src/dashboard/pages/04_overlap.py` | Modify | Add overlap explanation |
| `src/dashboard/pages/05_sector_country.py` | Modify | Add sector/country/deviation explanations |
| `src/dashboard/pages/06_factor_fingerprint.py` | Modify | Add factor fingerprint/coverage explanations |

No new files needed. No test files — these are pure UI copy changes verified by visual inspection and no-crash run.

---

### Task 1: Fix Active Bets Table

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py:71-100`

- [ ] **Step 1: Reproduce the bug — inspect Active Bets DataFrame**

Run this script to see what `active_share()` actually returns:

```python
from src.ingestion.orchestrator import FetchOrchestrator
from src.analytics.aggregator import aggregate_portfolio
from src.analytics.active_share import active_share
from src.analytics.benchmark import BenchmarkManager

o = FetchOrchestrator()
r1 = o.fetch("CSPX")
r2 = o.fetch("SWDA")

holdings_db = {"CSPX": r1.holdings, "SWDA": r2.holdings}
positions = [{"ticker": "CSPX", "capital": 30000}, {"ticker": "SWDA", "capital": 40000}]
agg = aggregate_portfolio(positions, holdings_db)

bmgr = BenchmarkManager()
bench_df = bmgr.get_benchmark_holdings("MSCI_WORLD")

result = active_share(agg, bench_df)
print("Active Share:", result["active_share_pct"])

bets = result["top_active_bets"]
print("\n--- top_active_bets columns:", bets.columns.tolist())
print("\n--- top 5 overweights:")
print(bets.nlargest(5, "overweight")[["name", "portfolio_weight", "benchmark_weight", "overweight"]].to_string())

print("\n--- top 5 underweights (smallest overweight, which are the least overweight):")
print(bets.nsmallest(5, "overweight")[["name", "portfolio_weight", "benchmark_weight", "overweight"]].to_string())

missed = result["missed_exposures"]
print("\n--- missed_exposures columns:", missed.columns.tolist())
print("\n--- top 5 missed:")
if not missed.empty:
    print(missed.head(5).to_string())
else:
    print("(empty)")
```

Check: Do `portfolio_weight` and `benchmark_weight` contain real non-zero values? Are column names exactly as expected?

- [ ] **Step 2: Fix the Active Bets rendering in 02_xray_overview.py**

The current code at lines 71-100 already selects 4 columns. The bug is likely that:
1. The `top_active_bets` DataFrame only contains positions where `overweight > 0` (see `active_share.py:59`), so there are no true underweights in `top_bets` — `nsmallest(10, "overweight")` picks the *least overweight*, not actually underweight positions.
2. The `missed_exposures` DataFrame (benchmark positions absent from portfolio) is never shown in the dashboard.

Fix: Replace the "Top 10 Sottopesi" section to use `missed_exposures` from `active_share_result` instead of `nsmallest` on `top_active_bets`. The `missed_exposures` DataFrame has columns: `composite_figi`, `name`, `benchmark_weight`. Display it as: Titolo, Benchmark %.

Updated code for lines 71-100:

```python
# ── Active Bets vs Benchmark ───────────────────────────────────────
if active_share_result:
    st.subheader("Active Bets vs Benchmark")
    col_over, col_under = st.columns(2)

    top_bets: pd.DataFrame = active_share_result["top_active_bets"]
    missed: pd.DataFrame = active_share_result["missed_exposures"]

    with col_over:
        st.markdown("**Top 10 Sovrappesi**")
        if top_bets is not None and not top_bets.empty:
            overweights = top_bets.nlargest(10, "overweight")[
                ["name", "portfolio_weight", "benchmark_weight", "overweight"]
            ].copy()
            overweights.columns = ["Titolo", "Portafoglio %", "Benchmark %", "Delta %"]
            for c in ["Portafoglio %", "Benchmark %", "Delta %"]:
                overweights[c] = overweights[c].map(lambda x: f"{x:.2f}")
            overweights = overweights.reset_index(drop=True)
            overweights.index = overweights.index + 1
            st.dataframe(overweights, use_container_width=True)
        else:
            st.info("Nessun sovrappeso rilevato.")

    with col_under:
        st.markdown("**Top 10 Sottopesi (assenti dal portafoglio)**")
        if missed is not None and not missed.empty:
            underweights = missed.nlargest(10, "benchmark_weight")[
                ["name", "benchmark_weight"]
            ].copy()
            underweights.columns = ["Titolo", "Benchmark %"]
            underweights["Benchmark %"] = underweights["Benchmark %"].map(lambda x: f"{x:.2f}")
            underweights = underweights.reset_index(drop=True)
            underweights.index = underweights.index + 1
            st.dataframe(underweights, use_container_width=True)
        else:
            st.info("Nessun titolo benchmark significativo assente dal portafoglio.")
```

- [ ] **Step 3: Verify the fix**

Run: `python3 -m streamlit run src/dashboard/app.py` (or verify via script that the code has no syntax errors)

Run quick smoke test:
```python
import ast
ast.parse(open("src/dashboard/pages/02_xray_overview.py").read())
print("Syntax OK")
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py
git commit -m "fix: Active Bets table — show all columns, use missed_exposures for underweights"
```

---

### Task 2: Add Metric Explanations — Page 02 (X-Ray Overview)

**Files:**
- Modify: `src/dashboard/pages/02_xray_overview.py:43-54`

- [ ] **Step 1: Add KPI explanations after the KPI row (after line 43)**

Insert after the KPI metrics row (line 43 `k5.metric(...)`) and before the Top 30 heading:

```python
# ── KPI explanations ───────────────────────────────────────────────
with st.expander("ℹ️ Cos'è HHI (Indice di Concentrazione)?"):
    st.markdown(
        "Misura quanto il tuo portafoglio dipende da pochi titoli. "
        "Più è basso, meglio è.\n\n"
        "- **Sotto 0.05** = ben diversificato\n"
        "- **Sopra 0.15** = troppo concentrato\n\n"
        "Se i tuoi top titoli crollano, un HHI alto significa che il tuo portafoglio "
        "ne risente pesantemente."
    )

with st.expander("ℹ️ Cos'è Effective N?"):
    st.markdown(
        "Il numero equivalente di titoli nel tuo portafoglio se fossero tutti con lo stesso peso. "
        "Hai 500 titoli ma Effective N è 30? Significa che il portafoglio è dominato da pochi nomi "
        "— si comporta come se ne avessi solo 30."
    )

if active_share_pct is not None:
    with st.expander("ℹ️ Cos'è Active Share?"):
        st.markdown(
            "Quanto il tuo portafoglio è diverso dal benchmark (es. MSCI World).\n\n"
            "- **0%** = identico al mercato\n"
            "- **100%** = completamente diverso\n\n"
            "- **Sotto 20%** = stai pagando più TER per replicare essenzialmente un indice\n"
            "- **Sopra 60%** = portafoglio molto diverso dal mercato, "
            "con rischi e opportunità specifiche"
        )

with st.expander("ℹ️ Cos'è Top-5 Concentration?"):
    st.markdown(
        "La somma dei pesi dei tuoi 5 titoli più grandi. "
        "Se è 25%, un quarto del tuo portafoglio dipende da 5 aziende "
        "(probabilmente NVDA, AAPL, MSFT, AMZN, GOOGL)."
    )
```

- [ ] **Step 2: Verify syntax**

```python
import ast
ast.parse(open("src/dashboard/pages/02_xray_overview.py").read())
print("Syntax OK")
```

- [ ] **Step 3: Commit**

```bash
git add src/dashboard/pages/02_xray_overview.py
git commit -m "UX: add KPI metric explanations in Italian (page 02)"
```

---

### Task 3: Add Metric Explanations — Page 03 (Redundancy)

**Files:**
- Modify: `src/dashboard/pages/03_redundancy.py:45-68`

- [ ] **Step 1: Add explanations after the bar chart (after line 45) and after TER section (after line 54)**

Insert after `st.plotly_chart(fig, ...)` (line 45) and before the TER subheader:

```python
with st.expander("ℹ️ Cos'è il Redundancy Score?"):
    st.markdown(
        "Per ogni ETF, misura quanta percentuale delle sue holdings è già presente "
        "in un altro ETF che hai in portafoglio.\n\n"
        "Se CSPX (S&P 500) è **75% ridondante**, significa che il 75% di quello che "
        "compri con CSPX lo hai già tramite un altro ETF (es. SWDA che include l'S&P 500).\n\n"
        "Quando la ridondanza è alta (>70%): considera di eliminare l'ETF ridondante "
        "e spostare il capitale sull'ETF più ampio che già copre quei titoli."
    )
```

Insert after the TER section (after line 54, after the `st.write` loop) and before `st.divider()`:

```python
with st.expander("ℹ️ Cos'è il TER Sprecato?"):
    st.markdown(
        "Il costo annuo che paghi per la parte ridondante di un ETF.\n\n"
        "**Calcolato come:** ridondanza % × TER dell'ETF × capitale investito.\n\n"
        "**Esempio:** se hai €30.000 in un ETF con TER 0.20% e ridondanza 75%, "
        "stai \"sprecando\" €45/anno in commissioni per esposizione che hai già."
    )
```

- [ ] **Step 2: Verify syntax**

```python
import ast
ast.parse(open("src/dashboard/pages/03_redundancy.py").read())
print("Syntax OK")
```

- [ ] **Step 3: Commit**

```bash
git add src/dashboard/pages/03_redundancy.py
git commit -m "UX: add redundancy/TER explanations in Italian (page 03)"
```

---

### Task 4: Add Metric Explanations — Page 04 (Overlap)

**Files:**
- Modify: `src/dashboard/pages/04_overlap.py:51`

- [ ] **Step 1: Add overlap explanation after the heatmap chart (after line 51)**

Insert after `st.plotly_chart(fig, ...)`:

```python
with st.expander("ℹ️ Cos'è l'Overlap?"):
    st.markdown(
        "La percentuale di esposizione condivisa tra due ETF. "
        "Un overlap del **53%** tra CSPX e SWDA significa che più della metà del peso "
        "dei due ETF è investita negli stessi titoli.\n\n"
        "Overlap alto (**>50%**) tra due ETF nel tuo portafoglio suggerisce che potresti "
        "semplificare rimuovendo uno dei due."
    )
```

- [ ] **Step 2: Verify syntax**

```python
import ast
ast.parse(open("src/dashboard/pages/04_overlap.py").read())
print("Syntax OK")
```

- [ ] **Step 3: Commit**

```bash
git add src/dashboard/pages/04_overlap.py
git commit -m "UX: add overlap explanation in Italian (page 04)"
```

---

### Task 5: Add Metric Explanations — Page 05 (Sector & Country)

**Files:**
- Modify: `src/dashboard/pages/05_sector_country.py:31-62`

- [ ] **Step 1: Add explanations before the sector/country charts**

Insert after `benchmark_df = ...` (line 31) and before the `col_s, col_c = st.columns(2)` line:

```python
with st.expander("ℹ️ Cos'è l'Esposizione Settoriale e Geografica?"):
    st.markdown(
        "**Esposizione Settoriale:** mostra dove sono realmente investiti i tuoi soldi "
        "per settore (Technology, Healthcare, Financials, ecc.). Se Information Technology "
        "è al 35%, un terzo del tuo portafoglio dipende dal settore tech.\n\n"
        "**Esposizione Geografica:** mostra in quali paesi sono domiciliate le aziende che "
        "possiedi. Se United States è al 65%, quasi due terzi del tuo portafoglio sono in "
        "aziende americane — anche se hai ETF con nomi \"globali\"."
    )
```

- [ ] **Step 2: Add deviation explanation after the benchmark deviation section**

Insert after the benchmark deviation section (after line 108, inside the `if benchmark_df` block, after the second `st.plotly_chart`):

```python
    with st.expander("ℹ️ Cos'è la Deviazione vs Benchmark?"):
        st.markdown(
            "Quanto il tuo portafoglio è sovrappesato o sottopesato rispetto al mercato "
            "in ogni settore/paese.\n\n"
            "- **Barra positiva (verde)** = sovrappeso rispetto al benchmark\n"
            "- **Barra negativa (rossa)** = sottopeso rispetto al benchmark"
        )
```

Note: this expander is indented inside the `if benchmark_df` block (4 spaces).

- [ ] **Step 3: Verify syntax**

```python
import ast
ast.parse(open("src/dashboard/pages/05_sector_country.py").read())
print("Syntax OK")
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/pages/05_sector_country.py
git commit -m "UX: add sector/country/deviation explanations in Italian (page 05)"
```

---

### Task 6: Add Metric Explanations — Page 06 (Factor Fingerprint)

**Files:**
- Modify: `src/dashboard/pages/06_factor_fingerprint.py:111-155`

- [ ] **Step 1: Add factor fingerprint explanation after the radar chart**

Insert after `st.caption("Nota: il fattore Momentum...")` (line 112) and before the Factor Scores table:

```python
with st.expander("ℹ️ Cos'è il Factor Fingerprint?"):
    st.markdown(
        "Il \"DNA\" del tuo portafoglio lungo 4 dimensioni:\n\n"
        "- **Size (Dimensione):** Large-cap (>$10B), Mid-cap ($2-10B) o Small-cap (<$2B). "
        "La maggior parte degli ETF globali è dominata da large-cap.\n\n"
        "- **Value/Growth:** P/E (prezzo/utili) basso = Value (aziende mature, dividendi). "
        "P/E alto = Growth (aziende in crescita, reinvestono utili). "
        "Un P/E medio sopra 25 indica tilt Growth.\n\n"
        "- **Quality:** ROE (ritorno sul patrimonio) alto e debito basso = aziende solide. "
        "ROE basso e debito alto = aziende più rischiose.\n\n"
        "- **Dividend Yield:** Quanto reddito generano le aziende nel tuo portafoglio come "
        "dividendi. Yield sotto 1% è tipico di portafogli Growth, sopra 3% di portafogli Income."
    )
```

- [ ] **Step 2: Add coverage explanation after the coverage stacked bar**

Insert after `st.plotly_chart(fig_cov, ...)` (line 155) and before the Factor Drivers section:

```python
with st.expander("ℹ️ Cos'è la Coverage?"):
    st.markdown(
        "Non tutti i titoli hanno dati fondamentali disponibili. "
        "La barra mostra quanta percentuale del portafoglio è stata analizzata e con quale fonte:\n\n"
        "- **L1 Sector** — classificazione settoriale disponibile\n"
        "- **L2 Fundamentals** — dati reali (P/E, ROE, etc.) da yfinance\n"
        "- **L3 Proxy** — stima basata sulla media del settore\n"
        "- **L4 Unclassified** — nessun dato disponibile"
    )
```

- [ ] **Step 3: Verify syntax**

```python
import ast
ast.parse(open("src/dashboard/pages/06_factor_fingerprint.py").read())
print("Syntax OK")
```

- [ ] **Step 4: Commit**

```bash
git add src/dashboard/pages/06_factor_fingerprint.py
git commit -m "UX: add factor fingerprint/coverage explanations in Italian (page 06)"
```

---

### Task 7: Final Verification and Combined Commit

- [ ] **Step 1: Run all tests**

```bash
python3 -m pytest tests/ -v
```

Expected: all tests pass (no dashboard tests exist, but analytics tests must not regress).

- [ ] **Step 2: Squash into single commit and push**

```bash
git reset --soft HEAD~6
git commit -m "UX: spiegazioni metriche italiano + fix Active Bets tabella"
git push origin main
```

This squashes the 6 task commits into a single clean commit matching the user's requested message.
