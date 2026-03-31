"""ETF redundancy analysis.

Calculates how much of each ETF's holdings overlap with other ETFs
in the portfolio, and estimates the wasted TER cost.
"""

import pandas as pd

# Default TER estimates for common ETFs (annual %)
DEFAULT_TER: dict[str, float] = {
    "CSPX": 0.07,
    "SWDA": 0.20,
    "IWDA": 0.20,
    "EIMI": 0.18,
    "VWCE": 0.22,
    "ISAC": 0.20,
    "CSNDX": 0.33,
    "IUIT": 0.15,
}


def redundancy_scores(
    portfolio_positions: list[dict],
    holdings_db: dict[str, pd.DataFrame],
    ter_override: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Calculate redundancy score for each ETF in a portfolio.

    For each ETF, determines what percentage of its weight is in holdings
    that also appear in at least one other ETF in the portfolio.

    Args:
        portfolio_positions: List of dicts with keys: ticker, capital.
        holdings_db: Dict mapping ETF ticker to holdings DataFrame.
            Must have composite_figi and weight_pct columns.
        ter_override: Optional dict to override default TER values.

    Returns:
        DataFrame with: etf_ticker, redundancy_pct, unique_pct,
        ter_wasted, verdict (green/yellow/red).
    """
    ter_map = {**DEFAULT_TER, **(ter_override or {})}
    tickers = [p["ticker"] for p in portfolio_positions]
    capital_map = {p["ticker"]: p["capital"] for p in portfolio_positions}

    # Build FIGI sets per ETF
    figi_sets: dict[str, set[str]] = {}
    figi_weights: dict[str, dict[str, float]] = {}

    for ticker in tickers:
        df = holdings_db.get(ticker)
        if df is None or df.empty:
            figi_sets[ticker] = set()
            figi_weights[ticker] = {}
            continue

        if "weight_pct" in df.columns:
            df = df.copy()
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce").fillna(0.0)

        weights: dict[str, float] = {}
        for _, row in df.iterrows():
            figi = row.get("composite_figi")
            if not figi or (isinstance(figi, float) and pd.isna(figi)):
                continue
            w = row.get("weight_pct", 0)
            if pd.isna(w):
                w = 0
            weights[figi] = weights.get(figi, 0) + float(w)
        figi_sets[ticker] = set(weights.keys())
        figi_weights[ticker] = weights

    rows = []
    for ticker in tickers:
        # FIGIs present in at least one OTHER ETF
        other_figis: set[str] = set()
        for other in tickers:
            if other != ticker:
                other_figis |= figi_sets.get(other, set())

        my_figis = figi_sets.get(ticker, set())
        my_weights = figi_weights.get(ticker, {})
        total_weight = sum(my_weights.values())

        if total_weight == 0:
            rows.append({
                "etf_ticker": ticker,
                "redundancy_pct": 0.0,
                "unique_pct": 100.0,
                "ter_wasted": 0.0,
                "verdict": "green",
            })
            continue

        redundant_weight = sum(
            my_weights[figi] for figi in my_figis & other_figis
        )
        redundancy_pct = (redundant_weight / total_weight) * 100
        unique_pct = 100.0 - redundancy_pct

        ter = ter_map.get(ticker.upper(), 0.20)  # default 0.20% if unknown
        capital = capital_map.get(ticker, 0)
        ter_wasted = (redundancy_pct / 100) * (ter / 100) * capital

        if redundancy_pct < 30:
            verdict = "green"
        elif redundancy_pct < 70:
            verdict = "yellow"
        else:
            verdict = "red"

        rows.append({
            "etf_ticker": ticker,
            "redundancy_pct": round(redundancy_pct, 2),
            "unique_pct": round(unique_pct, 2),
            "ter_wasted": round(ter_wasted, 2),
            "verdict": verdict,
        })

    return pd.DataFrame(rows)
