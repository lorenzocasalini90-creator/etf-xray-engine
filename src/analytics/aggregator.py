"""Portfolio aggregation: combine ETF holdings into a single view.

Calculates real exposure per security across multiple ETFs,
weighted by capital allocation.
"""

import pandas as pd

from src.analytics._match_key import add_match_key, build_match_keys_from_holdings


def aggregate_portfolio(
    portfolio_positions: list[dict],
    holdings_db: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Aggregate holdings across multiple ETFs weighted by capital allocation.

    Matches holdings by composite_figi if available, otherwise by
    holding_isin, otherwise by holding_ticker.

    Args:
        portfolio_positions: List of dicts with keys:
            - ticker: ETF ticker
            - capital: Capital allocated in EUR
        holdings_db: Dict mapping ETF ticker to its holdings DataFrame.

    Returns:
        DataFrame with columns: composite_figi, name, ticker, sector,
        country, real_weight_pct, n_etf_sources.
    """
    empty = pd.DataFrame(columns=[
        "composite_figi", "name", "ticker", "sector", "country",
        "real_weight_pct", "n_etf_sources",
    ])

    total_capital = sum(p["capital"] for p in portfolio_positions)
    if total_capital == 0:
        return empty

    # Pre-scan all holdings to build ticker↔ISIN cross-reference
    build_match_keys_from_holdings(holdings_db)

    records: dict[str, dict] = {}

    for position in portfolio_positions:
        ticker = position["ticker"]
        capital = position["capital"]
        etf_weight = capital / total_capital

        df = holdings_db.get(ticker)
        if df is None or df.empty:
            continue

        df = add_match_key(df)
        if "weight_pct" in df.columns:
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce").fillna(0.0)

        for _, row in df.iterrows():
            key = row.get("_match_key")
            if not key or (isinstance(key, float) and pd.isna(key)):
                continue

            holding_weight = row.get("weight_pct", 0)
            if pd.isna(holding_weight):
                holding_weight = 0
            real_weight = etf_weight * (holding_weight / 100.0)

            if key in records:
                records[key]["real_weight_pct"] += real_weight * 100
                records[key]["sources"].add(ticker)
            else:
                records[key] = {
                    "composite_figi": row.get("composite_figi") if "composite_figi" in row.index else None,
                    "name": row.get("holding_name", ""),
                    "ticker": row.get("holding_ticker", ""),
                    "sector": row.get("sector", ""),
                    "country": row.get("country", ""),
                    "real_weight_pct": real_weight * 100,
                    "sources": {ticker},
                }

    if not records:
        return empty

    rows = []
    for rec in records.values():
        rows.append({
            "composite_figi": rec["composite_figi"],
            "name": rec["name"],
            "ticker": rec["ticker"],
            "sector": rec["sector"],
            "country": rec["country"],
            "real_weight_pct": rec["real_weight_pct"],
            "n_etf_sources": len(rec["sources"]),
        })

    result = pd.DataFrame(rows)

    for col in ['real_weight_pct', 'weight_pct', 'market_value', 'shares']:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce').fillna(0.0)

    result = result.sort_values("real_weight_pct", ascending=False).reset_index(drop=True)
    return result


def sector_exposure(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate sector-level exposure from aggregated holdings."""
    if aggregated_df.empty or "sector" not in aggregated_df.columns:
        return pd.DataFrame(columns=["sector", "weight_pct", "n_holdings"])

    df = aggregated_df.copy()
    df["sector"] = df["sector"].fillna("Unknown").replace("", "Unknown")
    df["real_weight_pct"] = pd.to_numeric(df["real_weight_pct"], errors="coerce").fillna(0.0)

    grouped = df.groupby("sector", dropna=False).agg(
        weight_pct=("real_weight_pct", "sum"),
        n_holdings=("name", "count"),
    ).reset_index()
    return grouped.sort_values("weight_pct", ascending=False).reset_index(drop=True)


def country_exposure(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate country-level exposure from aggregated holdings."""
    if aggregated_df.empty or "country" not in aggregated_df.columns:
        return pd.DataFrame(columns=["country", "weight_pct", "n_holdings"])

    df = aggregated_df.copy()
    df["country"] = df["country"].fillna("Unknown").replace("", "Unknown")
    df["real_weight_pct"] = pd.to_numeric(df["real_weight_pct"], errors="coerce").fillna(0.0)

    grouped = df.groupby("country", dropna=False).agg(
        weight_pct=("real_weight_pct", "sum"),
        n_holdings=("name", "count"),
    ).reset_index()
    return grouped.sort_values("weight_pct", ascending=False).reset_index(drop=True)
