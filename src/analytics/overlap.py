"""Holdings overlap analysis.

Calculates overlap matrices, concentration metrics (HHI),
and shared holdings between ETF pairs.
"""

import pandas as pd

from src.analytics._match_key import add_match_key


def overlap_matrix(etf_holdings_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute NxN weighted Jaccard overlap matrix between ETFs.

    Matches holdings by composite_figi if available, otherwise by
    holding_isin, otherwise by holding_ticker.

    Args:
        etf_holdings_dict: Dict mapping ETF ticker to holdings DataFrame.

    Returns:
        NxN DataFrame with overlap percentages (0-100).
    """
    tickers = list(etf_holdings_dict.keys())
    n = len(tickers)

    weight_vectors: dict[str, dict[str, float]] = {}
    for ticker, df in etf_holdings_dict.items():
        df = add_match_key(df)
        if "weight_pct" in df.columns:
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce").fillna(0.0)
        weights: dict[str, float] = {}
        for _, row in df.iterrows():
            key = row.get("_match_key")
            if not key or (isinstance(key, float) and pd.isna(key)):
                continue
            w = row.get("weight_pct", 0)
            if pd.isna(w):
                w = 0
            weights[key] = weights.get(key, 0) + float(w)
        weight_vectors[ticker] = weights

    matrix = pd.DataFrame(0.0, index=tickers, columns=tickers)

    for i in range(n):
        matrix.iloc[i, i] = 100.0
        for j in range(i + 1, n):
            wa = weight_vectors[tickers[i]]
            wb = weight_vectors[tickers[j]]
            all_keys = set(wa.keys()) | set(wb.keys())

            if not all_keys:
                continue

            min_sum = sum(min(wa.get(k, 0), wb.get(k, 0)) for k in all_keys)
            max_sum = sum(max(wa.get(k, 0), wb.get(k, 0)) for k in all_keys)

            overlap = (min_sum / max_sum * 100) if max_sum > 0 else 0.0
            matrix.iloc[i, j] = overlap
            matrix.iloc[j, i] = overlap

    return matrix


def portfolio_hhi(aggregated_df: pd.DataFrame) -> dict:
    """Calculate Herfindahl-Hirschman Index and concentration metrics."""
    if aggregated_df.empty:
        return {"hhi": 0, "effective_n": 0, "top_5_pct": 0, "top_10_pct": 0, "top_20_pct": 0}

    weights = pd.to_numeric(aggregated_df["real_weight_pct"], errors="coerce").fillna(0.0).values
    total = weights.sum()
    if total == 0:
        return {"hhi": 0, "effective_n": 0, "top_5_pct": 0, "top_10_pct": 0, "top_20_pct": 0}

    fractions = weights / total
    hhi = float((fractions ** 2).sum())
    effective_n = 1.0 / hhi if hhi > 0 else 0

    sorted_weights = sorted(weights, reverse=True)
    top_5 = sum(sorted_weights[:5])
    top_10 = sum(sorted_weights[:10])
    top_20 = sum(sorted_weights[:20])

    return {
        "hhi": round(hhi, 6),
        "effective_n": round(effective_n, 1),
        "top_5_pct": round(top_5, 2),
        "top_10_pct": round(top_10, 2),
        "top_20_pct": round(top_20, 2),
    }


def shared_holdings(
    etf_a_holdings: pd.DataFrame,
    etf_b_holdings: pd.DataFrame,
) -> pd.DataFrame:
    """Find holdings shared between two ETFs with their weights."""
    def _to_dict(df: pd.DataFrame) -> dict[str, dict]:
        df = add_match_key(df)
        result: dict[str, dict] = {}
        for _, row in df.iterrows():
            key = row.get("_match_key")
            if not key or (isinstance(key, float) and pd.isna(key)):
                continue
            w = row.get("weight_pct", 0)
            if pd.isna(w):
                w = 0
            result[key] = {
                "name": row.get("holding_name", ""),
                "weight": float(w),
            }
        return result

    a_dict = _to_dict(etf_a_holdings)
    b_dict = _to_dict(etf_b_holdings)
    common_keys = set(a_dict.keys()) & set(b_dict.keys())

    if not common_keys:
        return pd.DataFrame(columns=[
            "composite_figi", "name", "weight_a", "weight_b", "weight_diff",
        ])

    rows = []
    for key in common_keys:
        wa = a_dict[key]["weight"]
        wb = b_dict[key]["weight"]
        rows.append({
            "composite_figi": key,
            "name": a_dict[key]["name"] or b_dict[key]["name"],
            "weight_a": wa,
            "weight_b": wb,
            "weight_diff": abs(wa - wb),
        })

    result = pd.DataFrame(rows)
    return result.sort_values("weight_diff", ascending=False).reset_index(drop=True)
