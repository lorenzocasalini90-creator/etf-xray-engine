"""Holdings overlap analysis.

Calculates overlap matrices, concentration metrics (HHI),
and shared holdings between ETF pairs.
"""

import pandas as pd


def overlap_matrix(etf_holdings_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute NxN weighted Jaccard overlap matrix between ETFs.

    Weighted Jaccard = sum(min(w_a_i, w_b_i)) / sum(max(w_a_i, w_b_i))
    for all securities i present in either ETF.

    Args:
        etf_holdings_dict: Dict mapping ETF ticker to holdings DataFrame.
            Each DataFrame must have composite_figi and weight_pct columns.

    Returns:
        NxN DataFrame with overlap percentages (0-100).
    """
    tickers = list(etf_holdings_dict.keys())
    n = len(tickers)

    # Build weight vectors: ticker -> {figi: weight}
    weight_vectors: dict[str, dict[str, float]] = {}
    for ticker, df in etf_holdings_dict.items():
        weights: dict[str, float] = {}
        for _, row in df.iterrows():
            figi = row.get("composite_figi")
            if not figi or (isinstance(figi, float) and pd.isna(figi)):
                continue
            w = row.get("weight_pct", 0)
            if pd.isna(w):
                w = 0
            weights[figi] = weights.get(figi, 0) + float(w)
        weight_vectors[ticker] = weights

    matrix = pd.DataFrame(0.0, index=tickers, columns=tickers)

    for i in range(n):
        matrix.iloc[i, i] = 100.0
        for j in range(i + 1, n):
            wa = weight_vectors[tickers[i]]
            wb = weight_vectors[tickers[j]]
            all_figis = set(wa.keys()) | set(wb.keys())

            if not all_figis:
                continue

            min_sum = sum(min(wa.get(f, 0), wb.get(f, 0)) for f in all_figis)
            max_sum = sum(max(wa.get(f, 0), wb.get(f, 0)) for f in all_figis)

            overlap = (min_sum / max_sum * 100) if max_sum > 0 else 0.0
            matrix.iloc[i, j] = overlap
            matrix.iloc[j, i] = overlap

    return matrix


def portfolio_hhi(aggregated_df: pd.DataFrame) -> dict:
    """Calculate Herfindahl-Hirschman Index and concentration metrics.

    Args:
        aggregated_df: Output of aggregate_portfolio() with real_weight_pct.

    Returns:
        Dict with: hhi, effective_n, top_5_pct, top_10_pct, top_20_pct.
    """
    if aggregated_df.empty:
        return {"hhi": 0, "effective_n": 0, "top_5_pct": 0, "top_10_pct": 0, "top_20_pct": 0}

    weights = aggregated_df["real_weight_pct"].values
    total = weights.sum()
    if total == 0:
        return {"hhi": 0, "effective_n": 0, "top_5_pct": 0, "top_10_pct": 0, "top_20_pct": 0}

    # Normalize to fractions
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
    """Find holdings shared between two ETFs with their weights.

    Args:
        etf_a_holdings: Holdings DataFrame for ETF A.
        etf_b_holdings: Holdings DataFrame for ETF B.

    Returns:
        DataFrame with: composite_figi, name, weight_a, weight_b, weight_diff.
    """
    def _to_dict(df: pd.DataFrame) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for _, row in df.iterrows():
            figi = row.get("composite_figi")
            if not figi or (isinstance(figi, float) and pd.isna(figi)):
                continue
            w = row.get("weight_pct", 0)
            if pd.isna(w):
                w = 0
            result[figi] = {
                "name": row.get("holding_name", ""),
                "weight": float(w),
            }
        return result

    a_dict = _to_dict(etf_a_holdings)
    b_dict = _to_dict(etf_b_holdings)
    common_figis = set(a_dict.keys()) & set(b_dict.keys())

    if not common_figis:
        return pd.DataFrame(columns=[
            "composite_figi", "name", "weight_a", "weight_b", "weight_diff",
        ])

    rows = []
    for figi in common_figis:
        wa = a_dict[figi]["weight"]
        wb = b_dict[figi]["weight"]
        rows.append({
            "composite_figi": figi,
            "name": a_dict[figi]["name"] or b_dict[figi]["name"],
            "weight_a": wa,
            "weight_b": wb,
            "weight_diff": abs(wa - wb),
        })

    result = pd.DataFrame(rows)
    return result.sort_values("weight_diff", ascending=False).reset_index(drop=True)
