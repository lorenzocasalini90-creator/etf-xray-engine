"""Holdings overlap analysis.

Calculates overlap matrices, concentration metrics (HHI),
and shared holdings between ETF pairs.
"""

import pandas as pd

from src.analytics._match_key import add_match_key, build_match_keys_from_holdings


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

    build_match_keys_from_holdings(etf_holdings_dict)

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


def compute_unique_exposure(
    target_ticker: str,
    all_holdings: dict[str, pd.DataFrame],
) -> dict:
    """Compute what would be lost by removing an ETF from the portfolio.

    Args:
        target_ticker: ETF to analyze.
        all_holdings: All portfolio ETFs {ticker: holdings DataFrame}.

    Returns:
        Dict with total_unique_pct, unique_holdings_count,
        total_holdings, main_covering_etf, holdings_detail (DataFrame).
    """
    build_match_keys_from_holdings(all_holdings)

    target_df = all_holdings.get(target_ticker)
    if target_df is None or target_df.empty:
        return {
            "total_unique_pct": 0.0,
            "unique_holdings_count": 0,
            "total_holdings": 0,
            "main_covering_etf": "",
            "holdings_detail": pd.DataFrame(),
        }

    target_df = add_match_key(target_df)
    if "weight_pct" in target_df.columns:
        target_df["weight_pct"] = pd.to_numeric(
            target_df["weight_pct"], errors="coerce"
        ).fillna(0.0)

    # Build target weights
    target_weights: dict[str, dict] = {}
    for _, row in target_df.iterrows():
        key = row.get("_match_key")
        if not key or (isinstance(key, float) and pd.isna(key)):
            continue
        w = float(row.get("weight_pct", 0) or 0)
        name = row.get("holding_name", "")
        ticker_h = row.get("holding_ticker", "")
        if key in target_weights:
            target_weights[key]["weight"] += w
        else:
            target_weights[key] = {"weight": w, "name": name, "ticker": ticker_h}

    # Build other ETFs weight vectors
    other_weights: dict[str, dict[str, float]] = {}
    for etf_ticker, df in all_holdings.items():
        if etf_ticker == target_ticker:
            continue
        df = add_match_key(df)
        if "weight_pct" in df.columns:
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce").fillna(0.0)
        for _, row in df.iterrows():
            key = row.get("_match_key")
            if not key or (isinstance(key, float) and pd.isna(key)):
                continue
            w = float(row.get("weight_pct", 0) or 0)
            if key not in other_weights:
                other_weights[key] = {}
            other_weights[key][etf_ticker] = (
                other_weights[key].get(etf_ticker, 0) + w
            )

    # Compute per-holding coverage
    etf_coverage_total: dict[str, float] = {}
    rows = []

    for key, info in target_weights.items():
        w = info["weight"]
        others = other_weights.get(key, {})
        if others:
            best_etf = max(others, key=others.get)
            covered = min(w, max(others.values()))
        else:
            best_etf = ""
            covered = 0.0

        unique = w - covered
        rows.append({
            "holding_name": info["name"],
            "ticker_holding": info["ticker"],
            "weight_in_target_pct": round(w, 4),
            "covered_weight_pct": round(covered, 4),
            "unique_weight_pct": round(unique, 4),
            "covered_by_etf": best_etf,
        })

        if best_etf:
            etf_coverage_total[best_etf] = (
                etf_coverage_total.get(best_etf, 0) + covered
            )

    detail_df = pd.DataFrame(rows)
    if not detail_df.empty:
        detail_df = detail_df.sort_values(
            "weight_in_target_pct", ascending=False
        ).reset_index(drop=True)

    unique_total = sum(r["unique_weight_pct"] for r in rows)
    unique_count = sum(1 for r in rows if r["covered_weight_pct"] == 0)
    main_etf = max(etf_coverage_total, key=etf_coverage_total.get) if etf_coverage_total else ""

    return {
        "total_unique_pct": round(unique_total, 2),
        "unique_holdings_count": unique_count,
        "total_holdings": len(rows),
        "main_covering_etf": main_etf,
        "holdings_detail": detail_df,
    }
