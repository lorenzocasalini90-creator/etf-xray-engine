"""Active Share calculation.

Measures how different a portfolio is from its benchmark
at the individual holdings level.
"""

import pandas as pd

from src.analytics._match_key import add_match_key


def active_share(
    portfolio_aggregated: pd.DataFrame,
    benchmark_aggregated: pd.DataFrame,
) -> dict:
    """Calculate Active Share between portfolio and benchmark.

    Active Share = 0.5 * sum(|w_portfolio(i) - w_benchmark(i)|)
    for all securities i present in either portfolio or benchmark.

    Matches by composite_figi if available, otherwise by holding_isin,
    otherwise by holding_ticker.

    Args:
        portfolio_aggregated: Aggregated portfolio DataFrame with
            real_weight_pct column.
        benchmark_aggregated: Benchmark holdings DataFrame with
            weight_pct column.

    Returns:
        Dict with:
            - active_share_pct: Active Share percentage (0-100)
            - top_active_bets: DataFrame of top overweight positions
            - missed_exposures: DataFrame of significant benchmark positions
              absent from portfolio
    """
    port_weights = _build_weight_dict(portfolio_aggregated, "real_weight_pct")
    bench_weights = _build_weight_dict(benchmark_aggregated, "weight_pct")

    port_weights = _normalize(port_weights)
    bench_weights = _normalize(bench_weights)

    all_keys = set(port_weights.keys()) | set(bench_weights.keys())

    diff_sum = sum(
        abs(port_weights.get(k, 0) - bench_weights.get(k, 0)) for k in all_keys
    )
    active_share_pct = diff_sum / 2.0

    port_names = _build_name_dict(portfolio_aggregated)
    bench_names = _build_name_dict(benchmark_aggregated, name_col="holding_name")

    # Top Active Bets: biggest overweights vs benchmark
    bets = []
    for key in all_keys:
        pw = port_weights.get(key, 0)
        bw = bench_weights.get(key, 0)
        diff = pw - bw
        if diff > 0:
            name = port_names.get(key) or bench_names.get(key, "")
            bets.append({
                "composite_figi": key,
                "name": name,
                "portfolio_weight": round(pw, 4),
                "benchmark_weight": round(bw, 4),
                "overweight": round(diff, 4),
            })
    bets_df = pd.DataFrame(bets)
    if not bets_df.empty:
        bets_df = bets_df.sort_values("overweight", ascending=False).reset_index(drop=True)

    # Missed Exposures: significant benchmark positions absent from portfolio
    missed = []
    for key in bench_weights:
        if key not in port_weights and bench_weights[key] >= 0.05:
            name = bench_names.get(key, "")
            missed.append({
                "composite_figi": key,
                "name": name,
                "benchmark_weight": round(bench_weights[key], 4),
            })
    missed_df = pd.DataFrame(missed)
    if not missed_df.empty:
        missed_df = missed_df.sort_values(
            "benchmark_weight", ascending=False
        ).reset_index(drop=True)

    return {
        "active_share_pct": round(active_share_pct, 2),
        "top_active_bets": bets_df,
        "missed_exposures": missed_df,
    }


def _build_weight_dict(df: pd.DataFrame, weight_col: str) -> dict[str, float]:
    """Extract {match_key: weight} from a DataFrame."""
    result: dict[str, float] = {}
    if df.empty:
        return result

    df = add_match_key(df)
    for _, row in df.iterrows():
        key = row.get("_match_key")
        if not key or (isinstance(key, float) and pd.isna(key)):
            # Fallback: try composite_figi directly (for aggregated DataFrames)
            key = row.get("composite_figi")
            if not key or (isinstance(key, float) and pd.isna(key)):
                continue
        w = row.get(weight_col, 0)
        if pd.isna(w):
            w = 0
        result[key] = result.get(key, 0) + float(w)
    return result


def _build_name_dict(df: pd.DataFrame, name_col: str = "name") -> dict[str, str]:
    """Extract {match_key: name} from a DataFrame."""
    result: dict[str, str] = {}
    if df.empty:
        return result

    df = add_match_key(df)
    for _, row in df.iterrows():
        key = row.get("_match_key")
        if not key or (isinstance(key, float) and pd.isna(key)):
            key = row.get("composite_figi")
            if not key or (isinstance(key, float) and pd.isna(key)):
                continue
        name = row.get(name_col, "")
        if name and isinstance(name, str):
            result[key] = name
    return result


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    """Normalize weights to sum to 100."""
    total = sum(weights.values())
    if total == 0:
        return weights
    factor = 100.0 / total
    return {k: v * factor for k, v in weights.items()}
