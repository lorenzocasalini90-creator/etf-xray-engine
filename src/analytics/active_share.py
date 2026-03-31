"""Active Share calculation.

Measures how different a portfolio is from its benchmark
at the individual holdings level.
"""

import pandas as pd


def active_share(
    portfolio_aggregated: pd.DataFrame,
    benchmark_aggregated: pd.DataFrame,
) -> dict:
    """Calculate Active Share between portfolio and benchmark.

    Active Share = 0.5 * sum(|w_portfolio(i) - w_benchmark(i)|)
    for all securities i present in either portfolio or benchmark.

    Args:
        portfolio_aggregated: Aggregated portfolio DataFrame with
            composite_figi and real_weight_pct columns.
        benchmark_aggregated: Benchmark holdings DataFrame with
            composite_figi and weight_pct columns.

    Returns:
        Dict with:
            - active_share_pct: Active Share percentage (0-100)
            - top_active_bets: DataFrame of top overweight positions
            - missed_exposures: DataFrame of significant benchmark positions
              absent from portfolio
    """
    # Build weight dicts (normalized to sum to 100)
    port_weights = _build_weight_dict(portfolio_aggregated, "real_weight_pct")
    bench_weights = _build_weight_dict(benchmark_aggregated, "weight_pct")

    # Normalize both to sum to 100
    port_weights = _normalize(port_weights)
    bench_weights = _normalize(bench_weights)

    all_figis = set(port_weights.keys()) | set(bench_weights.keys())

    # Active Share
    diff_sum = sum(
        abs(port_weights.get(f, 0) - bench_weights.get(f, 0)) for f in all_figis
    )
    active_share_pct = diff_sum / 2.0

    # Name lookups
    port_names = _build_name_dict(portfolio_aggregated)
    bench_names = _build_name_dict(benchmark_aggregated, name_col="holding_name")

    # Top Active Bets: biggest overweights vs benchmark
    bets = []
    for figi in all_figis:
        pw = port_weights.get(figi, 0)
        bw = bench_weights.get(figi, 0)
        diff = pw - bw
        if diff > 0:
            name = port_names.get(figi) or bench_names.get(figi, "")
            bets.append({
                "composite_figi": figi,
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
    for figi in bench_weights:
        if figi not in port_weights and bench_weights[figi] >= 0.05:
            name = bench_names.get(figi, "")
            missed.append({
                "composite_figi": figi,
                "name": name,
                "benchmark_weight": round(bench_weights[figi], 4),
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
    """Extract {figi: weight} from a DataFrame."""
    result: dict[str, float] = {}
    if df.empty:
        return result
    for _, row in df.iterrows():
        figi = row.get("composite_figi")
        if not figi or (isinstance(figi, float) and pd.isna(figi)):
            continue
        w = row.get(weight_col, 0)
        if pd.isna(w):
            w = 0
        result[figi] = result.get(figi, 0) + float(w)
    return result


def _build_name_dict(df: pd.DataFrame, name_col: str = "name") -> dict[str, str]:
    """Extract {figi: name} from a DataFrame."""
    result: dict[str, str] = {}
    if df.empty:
        return result
    for _, row in df.iterrows():
        figi = row.get("composite_figi")
        if not figi or (isinstance(figi, float) and pd.isna(figi)):
            continue
        name = row.get(name_col, "")
        if name and isinstance(name, str):
            result[figi] = name
    return result


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    """Normalize weights to sum to 100."""
    total = sum(weights.values())
    if total == 0:
        return weights
    factor = 100.0 / total
    return {k: v * factor for k, v in weights.items()}
