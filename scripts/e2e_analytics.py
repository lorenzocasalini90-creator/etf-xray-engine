"""End-to-end analytics test with real data.

Portfolio: CSPX 30K, SWDA 40K, EIMI 15K EUR.
Downloads holdings, resolves FIGI, runs full analytics suite.
"""

import logging
import sys

import pandas as pd

from src.analytics.active_share import active_share
from src.analytics.aggregator import aggregate_portfolio, country_exposure, sector_exposure
from src.analytics.overlap import overlap_matrix, portfolio_hhi, shared_holdings
from src.analytics.redundancy import redundancy_scores
from src.ingestion.ishares import ISharesFetcher
from src.resolution.figi_resolver import FigiResolver, get_api_key
from src.resolution.normalizer import normalize_isin, normalize_name
from src.storage.db import get_session_factory, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PORTFOLIO = [
    {"ticker": "CSPX", "capital": 30_000},
    {"ticker": "SWDA", "capital": 40_000},
    {"ticker": "EIMI", "capital": 15_000},
]

BENCHMARK = "SWDA"


def main() -> None:
    api_key = get_api_key()
    if not api_key:
        logger.warning("No OPENFIGI_API_KEY in .env — rate limits will be strict")

    init_db()
    session = get_session_factory()()
    fetcher = ISharesFetcher()
    resolver = FigiResolver(session, api_key=api_key)

    # Step 1: Fetch & resolve holdings for each ETF
    holdings_db: dict[str, pd.DataFrame] = {}
    for pos in PORTFOLIO:
        ticker = pos["ticker"]
        logger.info("=" * 40 + f" {ticker} " + "=" * 40)

        df = fetcher.fetch_holdings(ticker)
        logger.info("Fetched %d holdings for %s", len(df), ticker)

        # Normalize
        df["holding_isin"] = df["holding_isin"].apply(
            lambda x: normalize_isin(x) if isinstance(x, str) else x
        )
        df["holding_name"] = df["holding_name"].apply(
            lambda x: normalize_name(x) if isinstance(x, str) else x
        )

        # Resolve FIGIs
        df = resolver.resolve_batch(df)
        resolved = df["composite_figi"].notna().sum()
        logger.info("Resolved %d/%d FIGIs for %s", resolved, len(df), ticker)

        holdings_db[ticker] = df

    # Step 2: Aggregate portfolio
    print("\n" + "=" * 70)
    print("PORTFOLIO AGGREGATION")
    print("=" * 70)
    agg = aggregate_portfolio(PORTFOLIO, holdings_db)
    print(f"\nUnique securities: {len(agg)}")
    print(f"Total weight: {agg['real_weight_pct'].sum():.2f}%")
    print(f"\nTop 20 holdings:")
    print("-" * 70)
    for _, row in agg.head(20).iterrows():
        print(
            f"  {str(row['name'])[:35]:35s} "
            f"{row['real_weight_pct']:6.2f}%  "
            f"(in {row['n_etf_sources']} ETF)"
        )

    # Step 3: Sector & Country exposure
    print("\n" + "=" * 70)
    print("SECTOR EXPOSURE")
    print("=" * 70)
    sectors = sector_exposure(agg)
    for _, row in sectors.head(10).iterrows():
        print(f"  {str(row['sector'])[:30]:30s} {row['weight_pct']:6.2f}%  ({row['n_holdings']} holdings)")

    print("\n" + "=" * 70)
    print("COUNTRY EXPOSURE (top 10)")
    print("=" * 70)
    countries = country_exposure(agg)
    for _, row in countries.head(10).iterrows():
        print(f"  {str(row['country'])[:30]:30s} {row['weight_pct']:6.2f}%  ({row['n_holdings']} holdings)")

    # Step 4: Overlap matrix
    print("\n" + "=" * 70)
    print("OVERLAP MATRIX (Weighted Jaccard %)")
    print("=" * 70)
    matrix = overlap_matrix(holdings_db)
    print(matrix.round(1).to_string())

    # Step 5: Concentration (HHI)
    print("\n" + "=" * 70)
    print("CONCENTRATION (HHI)")
    print("=" * 70)
    hhi = portfolio_hhi(agg)
    print(f"  HHI:          {hhi['hhi']:.6f}")
    print(f"  Effective N:  {hhi['effective_n']:.0f}")
    print(f"  Top 5:        {hhi['top_5_pct']:.2f}%")
    print(f"  Top 10:       {hhi['top_10_pct']:.2f}%")
    print(f"  Top 20:       {hhi['top_20_pct']:.2f}%")

    # Step 6: Shared holdings CSPX vs SWDA
    print("\n" + "=" * 70)
    print("SHARED HOLDINGS: CSPX vs SWDA (top 10)")
    print("=" * 70)
    shared = shared_holdings(holdings_db["CSPX"], holdings_db["SWDA"])
    print(f"Total shared: {len(shared)} securities")
    for _, row in shared.head(10).iterrows():
        print(
            f"  {str(row['name'])[:30]:30s}  "
            f"CSPX: {row['weight_a']:5.2f}%  SWDA: {row['weight_b']:5.2f}%  "
            f"Diff: {row['weight_diff']:5.2f}%"
        )

    # Step 7: Redundancy scores
    print("\n" + "=" * 70)
    print("REDUNDANCY SCORES")
    print("=" * 70)
    redundancy = redundancy_scores(PORTFOLIO, holdings_db)
    for _, row in redundancy.iterrows():
        emoji = {"green": "OK", "yellow": "WARN", "red": "HIGH"}[row["verdict"]]
        print(
            f"  {row['etf_ticker']:6s}  "
            f"Redundancy: {row['redundancy_pct']:5.1f}%  "
            f"Unique: {row['unique_pct']:5.1f}%  "
            f"TER wasted: €{row['ter_wasted']:.2f}  "
            f"[{emoji}]"
        )

    # Step 8: Active Share vs SWDA benchmark
    print("\n" + "=" * 70)
    print(f"ACTIVE SHARE vs {BENCHMARK}")
    print("=" * 70)
    bench_df = holdings_db[BENCHMARK]
    result = active_share(agg, bench_df)
    print(f"  Active Share: {result['active_share_pct']:.1f}%")

    if not result["top_active_bets"].empty:
        print(f"\n  Top 10 Active Bets (overweight vs benchmark):")
        for _, row in result["top_active_bets"].head(10).iterrows():
            print(
                f"    {str(row['name'])[:30]:30s}  "
                f"Port: {row['portfolio_weight']:5.2f}%  "
                f"Bench: {row['benchmark_weight']:5.2f}%  "
                f"Over: +{row['overweight']:.2f}%"
            )

    if not result["missed_exposures"].empty:
        print(f"\n  Top 10 Missed Exposures (in benchmark, absent from portfolio):")
        for _, row in result["missed_exposures"].head(10).iterrows():
            print(
                f"    {str(row['name'])[:30]:30s}  "
                f"Bench: {row['benchmark_weight']:5.2f}%"
            )

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)

    session.close()


if __name__ == "__main__":
    main()
