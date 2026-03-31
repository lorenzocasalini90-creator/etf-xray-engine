"""Demo: Factor fingerprint for CSPX+SWDA+EIMI portfolio.

Fetches top 50 tickers from yfinance, uses sector proxy for the rest.
"""

import json
import sys
import time

sys.path.insert(0, ".")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.factors.factor_engine import FactorEngine
from src.ingestion.ishares import ISharesFetcher
from src.analytics.aggregator import aggregate_portfolio
from src.storage.models import Base


def main():
    # In-memory DB for demo
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    fetcher = ISharesFetcher()

    print("=" * 60)
    print("FASE 4 DEMO: Factor Fingerprint")
    print("=" * 60)

    # Fetch holdings
    etfs = ["CSPX", "SWDA", "EIMI"]
    holdings_db = {}
    for ticker in etfs:
        print(f"\nFetching {ticker}...", end=" ")
        try:
            df = fetcher.fetch_holdings(ticker)
            # Add composite_figi column (use ISIN as proxy for demo)
            df["composite_figi"] = df["holding_isin"].fillna(df["holding_ticker"])
            holdings_db[ticker] = df
            print(f"{len(df)} holdings")
        except Exception as e:
            print(f"FAILED: {e}")

    if not holdings_db:
        print("No holdings fetched, exiting.")
        return

    # Portfolio: equal capital
    portfolio_positions = [
        {"ticker": "CSPX", "capital": 50_000},
        {"ticker": "SWDA", "capital": 30_000},
        {"ticker": "EIMI", "capital": 20_000},
    ]

    print("\nAggregating portfolio...")
    agg = aggregate_portfolio(portfolio_positions, holdings_db)
    print(f"Total unique holdings: {len(agg)}")

    # Benchmark: SWDA
    benchmark_positions = [{"ticker": "SWDA", "capital": 100_000}]
    bench_agg = aggregate_portfolio(benchmark_positions, holdings_db)

    # Factor analysis (top 50 yfinance, rest sector proxy)
    print(f"\nRunning factor analysis (top 50 via yfinance, rest via sector proxy)...")
    print("This will take ~30 seconds for yfinance calls...\n")

    factor_engine = FactorEngine(session, top_n_yfinance=50)
    start = time.time()
    result = factor_engine.analyze(agg, bench_agg)
    elapsed = time.time() - start

    # Print results
    print("=" * 60)
    print("FACTOR FINGERPRINT")
    print("=" * 60)

    scores = result["factor_scores"]

    print("\n📊 SIZE DISTRIBUTION:")
    for bucket, pct in scores["size"].items():
        bar = "█" * int(pct / 2)
        print(f"  {bucket:10s}: {pct:5.1f}% {bar}")

    print("\n📊 VALUE/GROWTH:")
    vg = scores["value_growth"]
    print(f"  Weighted P/E:  {vg['weighted_pe']}")
    print(f"  Weighted P/B:  {vg['weighted_pb']}")
    print(f"  Style:         {vg['style']}")

    print("\n📊 QUALITY:")
    q = scores["quality"]
    print(f"  Weighted ROE:     {q['weighted_roe']}")
    print(f"  Weighted D/E:     {q['weighted_debt_equity']}")

    print("\n📊 DIVIDEND YIELD:")
    dy = scores["dividend_yield"]
    print(f"  Weighted Yield:   {dy['weighted_yield']}")

    # Coverage
    print("\n📋 COVERAGE REPORT:")
    cov = result["coverage_report"]
    print(f"  Total holdings:   {cov['total_holdings']}")
    print(f"  L1 (sector):      {cov['L1_sector_count']} ({cov['L1_pct']}%)")
    print(f"  L2 (yfinance):    {cov['L2_fundamentals_count']} ({cov['L2_pct']}%)")
    print(f"  L3 (proxy):       {cov['L3_proxy_count']} ({cov['L3_pct']}%)")
    print(f"  L4 (unclassified):{cov['L4_unclassified_count']} ({cov['L4_pct']}%)")

    # Benchmark comparison
    if result["benchmark_comparison"]:
        print("\n📊 BENCHMARK COMPARISON (vs SWDA):")
        delta = result["benchmark_comparison"]

        vg_d = delta["value_growth"]
        print(f"  P/E delta:     {vg_d['pe_delta']}")
        print(f"  P/B delta:     {vg_d['pb_delta']}")
        print(f"  Portfolio:     {vg_d['portfolio_style']}  vs  Benchmark: {vg_d['benchmark_style']}")

        q_d = delta["quality"]
        print(f"  ROE delta:     {q_d['roe_delta']}")
        print(f"  D/E delta:     {q_d['debt_equity_delta']}")

        dy_d = delta["dividend_yield"]
        print(f"  Yield delta:   {dy_d['yield_delta']}")

        size_d = delta["size"]
        for k, v in size_d.items():
            print(f"  {k}: {v:+.1f}%")

    # Factor drivers
    print("\n📊 TOP FACTOR DRIVERS (by weight):")
    drivers = result["factor_drivers"]
    print("\n  Value/Growth drivers:")
    for d in drivers["value_growth"]:
        print(f"    {d['ticker']:6s}  weight={d['weight']:5.2f}%  PE={d.get('pe_ratio', 'N/A')}  style={d['style']}")

    print(f"\nCompleted in {elapsed:.1f}s")
    session.close()


if __name__ == "__main__":
    main()
