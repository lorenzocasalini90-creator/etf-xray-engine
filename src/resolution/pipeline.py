"""End-to-end pipeline: fetch → resolve → store.

Fetches ETF holdings, resolves identifiers via OpenFIGI,
and persists results to the database.
"""

import logging
from datetime import date, datetime

import pandas as pd
from sqlalchemy.orm import Session

from src.ingestion.ishares import ISharesFetcher
from src.resolution.figi_resolver import FigiResolver
from src.resolution.normalizer import deduplicate_holdings, normalize_isin, normalize_name
from src.storage.db import get_session_factory, init_db
from src.storage.models import EtfMetadata, FigiMapping, Holding

logger = logging.getLogger(__name__)


def run_pipeline(ticker: str, api_key: str | None = None) -> None:
    """Run the full fetch → resolve → store pipeline for an ETF.

    Args:
        ticker: ETF ticker (e.g. "CSPX").
        api_key: Optional OpenFIGI API key.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Init DB
    init_db()
    session_factory = get_session_factory()
    session = session_factory()

    try:
        # Step 1: Fetch holdings
        logger.info("Fetching holdings for %s...", ticker)
        fetcher = ISharesFetcher()
        df = fetcher.fetch_holdings(ticker)
        logger.info("Fetched %d holdings", len(df))

        # Step 2: Normalize
        df["holding_isin"] = df["holding_isin"].apply(
            lambda x: normalize_isin(x) if isinstance(x, str) else x
        )
        df["holding_name"] = df["holding_name"].apply(
            lambda x: normalize_name(x) if isinstance(x, str) else x
        )

        # Step 3: Resolve FIGIs
        logger.info("Resolving FIGIs...")
        resolver = FigiResolver(session, api_key=api_key)
        df = resolver.resolve_batch(df)

        # Step 4: Deduplicate
        df = deduplicate_holdings(df)
        logger.info("After dedup: %d holdings", len(df))

        # Step 5: Store to DB
        _store_results(session, df, ticker)

        # Step 6: Print report
        report = resolver.get_report(len(df))
        print("\n" + "=" * 60)
        print(f"RESOLUTION REPORT — {ticker}")
        print("=" * 60)
        print(report)
        print("\nTop 10 holdings con FIGI:")
        print("-" * 60)
        resolved_df = df[df["composite_figi"].notna()].head(10)
        for _, row in resolved_df.iterrows():
            print(
                f"  {row.get('holding_name', 'N/A'):40s} "
                f"{row.get('weight_pct', 0):6.2f}%  "
                f"FIGI: {row['composite_figi']}"
            )
        print("=" * 60)

    finally:
        session.close()


def _store_results(session: Session, df: pd.DataFrame, ticker: str) -> None:
    """Store resolved holdings to the database.

    Args:
        session: SQLAlchemy session.
        df: Resolved holdings DataFrame.
        ticker: ETF ticker.
    """
    # Ensure ETF metadata exists
    etf = session.query(EtfMetadata).filter(EtfMetadata.ticker == ticker).first()
    if not etf:
        etf = EtfMetadata(
            ticker=ticker,
            name=f"iShares {ticker}",
            issuer="iShares",
        )
        session.add(etf)
        session.flush()

    as_of = None
    if "as_of_date" in df.columns:
        raw = df["as_of_date"].dropna().iloc[0] if not df["as_of_date"].dropna().empty else None
        if raw:
            if isinstance(raw, str):
                as_of = datetime.strptime(raw, "%Y-%m-%d").date()
            elif isinstance(raw, date):
                as_of = raw
    if not as_of:
        as_of = date.today()

    count = 0
    for _, row in df.iterrows():
        figi_id = None
        composite = row.get("composite_figi")
        if composite and isinstance(composite, str):
            mapping = session.query(FigiMapping).filter(
                FigiMapping.composite_figi == composite
            ).first()
            if mapping:
                figi_id = mapping.id

        holding = Holding(
            etf_id=etf.id,
            figi_id=figi_id,
            holding_name=str(row.get("holding_name", ""))[:255],
            weight_pct=row.get("weight_pct"),
            market_value=row.get("market_value"),
            shares=row.get("shares"),
            sector=row.get("sector"),
            country=row.get("country"),
            currency=row.get("currency"),
            as_of_date=as_of,
        )
        session.add(holding)
        count += 1

    session.commit()
    logger.info("Stored %d holdings for %s", count, ticker)


if __name__ == "__main__":
    import sys

    ticker = sys.argv[1] if len(sys.argv) > 1 else "CSPX"
    api_key = sys.argv[2] if len(sys.argv) > 2 else None
    run_pipeline(ticker, api_key)
