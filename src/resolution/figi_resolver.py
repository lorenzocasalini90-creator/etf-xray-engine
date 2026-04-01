"""OpenFIGI identity resolution with cascading lookup strategy.

Resolves holdings to Composite FIGI identifiers using the OpenFIGI v3 API.
Cascade order: ISIN → CUSIP → SEDOL → Ticker+Exchange.

Performance: bulk cache lookup + batched API calls (100 jobs/request).
For 1300 holdings with warm cache: ~0s. Cold: ~13 API calls × 12s = ~156s.
"""

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.storage.models import FigiMapping

# Load .env from project root
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logger = logging.getLogger(__name__)

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
BATCH_SIZE_WITH_KEY = 100  # OpenFIGI v3 allows 100 jobs with API key
BATCH_SIZE_NO_KEY = 10  # Without key: smaller batches to avoid 413
RATE_LIMIT_DELAY = 6.0  # ~10 req/min with key, ~5 req/min without
MAX_RETRIES = 3
BACKOFF_BASE = 2.0
RESOLVE_TIMEOUT = 180  # max seconds before returning partial results


def get_api_key() -> str | None:
    """Read OpenFIGI API key from environment / .env file."""
    return os.getenv("OPENFIGI_API_KEY")


@dataclass
class FigiResult:
    """Result of a single FIGI resolution."""

    composite_figi: str
    name: str | None = None
    ticker: str | None = None
    exchange: str | None = None
    security_type: str | None = None
    market_sector: str | None = None


class FigiResolver:
    """Resolve security identifiers to Composite FIGI via OpenFIGI API.

    Implements a cascade: ISIN → CUSIP → SEDOL → Ticker+Exchange.
    Caches results in DB to avoid redundant API calls.

    Args:
        session: SQLAlchemy session for cache lookups and writes.
        api_key: Optional OpenFIGI API key for higher rate limits.
    """

    def __init__(self, session: Session, api_key: str | None = None) -> None:
        self._session = session
        self._api_key = api_key
        self._http = requests.Session()
        self._http.headers["Content-Type"] = "application/json"
        if api_key:
            self._http.headers["X-OPENFIGI-APIKEY"] = api_key
        self._batch_size = BATCH_SIZE_WITH_KEY if api_key else BATCH_SIZE_NO_KEY
        self._last_request_time: float = 0.0
        self._start_time: float = 0.0

        # Resolution statistics
        self.stats: dict[str, int] = {
            "isin": 0,
            "cusip": 0,
            "sedol": 0,
            "ticker": 0,
            "cache": 0,
            "unresolved": 0,
        }

    def resolve_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resolve holdings using bulk cache + batched API calls.

        1. Bulk cache lookup for ALL rows in one SQL query.
        2. Cascade through ISIN → CUSIP → SEDOL → Ticker for uncached.
        3. Batch API calls (100 jobs per request).

        Args:
            df: Holdings DataFrame with standard schema columns.

        Returns:
            DataFrame with ``composite_figi`` column added.
        """
        self.stats = {k: 0 for k in self.stats}
        self._start_time = time.time()
        df = df.copy()
        df["composite_figi"] = None

        # Step 1: Bulk cache lookup — ONE query for all identifiers
        n_cached = self._bulk_cache_lookup(df)
        logger.info("Cache hit: %d/%d holdings", n_cached, len(df))

        # Step 2: Cascade through identifier types for uncached rows
        unresolved_mask = df["composite_figi"].isna()
        cascade_steps = [
            ("isin", "ID_ISIN", "holding_isin"),
            ("cusip", "ID_CUSIP", "holding_cusip"),
            ("sedol", "ID_SEDOL", "holding_sedol"),
        ]

        for step_name, id_type, col in cascade_steps:
            if not unresolved_mask.any() or self._is_timed_out():
                break

            unresolved = df.loc[unresolved_mask]
            has_id = unresolved[col].notna() & (unresolved[col].astype(str).str.strip() != "")
            candidates = unresolved.loc[has_id]

            if candidates.empty:
                continue

            jobs = []
            job_indices = []
            for idx, row in candidates.iterrows():
                jobs.append({"idType": id_type, "idValue": str(row[col]).strip()})
                job_indices.append(idx)

            resolved = self._send_batches(jobs)
            for i, figi_result in enumerate(resolved):
                if figi_result is not None:
                    df_idx = job_indices[i]
                    df.at[df_idx, "composite_figi"] = figi_result.composite_figi
                    self._save_to_cache(df.loc[df_idx], figi_result)
                    self.stats[step_name] += 1

            unresolved_mask = df["composite_figi"].isna()

        # Step 3: Ticker fallback for still-unresolved
        if unresolved_mask.any() and not self._is_timed_out():
            unresolved = df.loc[unresolved_mask]
            has_ticker = unresolved["holding_ticker"].notna() & (
                unresolved["holding_ticker"].astype(str).str.strip() != ""
            )
            candidates = unresolved.loc[has_ticker]

            if not candidates.empty:
                jobs = []
                job_indices = []
                for idx, row in candidates.iterrows():
                    jobs.append({"idType": "TICKER", "idValue": str(row["holding_ticker"]).strip()})
                    job_indices.append(idx)

                resolved = self._send_batches(jobs)
                for i, figi_result in enumerate(resolved):
                    if figi_result is not None:
                        df_idx = job_indices[i]
                        df.at[df_idx, "composite_figi"] = figi_result.composite_figi
                        self._save_to_cache(df.loc[df_idx], figi_result)
                        self.stats["ticker"] += 1

                unresolved_mask = df["composite_figi"].isna()

        self.stats["unresolved"] = int(unresolved_mask.sum())
        elapsed = time.time() - self._start_time
        logger.info("FIGI resolution done in %.1fs", elapsed)
        self._log_stats(len(df))
        return df

    # ------------------------------------------------------------------
    # Bulk cache
    # ------------------------------------------------------------------

    def _bulk_cache_lookup(self, df: pd.DataFrame) -> int:
        """Look up all identifiers in DB cache with a single query.

        Updates the ``composite_figi`` column in-place for cached rows.

        Args:
            df: DataFrame with identifier columns. Modified in place.

        Returns:
            Number of cache hits.
        """
        # Collect all unique identifiers to look up
        ticker_vals = set()
        isin_vals = set()

        for _, row in df.iterrows():
            isin = row.get("holding_isin")
            if isin and isinstance(isin, str) and isin.strip():
                isin_vals.add(isin.strip())
            ticker = row.get("holding_ticker")
            if ticker and isinstance(ticker, str) and ticker.strip():
                ticker_vals.add(ticker.strip())

        if not isin_vals and not ticker_vals:
            return 0

        # Single query for all identifiers
        filters = []
        if isin_vals:
            filters.append(FigiMapping.isin.in_(isin_vals))
        if ticker_vals:
            filters.append(FigiMapping.ticker.in_(ticker_vals))

        mappings = self._session.query(FigiMapping).filter(or_(*filters)).all()

        # Build lookup dicts
        isin_to_figi: dict[str, str] = {}
        ticker_to_figi: dict[str, str] = {}
        for m in mappings:
            if m.isin:
                isin_to_figi[m.isin] = m.composite_figi
            if m.ticker:
                ticker_to_figi[m.ticker] = m.composite_figi

        # Apply to DataFrame
        hits = 0
        for idx, row in df.iterrows():
            if df.at[idx, "composite_figi"] is not None:
                continue

            isin = row.get("holding_isin")
            if isin and isinstance(isin, str) and isin.strip() in isin_to_figi:
                df.at[idx, "composite_figi"] = isin_to_figi[isin.strip()]
                hits += 1
                continue

            ticker = row.get("holding_ticker")
            if ticker and isinstance(ticker, str) and ticker.strip() in ticker_to_figi:
                df.at[idx, "composite_figi"] = ticker_to_figi[ticker.strip()]
                hits += 1

        self.stats["cache"] = hits
        return hits

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    def _send_batches(self, jobs: list[dict]) -> list[FigiResult | None]:
        """Send jobs to OpenFIGI in batches of 100, respecting rate limits.

        Args:
            jobs: List of OpenFIGI job dicts.

        Returns:
            List of FigiResult (or None) in same order as jobs.
        """
        all_results: list[FigiResult | None] = []

        for i in range(0, len(jobs), self._batch_size):
            if self._is_timed_out():
                all_results.extend([None] * (len(jobs) - i))
                break

            batch = jobs[i:i + self._batch_size]
            self._rate_limit()

            logger.info(
                "OpenFIGI batch %d-%d of %d",
                i + 1, min(i + self._batch_size, len(jobs)), len(jobs),
            )
            response_data = self._api_call(batch)

            if response_data is None:
                all_results.extend([None] * len(batch))
                continue

            for item in response_data:
                if "data" in item and item["data"]:
                    d = item["data"][0]
                    all_results.append(FigiResult(
                        composite_figi=d.get("compositeFIGI", ""),
                        name=d.get("name"),
                        ticker=d.get("ticker"),
                        exchange=d.get("exchCode"),
                        security_type=d.get("securityType"),
                        market_sector=d.get("marketSectorDes"),
                    ))
                else:
                    all_results.append(None)

        return all_results

    def _api_call(self, batch: list[dict]) -> list[dict] | None:
        """Execute a single POST to the OpenFIGI API with retries.

        Args:
            batch: List of job dicts for a single API call.

        Returns:
            Parsed JSON response list, or None after all retries fail.
        """
        for attempt in range(MAX_RETRIES):
            try:
                resp = self._http.post(OPENFIGI_URL, json=batch, timeout=30)
                if resp.status_code == 429:
                    wait = RATE_LIMIT_DELAY * (attempt + 1)
                    logger.warning("Rate limited, waiting %.1fs", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ConnectionError, TimeoutError) as exc:
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    "OpenFIGI API error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1, MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)

        logger.error("OpenFIGI API failed after %d attempts", MAX_RETRIES)
        return None

    def _is_timed_out(self) -> bool:
        """Check if the current resolve_batch has exceeded the timeout."""
        elapsed = time.time() - self._start_time
        if elapsed > RESOLVE_TIMEOUT:
            logger.warning(
                "FIGI resolution timed out after %.0fs — returning partial results",
                elapsed,
            )
            return True
        return False

    def _rate_limit(self) -> None:
        """Enforce rate limiting between API calls."""
        if self._api_key:
            return  # Higher limits with key
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            sleep_time = RATE_LIMIT_DELAY - elapsed
            logger.debug("Rate limiting: sleeping %.1fs", sleep_time)
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def _save_to_cache(self, row: pd.Series, result: FigiResult) -> None:
        """Save a resolved FIGI mapping to the database cache.

        Args:
            row: Original holdings row with identifiers.
            result: Resolved FIGI data.
        """
        if not result.composite_figi:
            return

        existing = self._session.query(FigiMapping).filter(
            FigiMapping.composite_figi == result.composite_figi
        ).first()
        if existing:
            return

        isin = row.get("holding_isin")
        cusip = row.get("holding_cusip")
        sedol = row.get("holding_sedol")

        mapping = FigiMapping(
            composite_figi=result.composite_figi,
            isin=isin if isinstance(isin, str) and isin.strip() else None,
            cusip=cusip if isinstance(cusip, str) and cusip.strip() else None,
            sedol=sedol if isinstance(sedol, str) and sedol.strip() else None,
            ticker=result.ticker,
            name=result.name,
            exchange=result.exchange,
            market_sector=result.market_sector,
        )
        self._session.add(mapping)
        self._session.commit()

    def _log_stats(self, total: int) -> None:
        """Log resolution statistics."""
        resolved = total - self.stats["unresolved"]
        pct = (resolved / total * 100) if total else 0
        logger.info(
            "Resolution complete: %d/%d (%.1f%%) — "
            "ISIN: %d, CUSIP: %d, SEDOL: %d, Ticker: %d, Cache: %d, Unresolved: %d",
            resolved, total, pct,
            self.stats["isin"], self.stats["cusip"],
            self.stats["sedol"], self.stats["ticker"],
            self.stats["cache"], self.stats["unresolved"],
        )

    def get_report(self, total: int) -> str:
        """Generate a human-readable resolution report."""
        resolved = total - self.stats["unresolved"]
        pct = (resolved / total * 100) if total else 0
        return (
            f"{total} holdings totali, {resolved} risolte ({pct:.1f}%):\n"
            f"  - ISIN:   {self.stats['isin']}\n"
            f"  - CUSIP:  {self.stats['cusip']}\n"
            f"  - SEDOL:  {self.stats['sedol']}\n"
            f"  - Ticker: {self.stats['ticker']}\n"
            f"  - Cache:  {self.stats['cache']}\n"
            f"  - Non risolte: {self.stats['unresolved']}"
        )
