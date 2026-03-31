"""OpenFIGI identity resolution with cascading lookup strategy.

Resolves holdings to Composite FIGI identifiers using the OpenFIGI v3 API.
Cascade order: ISIN → CUSIP → SEDOL → Ticker+Exchange.
"""

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from src.storage.models import FigiMapping

# Load .env from project root
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logger = logging.getLogger(__name__)

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
BATCH_SIZE = 10  # max jobs per request without API key
RATE_LIMIT_DELAY = 12.0  # 5 req/min → 12s between requests
MAX_RETRIES = 3
BACKOFF_BASE = 2.0


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
        self._batch_size = 100 if api_key else BATCH_SIZE
        self._last_request_time: float = 0.0

        # Resolution statistics
        self.stats: dict[str, int] = {
            "isin": 0,
            "cusip": 0,
            "sedol": 0,
            "ticker": 0,
            "cache": 0,
            "unresolved": 0,
        }

    def resolve_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resolve all holdings in a DataFrame to Composite FIGIs.

        Adds a ``composite_figi`` column. Uses DB cache first, then
        calls OpenFIGI API for uncached identifiers.

        Args:
            df: Holdings DataFrame with standard schema columns.

        Returns:
            DataFrame with ``composite_figi`` column added.
        """
        self.stats = {k: 0 for k in self.stats}
        results: dict[int, str | None] = {}

        for idx, row in df.iterrows():
            figi = self._resolve_single(row)
            results[idx] = figi

        df = df.copy()
        df["composite_figi"] = df.index.map(results)

        self._log_stats(len(df))
        return df

    def resolve_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resolve holdings using batched API calls for efficiency.

        Groups unresolved identifiers by type and sends them in batches
        to the OpenFIGI API.

        Args:
            df: Holdings DataFrame with standard schema columns.

        Returns:
            DataFrame with ``composite_figi`` column added.
        """
        self.stats = {k: 0 for k in self.stats}
        df = df.copy()
        df["composite_figi"] = None
        df["_row_idx"] = range(len(df))

        # Step 1: Check cache for all rows
        for idx, row in df.iterrows():
            cached = self._check_cache(row)
            if cached:
                df.at[idx, "composite_figi"] = cached
                self.stats["cache"] += 1

        # Step 2: Cascade through identifier types for uncached rows
        unresolved_mask = df["composite_figi"].isna()
        cascade_steps = [
            ("isin", "ID_ISIN", "holding_isin"),
            ("cusip", "ID_CUSIP", "holding_cusip"),
            ("sedol", "ID_SEDOL", "holding_sedol"),
        ]

        for step_name, id_type, col in cascade_steps:
            if not unresolved_mask.any():
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
        if unresolved_mask.any():
            unresolved = df.loc[unresolved_mask]
            has_ticker = unresolved["holding_ticker"].notna() & (
                unresolved["holding_ticker"].astype(str).str.strip() != ""
            )
            candidates = unresolved.loc[has_ticker]

            if not candidates.empty:
                jobs = []
                job_indices = []
                for idx, row in candidates.iterrows():
                    job = {"idType": "TICKER", "idValue": str(row["holding_ticker"]).strip()}
                    jobs.append(job)
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
        df.drop(columns=["_row_idx"], inplace=True)
        self._log_stats(len(df))
        return df

    def _resolve_single(self, row: pd.Series) -> str | None:
        """Resolve a single holding row via cache then API cascade.

        Args:
            row: A single row from the holdings DataFrame.

        Returns:
            Composite FIGI string or None if unresolved.
        """
        cached = self._check_cache(row)
        if cached:
            self.stats["cache"] += 1
            return cached

        cascade = [
            ("isin", "ID_ISIN", row.get("holding_isin")),
            ("cusip", "ID_CUSIP", row.get("holding_cusip")),
            ("sedol", "ID_SEDOL", row.get("holding_sedol")),
        ]

        for step_name, id_type, id_value in cascade:
            if not id_value or (isinstance(id_value, str) and not id_value.strip()):
                continue
            result = self._query_openfigi(id_type, str(id_value).strip())
            if result:
                self._save_to_cache(row, result)
                self.stats[step_name] += 1
                return result.composite_figi

        # Ticker fallback
        ticker = row.get("holding_ticker")
        if ticker and isinstance(ticker, str) and ticker.strip():
            result = self._query_openfigi("TICKER", ticker.strip())
            if result:
                self._save_to_cache(row, result)
                self.stats["ticker"] += 1
                return result.composite_figi

        self.stats["unresolved"] += 1
        return None

    def _check_cache(self, row: pd.Series) -> str | None:
        """Check DB cache for an existing FIGI mapping.

        Args:
            row: Holdings row with identifier columns.

        Returns:
            Composite FIGI if found in cache, else None.
        """
        isin = row.get("holding_isin")
        if isin and isinstance(isin, str) and isin.strip():
            mapping = self._session.query(FigiMapping).filter(
                FigiMapping.isin == isin.strip()
            ).first()
            if mapping:
                return mapping.composite_figi

        cusip = row.get("holding_cusip")
        if cusip and isinstance(cusip, str) and cusip.strip():
            mapping = self._session.query(FigiMapping).filter(
                FigiMapping.cusip == cusip.strip()
            ).first()
            if mapping:
                return mapping.composite_figi

        sedol = row.get("holding_sedol")
        if sedol and isinstance(sedol, str) and sedol.strip():
            mapping = self._session.query(FigiMapping).filter(
                FigiMapping.sedol == sedol.strip()
            ).first()
            if mapping:
                return mapping.composite_figi

        return None

    def _query_openfigi(self, id_type: str, id_value: str) -> FigiResult | None:
        """Send a single-job query to the OpenFIGI API.

        Args:
            id_type: OpenFIGI identifier type (e.g. "ID_ISIN").
            id_value: The identifier value.

        Returns:
            FigiResult if resolved, else None.
        """
        results = self._send_batches([{"idType": id_type, "idValue": id_value}])
        return results[0] if results else None

    def _send_batches(self, jobs: list[dict]) -> list[FigiResult | None]:
        """Send jobs to OpenFIGI in batches, respecting rate limits.

        Args:
            jobs: List of OpenFIGI job dicts.

        Returns:
            List of FigiResult (or None) in same order as jobs.
        """
        all_results: list[FigiResult | None] = []

        for i in range(0, len(jobs), self._batch_size):
            batch = jobs[i:i + self._batch_size]
            self._rate_limit()
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
        """Log resolution statistics.

        Args:
            total: Total number of holdings processed.
        """
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
        """Generate a human-readable resolution report.

        Args:
            total: Total number of holdings.

        Returns:
            Formatted report string.
        """
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
