"""Base fetcher abstract class for ETF holdings ingestion."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

HOLDINGS_SCHEMA: list[str] = [
    "etf_ticker",
    "holding_name",
    "holding_isin",
    "holding_ticker",
    "holding_sedol",
    "holding_cusip",
    "weight_pct",
    "market_value",
    "shares",
    "sector",
    "country",
    "currency",
    "as_of_date",
]


class SchemaValidationError(Exception):
    """Raised when fetcher output does not match the expected schema."""


@dataclass
class FetchResult:
    """Result of a fetch attempt.

    Attributes:
        status: One of ``success``, ``cached``, ``partial``, ``failed``.
        holdings: Holdings DataFrame or ``None`` on failure.
        message: User-friendly description of the outcome.
        coverage_pct: Percentage of holdings resolved (0.0–100.0).
        source: Name of the fetcher or data source that produced the result.
    """

    status: str
    holdings: pd.DataFrame | None = field(default=None, repr=False)
    message: str = ""
    coverage_pct: float = 0.0
    source: str = ""


class BaseFetcher(ABC):
    """Abstract base class for all ETF holdings fetchers.

    Every fetcher must implement ``fetch_holdings`` and ``can_handle``.
    The base class provides schema validation so that all fetchers
    produce a uniform DataFrame.
    """

    @abstractmethod
    def can_handle(self, identifier: str) -> float:
        """Return a confidence score (0.0–1.0) for handling *identifier*.

        Args:
            identifier: ETF ticker, ISIN, or other identifier string.

        Returns:
            0.0 means cannot handle; 1.0 means certain match.
        """
        ...

    @abstractmethod
    def fetch_holdings(
        self, identifier: str, as_of_date: date | None = None
    ) -> pd.DataFrame:
        """Fetch holdings for the given ETF identifier.

        Args:
            identifier: ETF ticker, ISIN, or other identifier string.
            as_of_date: Optional reference date. ``None`` means latest.

        Returns:
            DataFrame with columns matching ``HOLDINGS_SCHEMA``.
        """
        ...

    def try_fetch(
        self, identifier: str, as_of_date: date | None = None
    ) -> FetchResult:
        """Wrap ``fetch_holdings`` in try/except and return a ``FetchResult``.

        Args:
            identifier: ETF ticker, ISIN, or other identifier string.
            as_of_date: Optional reference date.

        Returns:
            ``FetchResult`` with status ``success`` or ``failed``.
        """
        source = type(self).__name__
        try:
            df = self.fetch_holdings(identifier, as_of_date)
            df = self.validate_output(df)
            has_isin = df["holding_isin"].notna()
            has_ticker = df["holding_ticker"].notna() if "holding_ticker" in df.columns else pd.Series(False, index=df.index)
            n_resolved = (has_isin | has_ticker).sum()
            coverage = (n_resolved / len(df) * 100) if len(df) > 0 else 0.0
            return FetchResult(
                status="success",
                holdings=df,
                message=f"{source} fetched {len(df)} holdings for {identifier}",
                coverage_pct=coverage,
                source=source,
            )
        except Exception as exc:
            logger.warning("%s failed for %s: %s", source, identifier, exc)
            return FetchResult(
                status="failed",
                holdings=None,
                message=f"{source} failed for {identifier}: {exc}",
                coverage_pct=0.0,
                source=source,
            )

    def validate_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate that *df* conforms to the standard holdings schema.

        Missing columns are added with ``None``; extra columns are dropped.
        Raises ``SchemaValidationError`` if the DataFrame is empty.

        Args:
            df: Raw DataFrame returned by a concrete fetcher.

        Returns:
            Validated DataFrame with exactly the schema columns.
        """
        if df.empty:
            raise SchemaValidationError("Fetcher returned an empty DataFrame")

        missing = set(HOLDINGS_SCHEMA) - set(df.columns)
        for col in missing:
            df[col] = None

        return df[HOLDINGS_SCHEMA]
