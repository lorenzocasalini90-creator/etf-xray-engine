"""Base fetcher abstract class for ETF holdings ingestion."""

from abc import ABC, abstractmethod
from datetime import date

import pandas as pd

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


class BaseFetcher(ABC):
    """Abstract base class for all ETF holdings fetchers.

    Every fetcher must implement ``fetch_holdings`` and ``can_handle``.
    The base class provides schema validation so that all fetchers
    produce a uniform DataFrame.
    """

    @abstractmethod
    def can_handle(self, identifier: str) -> bool:
        """Return True if this fetcher knows how to handle *identifier*.

        Args:
            identifier: ETF ticker, ISIN, or other identifier string.
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
