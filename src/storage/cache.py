"""Holdings cache layer backed by SQLite.

Caches fetched ETF holdings to avoid repeated HTTP calls. Holdings
change at most monthly, so a 7-day TTL is a safe default.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import select, delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.ingestion.base_fetcher import FetchResult
from src.storage.models import HoldingsCache

logger = logging.getLogger(__name__)

CACHE_TTL_DAYS = 7


class HoldingsCacheManager:
    """Read/write cache for ETF holdings DataFrames.

    Args:
        session_factory: SQLAlchemy ``sessionmaker`` bound to an engine.
    """

    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, identifier: str) -> FetchResult | None:
        """Return cached holdings if cache is fresh (< TTL).

        Args:
            identifier: ETF ticker or ISIN.

        Returns:
            ``FetchResult`` with ``status="cached"`` or ``None``.
        """
        entry = self._load(identifier)
        if entry is None:
            return None

        now = datetime.now(timezone.utc)
        stale_after = entry.stale_after
        if stale_after.tzinfo is None:
            stale_after = stale_after.replace(tzinfo=timezone.utc)

        if now > stale_after:
            return None

        df = pd.read_json(io.StringIO(entry.holdings_json), orient="split")
        fetched_str = entry.fetched_at.strftime("%Y-%m-%d %H:%M")
        return FetchResult(
            status="cached",
            holdings=df,
            message=f"Dati aggiornati al {fetched_str}",
            coverage_pct=entry.coverage_pct,
            source=entry.source,
        )

    def get_stale(self, identifier: str) -> FetchResult | None:
        """Return cached holdings even if stale (for fallback).

        Args:
            identifier: ETF ticker or ISIN.

        Returns:
            ``FetchResult`` with ``status="cached"`` or ``None``.
        """
        entry = self._load(identifier)
        if entry is None:
            return None

        df = pd.read_json(io.StringIO(entry.holdings_json), orient="split")
        fetched_str = entry.fetched_at.strftime("%Y-%m-%d %H:%M")
        return FetchResult(
            status="cached",
            holdings=df,
            message=f"Usando dati del {fetched_str} (aggiornamento fallito)",
            coverage_pct=entry.coverage_pct,
            source=entry.source,
        )

    def set(
        self,
        identifier: str,
        df: pd.DataFrame,
        source: str,
        coverage_pct: float,
        status: str = "success",
    ) -> None:
        """Save holdings to cache.

        Args:
            identifier: ETF ticker or ISIN.
            df: Holdings DataFrame.
            source: Fetcher name that produced the data.
            coverage_pct: Coverage percentage.
            status: Fetch status (``success`` or ``partial``).
        """
        now = datetime.now(timezone.utc)
        holdings_json = df.to_json(orient="split", date_format="iso")

        clean_id = identifier.upper().strip()
        stale_after = now + timedelta(days=CACHE_TTL_DAYS)
        values = dict(
            source=source,
            holdings_json=holdings_json,
            fetched_at=now,
            stale_after=stale_after,
            coverage_pct=coverage_pct,
            num_holdings=len(df),
            status=status,
        )

        with self._session_factory() as session:
            entry = session.execute(
                select(HoldingsCache).where(
                    HoldingsCache.etf_identifier == clean_id
                )
            ).scalar_one_or_none()

            if entry:
                for k, v in values.items():
                    setattr(entry, k, v)
                session.commit()
            else:
                try:
                    new_entry = HoldingsCache(etf_identifier=clean_id, **values)
                    session.add(new_entry)
                    session.commit()
                except IntegrityError:
                    # Another thread inserted first — update instead
                    session.rollback()
                    session.execute(
                        update(HoldingsCache)
                        .where(HoldingsCache.etf_identifier == clean_id)
                        .values(**values)
                    )
                    session.commit()

    def is_fresh(self, identifier: str) -> bool:
        """Return True if cache for *identifier* is valid (< TTL).

        Args:
            identifier: ETF ticker or ISIN.
        """
        entry = self._load(identifier)
        if entry is None:
            return False

        now = datetime.now(timezone.utc)
        stale_after = entry.stale_after
        if stale_after.tzinfo is None:
            stale_after = stale_after.replace(tzinfo=timezone.utc)

        return now <= stale_after

    def clear(self, identifier: str | None = None) -> int:
        """Clear cache entries.

        Args:
            identifier: If provided, clear only this ETF. If ``None``,
                clear all entries.

        Returns:
            Number of entries deleted.
        """
        with self._session_factory() as session:
            if identifier:
                stmt = delete(HoldingsCache).where(
                    HoldingsCache.etf_identifier == identifier.upper().strip()
                )
            else:
                stmt = delete(HoldingsCache)
            result = session.execute(stmt)
            session.commit()
            return result.rowcount

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _load(self, identifier: str) -> HoldingsCache | None:
        """Load cache entry from DB.

        Args:
            identifier: ETF ticker or ISIN.

        Returns:
            ``HoldingsCache`` ORM instance or ``None``.
        """
        with self._session_factory() as session:
            return session.execute(
                select(HoldingsCache).where(
                    HoldingsCache.etf_identifier == identifier.upper().strip()
                )
            ).scalar_one_or_none()
