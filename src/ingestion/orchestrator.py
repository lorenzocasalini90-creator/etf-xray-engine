"""Fetch orchestrator — coordinates holdings retrieval with cascade fallback."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import date

from src.ingestion.base_fetcher import BaseFetcher, FetchResult
from src.ingestion.registry import FetcherRegistry
from src.storage.cache import HoldingsCacheManager

logger = logging.getLogger(__name__)

METADATA_TIMEOUT = 30  # max seconds for resolve_metadata before giving up


@dataclass
class ETFMetadata:
    """Metadata resolved for an ETF identifier.

    Attributes:
        isin: ISIN code (12 chars) or ``None``.
        issuer: Issuer name (e.g. ``iShares``, ``Vanguard``) or ``None``.
        name: Human-readable fund name or ``None``.
        ter: Total Expense Ratio as a percentage, or ``None``.
    """

    isin: str | None = None
    issuer: str | None = None
    name: str | None = None
    ter: float | None = None


class _MetadataTimeout(Exception):
    """Raised when metadata resolution exceeds the allowed time."""


def resolve_metadata(identifier: str) -> ETFMetadata | None:
    """Resolve ETF metadata via JustETFFetcher (if justetf-scraping installed).

    Uses ``JustETFFetcher.get_metadata()`` which calls ``get_etf_overview``
    to obtain ISIN, issuer name, TER, fund size for any UCITS ETF.
    Times out after ``METADATA_TIMEOUT`` seconds to prevent hanging.

    Args:
        identifier: ETF ticker, ISIN, or name.

    Returns:
        ``ETFMetadata`` with available fields, or ``None`` if resolution fails.
    """
    try:
        from src.ingestion.justetf import JustETFFetcher

        fetcher = JustETFFetcher()
        if not fetcher._available:
            return None

        # Use ThreadPoolExecutor for timeout (signal.alarm doesn't work in threads)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(fetcher.get_metadata, identifier)
            try:
                meta = future.result(timeout=METADATA_TIMEOUT)
            except FuturesTimeoutError:
                raise _MetadataTimeout("Metadata resolution timed out")

        if meta is None:
            return None

        return ETFMetadata(
            isin=meta.isin,
            issuer=meta.issuer,
            name=meta.name,
            ter=meta.ter,
        )
    except _MetadataTimeout:
        logger.warning(
            "Metadata resolution timed out after %ds for %s",
            METADATA_TIMEOUT, identifier,
        )
        return None
    except Exception as exc:
        logger.warning("Metadata resolution failed for %s: %s", identifier, exc)
        return None


# Maps issuer name (lowercase) → fetcher class name for prioritisation
_ISSUER_FETCHER_MAP: dict[str, str] = {
    "ishares": "ISharesFetcher",
    "blackrock": "ISharesFetcher",
    "vanguard": "VanguardFetcher",
    "amundi": "AmundiFetcher",
    "xtrackers": "XtrackersFetcher",
    "dws": "XtrackersFetcher",
    "invesco": "InvescoFetcher",
    "spdr": "SPDRFetcher",
    "state street": "SPDRFetcher",
    "ssga": "SPDRFetcher",
    "vaneck": "VanEckFetcher",
    "van eck": "VanEckFetcher",
    "ubs": "UBSFetcher",
    "l&g": "LGIMFetcher",
    "lgim": "LGIMFetcher",
    "legal & general": "LGIMFetcher",
    "legal and general": "LGIMFetcher",
}


class FetchOrchestrator:
    """Orchestrates ETF holdings fetch with cascade fallback and caching.

    Flow:
    0. Check cache — if fresh, return immediately.
    1. Resolve metadata via justetf-scraping (optional).
    2. If issuer is known, try the matching fetcher first.
    3. Try all registered fetchers ranked by confidence (brute force).
    4. Fall back to JustETF top-10 partial holdings.
    5. If live fetch fails but stale cache exists, return stale data.
    6. Return clear error if everything fails.

    Args:
        registry: Optional pre-built ``FetcherRegistry``. If ``None``,
            a new one is created with auto-discovery.
        cache: Optional ``HoldingsCacheManager``. If ``None``, caching
            is disabled.
    """

    def __init__(
        self,
        registry: FetcherRegistry | None = None,
        cache: HoldingsCacheManager | None = None,
    ) -> None:
        self._registry = registry or FetcherRegistry()
        self._cache = cache

    def fetch(
        self,
        identifier: str,
        as_of_date: date | None = None,
        force_refresh: bool = False,
    ) -> FetchResult:
        """Fetch holdings for *identifier* with cache and cascade fallback.

        Args:
            identifier: ETF ticker, ISIN, or name.
            as_of_date: Optional reference date.
            force_refresh: If True, bypass cache and fetch live.

        Returns:
            ``FetchResult`` with the best available data.
        """
        identifier = identifier.strip()
        if not identifier:
            return FetchResult(
                status="failed",
                message="Empty identifier provided.",
                source="FetchOrchestrator",
            )

        # Step 0: check fresh cache
        if self._cache and not force_refresh:
            cached = self._cache.get(identifier)
            if cached:
                logger.info("Cache hit for %s", identifier)
                return cached

        # Step 1: resolve metadata
        metadata = resolve_metadata(identifier)
        lookup_id = identifier
        if metadata and metadata.isin:
            logger.info(
                "Resolved %s → ISIN=%s issuer=%s",
                identifier, metadata.isin, metadata.issuer,
            )
            lookup_id = metadata.isin

            # Also check cache for resolved ISIN
            if self._cache and not force_refresh and lookup_id != identifier:
                cached = self._cache.get(lookup_id)
                if cached:
                    logger.info("Cache hit for resolved ISIN %s", lookup_id)
                    return cached

        # Step 2: if issuer is known, try the specific fetcher first
        result = self._try_live_fetch(identifier, lookup_id, metadata, as_of_date)
        if result and result.status != "failed":
            self._save_to_cache(identifier, result)
            return result

        # Step 5: live fetch failed — try stale cache as fallback
        # (skip if force_refresh — user explicitly wants fresh data)
        if self._cache and not force_refresh:
            stale = self._cache.get_stale(identifier)
            if stale is None and lookup_id != identifier:
                stale = self._cache.get_stale(lookup_id)
            if stale:
                logger.info("Using stale cache for %s", identifier)
                return stale

        # Step 6: everything failed
        return FetchResult(
            status="failed",
            message=(
                f"Could not fetch holdings for '{identifier}'. "
                f"Tried all registered fetchers and JustETF fallback. "
                f"Check that the identifier is valid and the ETF is supported."
            ),
            source="FetchOrchestrator",
        )

    @staticmethod
    def _looks_like_ie_isin(identifier: str) -> bool:
        """Check if identifier looks like an Irish-domiciled ISIN."""
        clean = identifier.strip().upper()
        return len(clean) == 12 and clean.startswith("IE") and clean.isalnum()

    def _try_live_fetch(
        self,
        identifier: str,
        lookup_id: str,
        metadata: ETFMetadata | None,
        as_of_date: date | None,
    ) -> FetchResult | None:
        """Try all live fetch strategies in cascade order.

        Order:
        1. Issuer-specific fetcher (from metadata).
        2. For IE ISINs without metadata: try iShares directly.
        3. Brute force all fetchers ranked by confidence.
        4. JustETF top-10 fallback (always last).

        Args:
            identifier: Original user identifier.
            lookup_id: Resolved ISIN or original identifier.
            metadata: Resolved metadata, if any.
            as_of_date: Optional reference date.

        Returns:
            Successful ``FetchResult`` or ``None``.
        """
        # Issuer-specific
        if metadata and metadata.issuer:
            result = self._try_issuer_fetcher(
                metadata.issuer, lookup_id, as_of_date
            )
            if result and result.status != "failed":
                return result

        # IE ISIN fast path: try iShares directly when metadata is missing
        if not (metadata and metadata.issuer) and self._looks_like_ie_isin(lookup_id):
            logger.info("IE ISIN detected without issuer info — trying iShares directly")
            result = self._try_issuer_fetcher("ishares", lookup_id, as_of_date)
            if result and result.status != "failed":
                return result

        # Brute force all fetchers (excluding JustETF, which scores 0.1)
        result = self._try_all_fetchers(lookup_id, as_of_date)
        if result and result.status != "failed":
            return result

        if lookup_id != identifier:
            result = self._try_all_fetchers(identifier, as_of_date)
            if result and result.status != "failed":
                return result

        # JustETF fallback — always last resort
        result = self._try_justetf_fallback(lookup_id, metadata)
        if result and result.status != "failed":
            return result

        return None

    def _save_to_cache(self, identifier: str, result: FetchResult) -> None:
        """Save a successful fetch result to cache.

        Args:
            identifier: ETF identifier used as cache key.
            result: Successful ``FetchResult``.
        """
        if self._cache and result.holdings is not None:
            self._cache.set(
                identifier=identifier,
                df=result.holdings,
                source=result.source,
                coverage_pct=result.coverage_pct,
                status=result.status,
            )

    def _try_issuer_fetcher(
        self,
        issuer: str,
        identifier: str,
        as_of_date: date | None,
    ) -> FetchResult | None:
        """Try the fetcher matching *issuer*.

        Args:
            issuer: Issuer name from metadata.
            identifier: ETF identifier to fetch.
            as_of_date: Optional reference date.

        Returns:
            ``FetchResult`` or ``None`` if no matching fetcher.
        """
        target_cls = _ISSUER_FETCHER_MAP.get(issuer.lower())
        if not target_cls:
            return None

        for fetcher in self._registry.fetchers:
            if type(fetcher).__name__ == target_cls:
                logger.info(
                    "Trying %s for issuer %s", target_cls, issuer,
                )
                return fetcher.try_fetch(identifier, as_of_date)
        return None

    def _try_all_fetchers(
        self,
        identifier: str,
        as_of_date: date | None,
    ) -> FetchResult | None:
        """Try all registered fetchers ranked by confidence score.

        Args:
            identifier: ETF identifier to fetch.
            as_of_date: Optional reference date.

        Returns:
            First successful ``FetchResult``, or the last failed one,
            or ``None`` if no fetchers scored > 0.
        """
        ranked = self._registry.get_fetchers_ranked(identifier)
        if not ranked:
            return None

        last_result: FetchResult | None = None
        for fetcher, score in ranked:
            logger.info(
                "Trying %s (score=%.2f) for %s",
                type(fetcher).__name__, score, identifier,
            )
            result = fetcher.try_fetch(identifier, as_of_date)
            if result.status != "failed":
                return result
            last_result = result

        return last_result

    @staticmethod
    def _try_justetf_fallback(
        identifier: str,
        metadata: ETFMetadata | None,
    ) -> FetchResult | None:
        """Try JustETF top-10 holdings as a partial fallback.

        Delegates to ``JustETFFetcher.try_fetch()`` which returns
        ``status="partial"`` on success.

        Args:
            identifier: ETF identifier (preferably ISIN).
            metadata: Previously resolved metadata, if any.

        Returns:
            ``FetchResult`` with status ``partial``, or ``None``.
        """
        try:
            from src.ingestion.justetf import JustETFFetcher

            fetcher = JustETFFetcher()
            if not fetcher._available:
                return None

            isin = (metadata.isin if metadata else None) or identifier
            result = fetcher.try_fetch(isin)
            if result.status != "failed":
                return result
            return None
        except Exception as exc:
            logger.warning("JustETF fallback failed for %s: %s", identifier, exc)
            return None
