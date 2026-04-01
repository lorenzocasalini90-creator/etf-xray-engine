"""Fetcher registry with auto-discovery of concrete fetcher classes."""

import importlib
import logging
import pkgutil
from typing import Type

from src.ingestion.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)


class FetcherRegistry:
    """Registry that auto-discovers and manages fetcher instances.

    On instantiation it scans the ``src.ingestion`` package for any
    concrete subclass of ``BaseFetcher`` and registers it.
    """

    def __init__(self) -> None:
        self._fetchers: list[BaseFetcher] = []
        self._discover()

    def _discover(self) -> None:
        """Import all modules in ``src.ingestion`` and register subclasses."""
        import src.ingestion as pkg

        for module_info in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
            importlib.import_module(module_info.name)

        for cls in BaseFetcher.__subclasses__():
            self.register(cls)

    def register(self, fetcher_cls: Type[BaseFetcher]) -> None:
        """Register a concrete fetcher class (instantiates it).

        Args:
            fetcher_cls: A concrete subclass of ``BaseFetcher``.
        """
        instance = fetcher_cls()
        if instance not in self._fetchers:
            self._fetchers.append(instance)

    def get_fetcher(self, identifier: str) -> BaseFetcher:
        """Return the fetcher with highest confidence for *identifier*.

        Args:
            identifier: ETF ticker, ISIN, or other identifier string.

        Returns:
            A ``BaseFetcher`` instance that can handle the identifier.

        Raises:
            ValueError: If no registered fetcher scores above 0.
        """
        best_score = 0.0
        best_fetcher: BaseFetcher | None = None
        for fetcher in self._fetchers:
            score = fetcher.can_handle(identifier)
            if score > best_score:
                best_score = score
                best_fetcher = fetcher

        if best_fetcher is not None and best_score > 0.0:
            logger.info(
                "Best fetcher for %s: %s (score=%.2f)",
                identifier, type(best_fetcher).__name__, best_score,
            )
            return best_fetcher

        raise ValueError(
            f"No registered fetcher can handle identifier: {identifier!r}"
        )

    def get_fetchers_ranked(self, identifier: str) -> list[tuple[BaseFetcher, float]]:
        """Return all fetchers sorted by descending confidence for *identifier*.

        Args:
            identifier: ETF ticker, ISIN, or other identifier string.

        Returns:
            List of (fetcher, score) tuples with score > 0, sorted descending.
        """
        scored = [
            (f, f.can_handle(identifier))
            for f in self._fetchers
        ]
        return sorted(
            [(f, s) for f, s in scored if s > 0.0],
            key=lambda x: x[1],
            reverse=True,
        )

    @property
    def fetchers(self) -> list[BaseFetcher]:
        """Return a copy of the registered fetchers list."""
        return list(self._fetchers)
