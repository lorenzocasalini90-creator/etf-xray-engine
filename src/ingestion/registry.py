"""Fetcher registry with auto-discovery of concrete fetcher classes."""

import importlib
import pkgutil
from typing import Type

from src.ingestion.base_fetcher import BaseFetcher


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
        """Return the first fetcher that can handle *identifier*.

        Args:
            identifier: ETF ticker, ISIN, or other identifier string.

        Returns:
            A ``BaseFetcher`` instance that can handle the identifier.

        Raises:
            ValueError: If no registered fetcher can handle the identifier.
        """
        for fetcher in self._fetchers:
            if fetcher.can_handle(identifier):
                return fetcher
        raise ValueError(
            f"No registered fetcher can handle identifier: {identifier!r}"
        )

    @property
    def fetchers(self) -> list[BaseFetcher]:
        """Return a copy of the registered fetchers list."""
        return list(self._fetchers)
