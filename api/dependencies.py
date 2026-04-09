"""Shared dependencies for API routes."""

from functools import lru_cache

from src.ingestion.orchestrator import FetchOrchestrator
from src.storage.cache import HoldingsCacheManager
from src.storage.db import get_session_factory, init_db


@lru_cache(maxsize=1)
def _init() -> None:
    """Initialize DB tables once."""
    init_db()


@lru_cache(maxsize=1)
def get_session_factory_cached():
    """Return a thread-safe sessionmaker singleton."""
    _init()
    return get_session_factory()


def get_db():
    """Yield a DB session, closing it after use."""
    factory = get_session_factory_cached()
    session = factory()
    try:
        yield session
    finally:
        session.close()


@lru_cache(maxsize=1)
def get_orchestrator() -> FetchOrchestrator:
    """Return a singleton FetchOrchestrator."""
    _init()
    factory = get_session_factory_cached()
    cache_manager = HoldingsCacheManager(factory)
    return FetchOrchestrator(cache=cache_manager)
