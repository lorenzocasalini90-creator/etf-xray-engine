"""Tests for the FIGI resolver, normalizer, and resolution pipeline."""

import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.resolution.figi_resolver import FigiResolver, FigiResult, RATE_LIMIT_DELAY
from src.resolution.normalizer import deduplicate_holdings, normalize_isin, normalize_name
from src.storage.models import Base, FigiMapping

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_session():
    """In-memory SQLite session for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


@pytest.fixture
def resolver(db_session):
    """FigiResolver with mocked HTTP to avoid real API calls."""
    r = FigiResolver(db_session)
    return r


SAMPLE_HOLDINGS = pd.DataFrame({
    "holding_name": ["APPLE INC", "MICROSOFT CORP", "UNKNOWN CO"],
    "holding_isin": ["US0378331005", "US5949181045", None],
    "holding_cusip": ["037833100", "594918104", "999999999"],
    "holding_sedol": ["2046251", "2588173", None],
    "holding_ticker": ["AAPL", "MSFT", "XYZZ"],
    "weight_pct": [6.5, 5.0, 0.1],
    "market_value": [46000000, 34000000, 100000],
    "shares": [188000, 95000, 500],
    "sector": ["Technology", "Technology", "Unknown"],
    "country": ["US", "US", "US"],
    "currency": ["USD", "USD", "USD"],
    "etf_ticker": ["CSPX", "CSPX", "CSPX"],
    "as_of_date": ["2026-03-28", "2026-03-28", "2026-03-28"],
})


def _mock_openfigi_response(jobs):
    """Generate mock OpenFIGI responses for given jobs."""
    responses = []
    known = {
        "US0378331005": {"compositeFIGI": "BBG000B9XRY4", "name": "APPLE INC", "ticker": "AAPL", "exchCode": "US", "securityType": "Common Stock", "marketSectorDes": "Equity"},
        "US5949181045": {"compositeFIGI": "BBG000BPH459", "name": "MICROSOFT CORP", "ticker": "MSFT", "exchCode": "US", "securityType": "Common Stock", "marketSectorDes": "Equity"},
        "037833100": {"compositeFIGI": "BBG000B9XRY4", "name": "APPLE INC", "ticker": "AAPL", "exchCode": "US", "securityType": "Common Stock", "marketSectorDes": "Equity"},
        "594918104": {"compositeFIGI": "BBG000BPH459", "name": "MICROSOFT CORP", "ticker": "MSFT", "exchCode": "US", "securityType": "Common Stock", "marketSectorDes": "Equity"},
        "2046251": {"compositeFIGI": "BBG000B9XRY4", "name": "APPLE INC", "ticker": "AAPL", "exchCode": "US", "securityType": "Common Stock", "marketSectorDes": "Equity"},
        "2588173": {"compositeFIGI": "BBG000BPH459", "name": "MICROSOFT CORP", "ticker": "MSFT", "exchCode": "US", "securityType": "Common Stock", "marketSectorDes": "Equity"},
    }
    for job in jobs:
        id_value = job.get("idValue", "")
        if id_value in known:
            responses.append({"data": [known[id_value]]})
        else:
            responses.append({"warning": "No identifier found."})
    return responses


# ---------------------------------------------------------------------------
# Normalizer tests
# ---------------------------------------------------------------------------

class TestNormalizeName:
    def test_uppercase_and_strip(self):
        assert normalize_name("  apple inc  ") == "APPLE"

    def test_remove_corp(self):
        assert normalize_name("Microsoft Corp") == "MICROSOFT"

    def test_remove_plc(self):
        assert normalize_name("HSBC Holdings PLC") == "HSBC HOLDINGS"

    def test_remove_ltd(self):
        assert normalize_name("Samsung Electronics Ltd") == "SAMSUNG ELECTRONICS"

    def test_remove_ag(self):
        assert normalize_name("Siemens AG") == "SIEMENS"

    def test_empty_input(self):
        assert normalize_name("") == ""
        assert normalize_name(None) == ""

    def test_collapse_whitespace(self):
        assert normalize_name("  BANK  OF  AMERICA  CORP  ") == "BANK OF AMERICA"


class TestNormalizeIsin:
    def test_valid_isin(self):
        assert normalize_isin("US0378331005") == "US0378331005"

    def test_lowercase_to_upper(self):
        assert normalize_isin("us0378331005") == "US0378331005"

    def test_with_whitespace(self):
        assert normalize_isin("  US0378331005  ") == "US0378331005"

    def test_invalid_format(self):
        assert normalize_isin("12345") is None
        assert normalize_isin("XYZABC") is None

    def test_empty(self):
        assert normalize_isin("") is None
        assert normalize_isin(None) is None


class TestDeduplicateHoldings:
    def test_merges_same_figi(self):
        df = pd.DataFrame({
            "composite_figi": ["BBG000B9XRY4", "BBG000B9XRY4", "BBG000BPH459"],
            "holding_name": ["APPLE A", "APPLE B", "MICROSOFT"],
            "weight_pct": [3.0, 3.5, 5.0],
            "market_value": [100, 200, 300],
            "shares": [10, 20, 30],
        })
        result = deduplicate_holdings(df)
        assert len(result) == 2
        apple = result[result["composite_figi"] == "BBG000B9XRY4"].iloc[0]
        assert apple["weight_pct"] == 6.5
        assert apple["shares"] == 30

    def test_preserves_no_figi_rows(self):
        df = pd.DataFrame({
            "composite_figi": [None, None, "BBG000B9XRY4"],
            "holding_name": ["A", "B", "C"],
            "weight_pct": [1.0, 2.0, 3.0],
            "market_value": [10, 20, 30],
            "shares": [1, 2, 3],
        })
        result = deduplicate_holdings(df)
        assert len(result) == 3

    def test_no_figi_column(self):
        df = pd.DataFrame({"holding_name": ["A"], "weight_pct": [1.0]})
        result = deduplicate_holdings(df)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# FigiResolver tests
# ---------------------------------------------------------------------------

class TestFigiResolverCascade:
    """Test that the resolver cascades through identifier types."""

    def test_isin_resolves_first(self, db_session):
        """If ISIN works, CUSIP and SEDOL are not tried."""
        resolver = FigiResolver(db_session)

        call_count = 0
        def mock_post(url, json=None, timeout=None):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = _mock_openfigi_response(json)
            return resp

        resolver._http.post = mock_post
        resolver._last_request_time = 0

        df = SAMPLE_HOLDINGS.iloc[[0]].copy()  # AAPL only
        result = resolver.resolve_batch(df)
        assert result["composite_figi"].iloc[0] == "BBG000B9XRY4"
        assert resolver.stats["isin"] == 1
        assert resolver.stats["cusip"] == 0

    def test_cusip_fallback(self, db_session):
        """If ISIN fails, CUSIP is tried next."""
        resolver = FigiResolver(db_session)

        def mock_post(url, json=None, timeout=None):
            resp = MagicMock()
            resp.status_code = 200
            results = []
            for job in json:
                if job["idType"] == "ID_ISIN":
                    results.append({"warning": "No identifier found."})
                else:
                    results.append(_mock_openfigi_response([job])[0])
            resp.json.return_value = results
            return resp

        resolver._http.post = mock_post
        resolver._last_request_time = 0

        df = SAMPLE_HOLDINGS.iloc[[0]].copy()
        result = resolver.resolve_batch(df)
        assert result["composite_figi"].iloc[0] == "BBG000B9XRY4"
        assert resolver.stats["isin"] == 0
        assert resolver.stats["cusip"] == 1

    def test_sedol_fallback(self, db_session):
        """If ISIN and CUSIP fail, SEDOL is tried."""
        resolver = FigiResolver(db_session)

        def mock_post(url, json=None, timeout=None):
            resp = MagicMock()
            resp.status_code = 200
            results = []
            for job in json:
                if job["idType"] in ("ID_ISIN", "ID_CUSIP"):
                    results.append({"warning": "No identifier found."})
                else:
                    results.append(_mock_openfigi_response([job])[0])
            resp.json.return_value = results
            return resp

        resolver._http.post = mock_post
        resolver._last_request_time = 0

        df = SAMPLE_HOLDINGS.iloc[[0]].copy()
        result = resolver.resolve_batch(df)
        assert result["composite_figi"].iloc[0] == "BBG000B9XRY4"
        assert resolver.stats["sedol"] == 1


class TestFigiResolverCache:
    """Test DB cache behavior."""

    def test_cache_hit_skips_api(self, db_session):
        """Second resolution uses cache, no HTTP calls."""
        # Pre-populate cache
        db_session.add(FigiMapping(
            composite_figi="BBG000B9XRY4",
            isin="US0378331005",
            ticker="AAPL",
            name="APPLE INC",
        ))
        db_session.commit()

        resolver = FigiResolver(db_session)
        call_count = 0
        original_post = resolver._http.post

        def counting_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return original_post(*args, **kwargs)

        resolver._http.post = counting_post

        df = SAMPLE_HOLDINGS.iloc[[0]].copy()  # AAPL
        result = resolver.resolve_batch(df)
        assert result["composite_figi"].iloc[0] == "BBG000B9XRY4"
        assert call_count == 0
        assert resolver.stats["cache"] == 1

    def test_resolved_data_saved_to_cache(self, db_session):
        """After resolution, FIGI mapping is in the DB."""
        resolver = FigiResolver(db_session)

        def mock_post(url, json=None, timeout=None):
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = _mock_openfigi_response(json)
            return resp

        resolver._http.post = mock_post
        resolver._last_request_time = 0

        df = SAMPLE_HOLDINGS.iloc[[0]].copy()
        resolver.resolve_batch(df)

        cached = db_session.query(FigiMapping).filter(
            FigiMapping.composite_figi == "BBG000B9XRY4"
        ).first()
        assert cached is not None
        assert cached.isin == "US0378331005"


class TestFigiResolverBatching:
    """Test batch grouping logic."""

    def test_batches_respect_size_limit(self, db_session):
        """Jobs are sent in groups of BATCH_SIZE."""
        resolver = FigiResolver(db_session)
        batch_sizes = []

        def mock_post(url, json=None, timeout=None):
            batch_sizes.append(len(json))
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = [{"warning": "No identifier found."}] * len(json)
            return resp

        resolver._http.post = mock_post
        resolver._last_request_time = 0

        # Create 25 holdings with only ISINs (no other identifiers)
        rows = []
        for i in range(25):
            rows.append({
                "holding_name": f"STOCK_{i}",
                "holding_isin": f"US{i:010d}",
                "holding_cusip": None,
                "holding_sedol": None,
                "holding_ticker": None,
                "weight_pct": 0.1,
                "market_value": 1000,
                "shares": 10,
                "sector": "Test",
                "country": "US",
                "currency": "USD",
                "etf_ticker": "TEST",
                "as_of_date": "2026-03-28",
            })
        df = pd.DataFrame(rows)
        resolver.resolve_batch(df)

        # BATCH_SIZE=100 → 25 ISINs = 1 batch of 25
        assert batch_sizes == [25]


class TestFigiResolverRateLimit:
    """Test rate limiting between API calls."""

    def test_respects_rate_limit_delay(self, db_session):
        """Consecutive API calls are spaced by at least RATE_LIMIT_DELAY."""
        resolver = FigiResolver(db_session)
        timestamps = []

        def mock_post(url, json=None, timeout=None):
            timestamps.append(time.time())
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = [{"warning": "No identifier found."}] * len(json)
            return resp

        resolver._http.post = mock_post
        resolver._last_request_time = 0

        # 2 batches → need >100 items to split, use 150
        rows = []
        for i in range(150):
            rows.append({
                "holding_name": f"STOCK_{i}",
                "holding_isin": f"US{i:010d}",
                "holding_cusip": None,
                "holding_sedol": None,
                "holding_ticker": None,
                "weight_pct": 0.1,
                "market_value": 1000,
                "shares": 10,
                "sector": "Test",
                "country": "US",
                "currency": "USD",
                "etf_ticker": "TEST",
                "as_of_date": "2026-03-28",
            })
        df = pd.DataFrame(rows)

        # Patch the delay to be tiny for fast tests
        with patch("src.resolution.figi_resolver.RATE_LIMIT_DELAY", 0.1):
            resolver.resolve_batch(df)

        assert len(timestamps) == 2
        if len(timestamps) >= 2:
            gap = timestamps[1] - timestamps[0]
            assert gap >= 0.05  # at least some delay enforced


class TestFigiResolverRetry:
    """Test retry behavior on API errors."""

    def test_retries_on_failure(self, db_session):
        """Retries up to MAX_RETRIES on network errors."""
        resolver = FigiResolver(db_session)
        attempt_count = 0

        def mock_post(url, json=None, timeout=None):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise ConnectionError("Network error")
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = _mock_openfigi_response(json)
            return resp

        resolver._http.post = mock_post
        resolver._last_request_time = 0

        with patch("src.resolution.figi_resolver.BACKOFF_BASE", 0.01):
            result = resolver._api_call([{"idType": "ID_ISIN", "idValue": "US0378331005"}])

        assert result is not None
        assert attempt_count == 3


class TestFigiReport:
    """Test the report generation."""

    def test_report_format(self, db_session):
        resolver = FigiResolver(db_session)
        resolver.stats = {
            "isin": 400, "cusip": 30, "sedol": 20,
            "ticker": 5, "cache": 0, "unresolved": 45,
        }
        report = resolver.get_report(500)
        assert "500 holdings totali" in report
        assert "455 risolte" in report
        assert "91.0%" in report
