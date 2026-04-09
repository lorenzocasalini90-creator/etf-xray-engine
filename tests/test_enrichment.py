"""Tests for src.analytics.enrichment module."""

import pandas as pd
import pytest

from src.analytics.enrichment import (
    EXCHANGE_COUNTRY_MAP,
    STATIC_SECTOR_COUNTRY,
    enrich_missing_data,
    _enrich_from_portfolio_cross_ref,
    _enrich_from_static_mapping,
    _normalize_holding_name,
    _normalize_name_for_yfinance,
)


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Helper to create a holdings-like DataFrame."""
    cols = ["name", "ticker", "sector", "country", "real_weight_pct"]
    df = pd.DataFrame(rows, columns=cols)
    return df


class TestCrossRefEnrichment:
    """Test cross-reference enrichment from portfolio data."""

    def test_fills_sector_from_same_ticker(self):
        df = _make_df([
            {"name": "Apple Inc", "ticker": "AAPL", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
            {"name": "Apple Inc", "ticker": "AAPL", "sector": "",
             "country": "", "real_weight_pct": 3.0},
        ])
        _enrich_from_portfolio_cross_ref(df)
        assert df.iloc[1]["sector"] == "Technology"
        assert df.iloc[1]["country"] == "United States"

    def test_fills_from_name_match(self):
        df = _make_df([
            {"name": "Microsoft Corp", "ticker": "MSFT", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
            {"name": "Microsoft Corp", "ticker": "", "sector": "",
             "country": "", "real_weight_pct": 2.0},
        ])
        _enrich_from_portfolio_cross_ref(df)
        assert df.iloc[1]["sector"] == "Technology"
        assert df.iloc[1]["country"] == "United States"

    def test_does_not_overwrite_existing(self):
        df = _make_df([
            {"name": "Apple", "ticker": "AAPL", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
            {"name": "Apple", "ticker": "AAPL", "sector": "Consumer Electronics",
             "country": "US", "real_weight_pct": 3.0},
        ])
        _enrich_from_portfolio_cross_ref(df)
        assert df.iloc[1]["sector"] == "Consumer Electronics"
        assert df.iloc[1]["country"] == "US"

    def test_no_data_available(self):
        df = _make_df([
            {"name": "Unknown Co", "ticker": "UNK", "sector": "",
             "country": "", "real_weight_pct": 1.0},
        ])
        _enrich_from_portfolio_cross_ref(df)
        assert df.iloc[0]["sector"] == ""
        assert df.iloc[0]["country"] == ""


class TestEnrichMissingData:
    """Test the main enrich_missing_data function."""

    def test_empty_df(self):
        df = pd.DataFrame(columns=["name", "ticker", "sector", "country", "real_weight_pct"])
        result = enrich_missing_data(df)
        assert result.empty

    def test_all_populated_no_change(self):
        df = _make_df([
            {"name": "Apple", "ticker": "AAPL", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[0]["sector"] == "Technology"
        assert result.iloc[0]["country"] == "United States"

    def test_missing_becomes_unknown(self):
        """Holdings with no data available should get 'Unknown' not empty string."""
        df = _make_df([
            {"name": "Mystery Co", "ticker": "XYZ123", "sector": "",
             "country": "", "real_weight_pct": 0.1},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[0]["sector"] == "Unknown"
        assert result.iloc[0]["country"] == "Unknown"

    def test_none_values_handled(self):
        df = _make_df([
            {"name": "Test", "ticker": "TST", "sector": None,
             "country": None, "real_weight_pct": 1.0},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[0]["sector"] == "Unknown"
        assert result.iloc[0]["country"] == "Unknown"

    def test_cross_ref_fills_gaps(self):
        df = _make_df([
            {"name": "Apple Inc", "ticker": "AAPL", "sector": "Technology",
             "country": "United States", "real_weight_pct": 5.0},
            {"name": "Apple Inc", "ticker": "AAPL", "sector": None,
             "country": None, "real_weight_pct": 3.0},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[1]["sector"] == "Technology"
        assert result.iloc[1]["country"] == "United States"

    def test_static_mapping_fills_via_enrich(self):
        """Integration: static mapping fills sector+country via enrich_missing_data."""
        df = _make_df([
            {"name": "RTX", "ticker": "", "sector": "",
             "country": "Germany", "real_weight_pct": 5.0},
        ])
        result = enrich_missing_data(df)
        assert result.iloc[0]["sector"] == "Industrials"
        assert result.iloc[0]["country"] == "United States"


class TestNormalizeHoldingName:
    """Test the name normalization for static matching."""

    def test_strips_inc(self):
        assert _normalize_holding_name("Apple Inc") == "APPLE"
        assert _normalize_holding_name("Apple Inc.") == "APPLE"

    def test_strips_corp(self):
        assert _normalize_holding_name("NVIDIA Corp.") == "NVIDIA"
        assert _normalize_holding_name("RTX Corp") == "RTX"

    def test_strips_sa(self):
        assert _normalize_holding_name("Thales SA") == "THALES"

    def test_strips_spa(self):
        assert _normalize_holding_name("Leonardo SpA") == "LEONARDO"

    def test_strips_ag(self):
        assert _normalize_holding_name("Rheinmetall AG") == "RHEINMETALL"

    def test_strips_comma_ltd(self):
        assert _normalize_holding_name("HANWHA AEROSPACE Co., Ltd.") == "HANWHA AEROSPACE"
        assert _normalize_holding_name("Taiwan Semiconductor Manufacturing Co., Ltd.") == "TAIWAN SEMICONDUCTOR MANUFACTURING"

    def test_strips_comma_inc(self):
        assert _normalize_holding_name("Palantir Technologies, Inc.") == "PALANTIR TECHNOLOGIES"
        assert _normalize_holding_name("Teradyne, Inc.") == "TERADYNE"

    def test_strips_nv(self):
        assert _normalize_holding_name("ASML Holding NV") == "ASML"

    def test_strips_holdings(self):
        assert _normalize_holding_name("Lumentum Holdings") == "LUMENTUM"
        assert _normalize_holding_name("Leidos Holdings") == "LEIDOS"

    def test_empty(self):
        assert _normalize_holding_name("") == ""

    def test_no_suffix(self):
        assert _normalize_holding_name("RTX") == "RTX"

    def test_complex_suffix_chain(self):
        """Multiple suffixes stripped iteratively."""
        assert _normalize_holding_name("Archer-Daniels-Midland Co.") == "ARCHER-DANIELS-MIDLAND"


class TestStaticMappingEnrichment:
    """Test static mapping for well-known securities."""

    def test_fills_by_ticker(self):
        df = _make_df([
            {"name": "RTX Corp", "ticker": "RTX", "sector": "",
             "country": "", "real_weight_pct": 3.0},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Industrials"
        assert df.iloc[0]["country"] == "United States"

    def test_fills_by_normalized_name(self):
        """'Thales SA' normalizes to 'THALES' which is in the mapping."""
        df = _make_df([
            {"name": "Thales SA", "ticker": "", "sector": "",
             "country": "", "real_weight_pct": 2.0},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Industrials"
        assert df.iloc[0]["country"] == "France"

    def test_fills_leonardo_spa(self):
        df = _make_df([
            {"name": "Leonardo SpA", "ticker": "", "sector": "",
             "country": "Germany", "real_weight_pct": 2.0},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Industrials"
        assert df.iloc[0]["country"] == "Italy"

    def test_fills_nvidia_corp_dot(self):
        """NVIDIA Corp. → normalized NVIDIA → match."""
        df = _make_df([
            {"name": "NVIDIA Corp.", "ticker": "", "sector": "",
             "country": "Germany", "real_weight_pct": 4.0},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Information Technology"
        assert df.iloc[0]["country"] == "United States"

    def test_fills_hanwha_co_ltd(self):
        df = _make_df([
            {"name": "HANWHA AEROSPACE Co., Ltd.", "ticker": "", "sector": "",
             "country": "Germany", "real_weight_pct": 1.5},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Industrials"
        assert df.iloc[0]["country"] == "South Korea"

    def test_fills_palantir_inc(self):
        df = _make_df([
            {"name": "Palantir Technologies, Inc.", "ticker": "", "sector": "",
             "country": "Germany", "real_weight_pct": 1.5},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Information Technology"
        assert df.iloc[0]["country"] == "United States"

    def test_fills_tsmc_long_name(self):
        df = _make_df([
            {"name": "Taiwan Semiconductor Manufacturing Co., Ltd.", "ticker": "",
             "sector": "", "country": "Germany", "real_weight_pct": 3.0},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Information Technology"
        assert df.iloc[0]["country"] == "Taiwan"

    def test_fills_asml_holding_nv(self):
        df = _make_df([
            {"name": "ASML Holding NV", "ticker": "", "sector": "",
             "country": "Netherlands", "real_weight_pct": 2.0},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Information Technology"

    def test_fills_by_substring(self):
        """Name contains a mapping key as substring."""
        df = _make_df([
            {"name": "RHEINMETALL AG ORD", "ticker": "", "sector": "",
             "country": "Germany", "real_weight_pct": 2.0},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Industrials"
        # Country overwritten because sector was missing
        assert df.iloc[0]["country"] == "Germany"

    def test_does_not_overwrite_existing_sector_and_country(self):
        df = _make_df([
            {"name": "Leonardo SPA", "ticker": "LDO", "sector": "Aerospace",
             "country": "Italy", "real_weight_pct": 2.0},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Aerospace"
        assert df.iloc[0]["country"] == "Italy"

    def test_fixes_exchange_country_when_sector_missing(self):
        """When sector is missing, also fix country (likely exchange-based, not domicile)."""
        df = _make_df([
            {"name": "SAFRAN SA", "ticker": "", "sector": "",
             "country": "Germany", "real_weight_pct": 1.5},
        ])
        _enrich_from_static_mapping(df)
        assert df.iloc[0]["sector"] == "Industrials"
        assert df.iloc[0]["country"] == "France"

    def test_all_user_reported_names(self):
        """All names from the user's bug report should match."""
        test_cases = [
            ("RTX", "Industrials", "United States"),
            ("Thales SA", "Industrials", "France"),
            ("Leonardo SpA", "Industrials", "Italy"),
            ("SAAB", "Industrials", "Sweden"),
            ("Palo Alto Networks", "Information Technology", "United States"),
            ("NVIDIA Corp.", "Information Technology", "United States"),
            ("Lumentum Holdings", "Information Technology", "United States"),
            ("Elbit Systems", "Industrials", "Israel"),
            ("Curtiss-Wright", "Industrials", "United States"),
            ("Leidos Holdings", "Industrials", "United States"),
            ("ConocoPhillips", "Energy", "United States"),
            ("ASML Holding NV", "Information Technology", "Netherlands"),
            ("Analog Devices", "Information Technology", "United States"),
            ("Cognex", "Information Technology", "United States"),
            ("Infineon Technologies AG", "Information Technology", "Germany"),
        ]
        for name, expected_sector, expected_country in test_cases:
            df = _make_df([
                {"name": name, "ticker": "", "sector": "",
                 "country": "Germany", "real_weight_pct": 2.0},
            ])
            _enrich_from_static_mapping(df)
            assert df.iloc[0]["sector"] == expected_sector, (
                f"Failed for {name}: expected sector={expected_sector}, got={df.iloc[0]['sector']}"
            )
            assert df.iloc[0]["country"] == expected_country, (
                f"Failed for {name}: expected country={expected_country}, got={df.iloc[0]['country']}"
            )


class TestNormalizeNameForYfinance:
    """Test name-to-ticker normalization."""

    def test_simple_name(self):
        assert _normalize_name_for_yfinance("Apple Inc") == "APPLE"

    def test_compound_name(self):
        result = _normalize_name_for_yfinance("Booz Allen Hamilton")
        assert result == "BOOZ-ALLEN-HAMILTON"

    def test_empty_name(self):
        assert _normalize_name_for_yfinance("") is None

    def test_strips_suffixes(self):
        assert _normalize_name_for_yfinance("Thales SA") == "THALES"


class TestExchangeCountryMap:
    """Test the exchange code mapping."""

    def test_major_exchanges(self):
        assert EXCHANGE_COUNTRY_MAP["US"] == "United States"
        assert EXCHANGE_COUNTRY_MAP["LN"] == "United Kingdom"
        assert EXCHANGE_COUNTRY_MAP["GY"] == "Germany"
        assert EXCHANGE_COUNTRY_MAP["FP"] == "France"
        assert EXCHANGE_COUNTRY_MAP["JT"] == "Japan"
        assert EXCHANGE_COUNTRY_MAP["HK"] == "Hong Kong"
        assert EXCHANGE_COUNTRY_MAP["AT"] == "Australia"

    def test_european_exchanges(self):
        assert EXCHANGE_COUNTRY_MAP["NA"] == "Netherlands"
        assert EXCHANGE_COUNTRY_MAP["SM"] == "Spain"
        assert EXCHANGE_COUNTRY_MAP["IM"] == "Italy"
        assert EXCHANGE_COUNTRY_MAP["SS"] == "Sweden"
        assert EXCHANGE_COUNTRY_MAP["DC"] == "Denmark"


class TestExpandedStaticMapping:
    """Test that defense/banking/energy entries exist in static mapping."""

    @pytest.mark.parametrize("name,expected_sector,expected_country", [
        ("BOEING", "Industrials", "United States"),
        ("AIRBUS", "Industrials", "France"),
        ("DEUTSCHE BANK", "Financials", "Germany"),
        ("HSBC", "Financials", "United Kingdom"),
        ("SOCIETE GENERALE", "Financials", "France"),
        ("CREDIT AGRICOLE", "Financials", "France"),
        ("BARCLAYS", "Financials", "United Kingdom"),
        ("STANDARD CHARTERED", "Financials", "United Kingdom"),
        ("NORDEA", "Financials", "Finland"),
        ("DANSKE BANK", "Financials", "Denmark"),
        ("KBC GROUP", "Financials", "Belgium"),
        ("ERSTE GROUP", "Financials", "Austria"),
        ("RAIFFEISEN", "Financials", "Austria"),
        ("COMMERZBANK", "Financials", "Germany"),
        ("REPSOL", "Energy", "Spain"),
        ("GALP", "Energy", "Portugal"),
        ("OMV", "Energy", "Austria"),
    ])
    def test_entry_exists(self, name, expected_sector, expected_country):
        assert name in STATIC_SECTOR_COUNTRY
        sector, country = STATIC_SECTOR_COUNTRY[name]
        assert sector == expected_sector
        assert country == expected_country
