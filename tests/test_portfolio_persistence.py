"""Tests for portfolio JSON persistence."""

import json

import pytest

from src.dashboard.components.portfolio_persistence import (
    deserialize_portfolio,
    generate_portfolio_filename,
    serialize_portfolio,
)


class TestSerializeDeserialize:
    def test_roundtrip(self):
        positions = [{"ticker": "CSPX", "capital": 30000}]
        json_str = serialize_portfolio(positions, benchmark="MSCI_WORLD")
        loaded, benchmark, warnings = deserialize_portfolio(json_str)
        assert benchmark == "MSCI_WORLD"
        assert len(loaded) == 1
        assert loaded[0]["ticker"] == "CSPX"
        assert loaded[0]["capital"] == 30000.0
        assert len(warnings) == 0

    def test_multiple_positions(self):
        positions = [
            {"ticker": "CSPX", "capital": 30000},
            {"ticker": "SWDA", "capital": 40000},
        ]
        json_str = serialize_portfolio(positions)
        loaded, _, _ = deserialize_portfolio(json_str)
        assert len(loaded) == 2

    def test_no_benchmark(self):
        positions = [{"ticker": "CSPX", "capital": 10000}]
        json_str = serialize_portfolio(positions, benchmark=None)
        _, benchmark, _ = deserialize_portfolio(json_str)
        assert benchmark is None


class TestDeserializeEdgeCases:
    def test_missing_optional_fields(self):
        json_str = '{"version":"1.0","positions":[{"ticker":"CSPX","amount_eur":30000}]}'
        positions, _, warnings = deserialize_portfolio(json_str)
        assert len(positions) == 1
        assert positions[0]["ticker"] == "CSPX"

    def test_invalid_json(self):
        with pytest.raises(ValueError, match="JSON"):
            deserialize_portfolio("questo non è json {{{")

    def test_missing_positions(self):
        with pytest.raises(ValueError):
            deserialize_portfolio('{"version":"1.0"}')

    def test_old_format_input_identifier(self):
        json_str = '{"version":"0.9","positions":[{"input_identifier":"CSPX","amount_eur":30000}]}'
        positions, _, warnings = deserialize_portfolio(json_str)
        assert len(positions) == 1
        assert positions[0]["ticker"] == "CSPX"
        assert any("Versione" in w for w in warnings)

    def test_invalid_amount_skipped(self):
        json_str = '{"version":"1.0","positions":[{"ticker":"CSPX","amount_eur":"abc"},{"ticker":"SWDA","amount_eur":40000}]}'
        positions, _, warnings = deserialize_portfolio(json_str)
        assert len(positions) == 1
        assert positions[0]["ticker"] == "SWDA"
        assert any("non valido" in w for w in warnings)


class TestGenerateFilename:
    def test_two_etf(self):
        positions = [{"display_ticker": "CSPX"}, {"display_ticker": "SWDA"}]
        name = generate_portfolio_filename(positions)
        assert "CSPX" in name and "SWDA" in name
        assert name.endswith(".json")

    def test_many_etf(self):
        positions = [{"display_ticker": f"ETF{i}"} for i in range(5)]
        name = generate_portfolio_filename(positions)
        assert "altri" in name

    def test_fallback_to_ticker(self):
        positions = [{"ticker": "CSPX"}]
        name = generate_portfolio_filename(positions)
        assert "CSPX" in name
