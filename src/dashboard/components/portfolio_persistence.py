"""Portfolio JSON serialization and deserialization."""

from __future__ import annotations

import json
from datetime import datetime


_CURRENT_VERSION = "1.0"


def serialize_portfolio(
    positions: list[dict],
    benchmark: str | None = None,
) -> str:
    """Serialize portfolio positions to JSON string.

    Args:
        positions: List of {"ticker": str, "capital": float}.
        benchmark: Benchmark name (e.g. "MSCI_WORLD").

    Returns:
        JSON string.
    """
    data = {
        "version": _CURRENT_VERSION,
        "positions": [
            {"ticker": p["ticker"], "amount_eur": p["capital"]}
            for p in positions
        ],
        "benchmark": benchmark,
        "saved_at": datetime.now().isoformat(),
    }
    return json.dumps(data, indent=2, ensure_ascii=False)


def deserialize_portfolio(
    json_str: str,
) -> tuple[list[dict], str | None, list[str]]:
    """Deserialize portfolio JSON string.

    Args:
        json_str: JSON string from serialize_portfolio.

    Returns:
        Tuple of (positions, benchmark, warnings).

    Raises:
        ValueError: If JSON is invalid or missing required fields.
    """
    warnings: list[str] = []

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON non valido: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("Formato JSON non valido: deve essere un oggetto")

    if "positions" not in data:
        raise ValueError("Campo 'positions' mancante nel JSON")

    raw_positions = data["positions"]
    if not isinstance(raw_positions, list):
        raise ValueError("Il campo 'positions' deve essere una lista")

    positions: list[dict] = []
    for i, p in enumerate(raw_positions):
        if not isinstance(p, dict):
            warnings.append(f"Posizione {i + 1}: formato non valido, ignorata")
            continue

        ticker = p.get("ticker") or p.get("input_identifier", "")
        amount = p.get("amount_eur") or p.get("capital", 0)

        if not ticker:
            warnings.append(f"Posizione {i + 1}: ticker mancante, ignorata")
            continue

        try:
            amount = float(amount)
        except (TypeError, ValueError):
            warnings.append(f"Posizione {i + 1}: importo non valido, ignorata")
            continue

        positions.append({"ticker": str(ticker).strip().upper(), "capital": amount})

    if not positions:
        raise ValueError("Nessuna posizione valida trovata nel JSON")

    benchmark = data.get("benchmark")

    version = data.get("version", "unknown")
    if version != _CURRENT_VERSION:
        warnings.append(f"Versione file: {version} (attuale: {_CURRENT_VERSION})")

    return positions, benchmark, warnings


def generate_portfolio_filename(positions: list[dict]) -> str:
    """Generate a filename for the portfolio JSON.

    Uses display_ticker if available, falls back to ticker.
    Shows first 3 tickers, then "e N altri" if more.
    """
    tickers = [
        p.get("display_ticker") or p.get("ticker", "ETF")
        for p in positions
    ]

    if len(tickers) <= 3:
        name_part = "_".join(tickers)
    else:
        name_part = "_".join(tickers[:3]) + f"_e_{len(tickers) - 3}_altri"

    date_str = datetime.now().strftime("%Y%m%d")
    return f"portafoglio_{name_part}_{date_str}.json"
