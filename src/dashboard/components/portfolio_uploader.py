"""Parse portfolio files (Excel/CSV) into positions list."""

from __future__ import annotations

import io
import re

import pandas as pd


# Accepted column names (case-insensitive, stripped)
_TICKER_NAMES = {"ticker", "isin", "etf", "ticker/isin", "ticker_isin", "identificativo"}
_AMOUNT_NAMES = {"importo", "amount", "eur", "value", "importo eur", "amount eur", "importo_eur", "amount_eur"}


def generate_template_xlsx() -> bytes:
    """Generate a template Excel file with example data."""
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Portafoglio"
    ws.append(["Ticker/ISIN", "Importo EUR"])
    ws.append(["CSPX", 30000])
    ws.append(["SWDA", 40000])
    ws.append(["VWCE", 15000])

    ws.column_dimensions["A"].width = 20
    ws.column_dimensions["B"].width = 15

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def parse_portfolio_file(
    file,
    filename: str = "",
) -> tuple[list[dict], list[str]]:
    """Parse an uploaded portfolio file.

    Args:
        file: File-like object (from st.file_uploader).
        filename: Original filename for format detection.

    Returns:
        Tuple of (positions, errors) where:
        - positions: list of {"ticker": str, "capital": float}
        - errors: list of human-readable error strings
    """
    errors: list[str] = []

    # Read into DataFrame
    try:
        if filename.endswith(".csv"):
            content = file.read()
            if isinstance(content, bytes):
                content = content.decode("utf-8-sig")
            try:
                df = pd.read_csv(io.StringIO(content), sep=",", on_bad_lines="skip")
                if len(df.columns) < 2:
                    df = pd.read_csv(io.StringIO(content), sep=";", on_bad_lines="skip")
            except Exception:
                df = pd.read_csv(io.StringIO(content), sep=";", on_bad_lines="skip")
        else:
            df = pd.read_excel(file)
    except Exception as exc:
        return [], [f"Impossibile leggere il file: {exc}"]

    if df.empty:
        return [], ["Il file è vuoto."]

    # Find ticker column
    df.columns = [str(c).strip() for c in df.columns]
    col_map = {c.lower(): c for c in df.columns}

    ticker_col = None
    for name in _TICKER_NAMES:
        if name in col_map:
            ticker_col = col_map[name]
            break

    amount_col = None
    for name in _AMOUNT_NAMES:
        if name in col_map:
            amount_col = col_map[name]
            break

    if ticker_col is None or amount_col is None:
        return [], [
            "Colonne non trovate. Il file deve avere intestazioni "
            "'Ticker/ISIN' e 'Importo EUR'. Scarica il template per un esempio. "
            f"Colonne trovate: {', '.join(df.columns)}"
        ]

    # Parse rows
    positions: list[dict] = []
    seen: dict[str, int] = {}

    for i, row in df.iterrows():
        row_num = i + 2
        ticker = str(row[ticker_col]).strip().upper()

        if not ticker or ticker == "NAN":
            continue

        raw_amount = str(row[amount_col]).strip()
        amount = _parse_amount(raw_amount)

        if amount is None:
            errors.append(f"Riga {row_num}: importo non valido '{raw_amount}' per {ticker}")
            continue

        if amount <= 0:
            errors.append(f"Riga {row_num}: importo deve essere positivo per {ticker}")
            continue

        if ticker in seen:
            idx = seen[ticker]
            positions[idx]["capital"] += amount
            errors.append(
                f"{ticker} trovato 2 volte — importi sommati: "
                f"€{positions[idx]['capital']:,.0f}"
            )
        else:
            seen[ticker] = len(positions)
            positions.append({"ticker": ticker, "capital": amount})

    if len(positions) > 20:
        errors.append(
            f"Attenzione: {len(positions)} ETF caricati. "
            "Portafogli molto grandi possono rallentare l'analisi."
        )

    return positions, errors


def _parse_amount(raw: str) -> float | None:
    """Parse amount string handling euro sign, locale separators.

    Handles:
    - European format: 30.000 or 30.000,50 (dot=thousands, comma=decimal)
    - US format: 30,000 or 30,000.50 (comma=thousands, dot=decimal)
    - Euro sign prefix, spaces
    """
    s = raw.replace("€", "").replace(" ", "").strip()
    if not s:
        return None

    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            # European: 30.000,50 → 30000.50
            s = s.replace(".", "").replace(",", ".")
        else:
            # US: 30,000.50 → 30000.50
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Decimal comma: 30000,50 → 30000.50
            s = s.replace(",", ".")
        else:
            # Thousands comma: 30,000 → 30000
            s = s.replace(",", "")
    elif "." in s:
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            # European thousands dot: 30.000 → 30000
            s = s.replace(".", "")
        # else: regular decimal dot, leave as-is

    try:
        return float(s)
    except ValueError:
        return None
