"""Swap-based ETF detection.

Identifies ETFs using synthetic replication (swap-based), where the
disclosed holdings represent a substitute basket rather than the actual
economic exposure.  This matters for overlap analysis — two ETFs tracking
the same index may show low overlap if one is swap-based.
"""

# Ticker or ISIN → human-readable label for the warning message.
SWAP_BASED_ETFS: dict[str, str] = {
    # Amundi swap-based
    "CW8": "Amundi MSCI World",
    "MWRD": "Amundi MSCI World",
    "LCWD": "Amundi MSCI World",
    "CC1": "Amundi S&P 500",
    "PANX": "Amundi Nasdaq-100",
    "RS2K": "Amundi Russell 2000",
    "PAEEM": "Amundi MSCI Emerging Markets",
    "LU1681043599": "Amundi MSCI World",
    "LU1437016972": "Amundi MSCI Emerging Markets",
    "LU0908500753": "Amundi S&P 500",
    "LU1681038672": "Amundi Nasdaq-100",
    "LU1681038912": "Amundi Russell 2000",
    "LU1829219390": "Amundi Euro Stoxx Banks",
    # Xtrackers swap-based
    "XDWD": "Xtrackers MSCI World Swap",
    "DBXW": "Xtrackers MSCI World Swap",
    "DBZB": "Xtrackers Euro Govt Bond Swap",
    "X25E": "Xtrackers Euro Stoxx 50 Swap",
    "XMEM": "Xtrackers MSCI EM Swap",
    "IE00BJ0KDQ92": "Xtrackers MSCI World Swap",
    "LU0274208692": "Xtrackers MSCI EM Swap",
}


def detect_swap_etfs(tickers: list[str]) -> list[dict]:
    """Check which portfolio ETFs are swap-based.

    Args:
        tickers: List of ETF tickers or ISINs in the portfolio.

    Returns:
        List of dicts with keys ``ticker`` and ``label`` for each
        swap-based ETF found.
    """
    found = []
    for t in tickers:
        label = SWAP_BASED_ETFS.get(t.strip().upper())
        if label:
            found.append({"ticker": t, "label": label})
    return found
