"""Holdings normalization utilities.

Provides name cleaning, ISIN validation, and deduplication by FIGI.
"""

import re

import pandas as pd

# Corporate suffixes to strip from security names
_SUFFIXES = re.compile(
    r"\b(Inc\.?|Corp\.?|Ltd\.?|PLC|SA|AG|SE|NV|SpA|GmbH|Co\.?|Class\s+[A-Z])\s*$",
    re.IGNORECASE,
)

# Valid ISIN: 2 uppercase letters + 10 alphanumeric characters
_ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def normalize_name(name: str) -> str:
    """Normalize a security name for matching.

    Uppercases, removes corporate suffixes, and strips extra whitespace.

    Args:
        name: Raw security name.

    Returns:
        Cleaned uppercase name.
    """
    if not name or not isinstance(name, str):
        return ""
    result = name.strip().upper()
    result = _SUFFIXES.sub("", result).strip()
    result = re.sub(r"\s+", " ", result)
    return result


def normalize_isin(isin: str) -> str | None:
    """Validate and normalize an ISIN.

    Args:
        isin: Raw ISIN string.

    Returns:
        Uppercase ISIN if valid, else None.
    """
    if not isin or not isinstance(isin, str):
        return None
    cleaned = isin.strip().upper()
    if _ISIN_PATTERN.match(cleaned):
        return cleaned
    return None


def deduplicate_holdings(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate holdings by composite_figi, summing weights.

    Holdings with the same FIGI are merged: weight_pct, market_value,
    and shares are summed. Other columns take the first value.

    Args:
        df: Holdings DataFrame with ``composite_figi`` column.

    Returns:
        Deduplicated DataFrame.
    """
    if "composite_figi" not in df.columns:
        return df

    has_figi = df["composite_figi"].notna()
    no_figi = df.loc[~has_figi].copy()
    with_figi = df.loc[has_figi].copy()

    if with_figi.empty:
        return df

    sum_cols = ["weight_pct", "market_value", "shares"]
    agg_dict = {}
    for col in with_figi.columns:
        if col == "composite_figi":
            continue
        if col in sum_cols:
            agg_dict[col] = "sum"
        else:
            agg_dict[col] = "first"

    deduped = with_figi.groupby("composite_figi", as_index=False).agg(agg_dict)
    result = pd.concat([deduped, no_figi], ignore_index=True)
    return result
