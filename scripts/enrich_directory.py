"""
Arricchisce etf_directory.csv con ETF non ancora presenti.
Prova in ordine: JustETF API → JustETF scraping → fallback manuale.
Aggiorna il CSV in-place e non tocca le righe esistenti.
"""
import pandas as pd
import requests
import re
import time
from pathlib import Path

CSV = Path("src/dashboard/data/etf_directory.csv")
df = pd.read_csv(CSV, dtype=str).fillna("")

# Tutti gli ISIN da aggiungere (nome vuoto = da cercare)
TO_ADD = [
    ("IE00BK5BCD43", ""),
    ("IE000YYE6WK5", ""),
    ("LU1829219390", ""),
    ("IE00B6R52143", ""),
    ("IE00B6R51Z18", ""),
    ("IE00BF2B0P08", ""),
    ("IE00B1XNHC34", ""),
    ("IE000RDRMSD1", ""),
    ("IE00BF0M6N54", ""),
    ("LU1681042518", ""),
]


def fetch_name_justetf(isin: str) -> str:
    """Prova a recuperare nome da JustETF."""
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

    # Tentativo 1: pagina profilo ETF
    try:
        url = f"https://www.justetf.com/en/etf-profile.html?isin={isin}"
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            m = re.search(r"<h1[^>]*>([^<]+)</h1>", r.text)
            if m:
                name = m.group(1).strip()
                if name and isin not in name and len(name) > 5:
                    return name
    except Exception:
        pass

    # Tentativo 2: search endpoint
    try:
        url = f"https://www.justetf.com/en/search.html?search=ETFS&isin={isin}"
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            m = re.search(r'class="[^"]*etf-title[^"]*"[^>]*>([^<]+)', r.text)
            if m:
                return m.group(1).strip()
    except Exception:
        pass

    # Tentativo 3: justetf-scraping library
    try:
        from justetf_scraping import search_etfs
        results = search_etfs(isin)
        if results is not None and not results.empty:
            if "name" in results.columns:
                return str(results.iloc[0]["name"])
    except Exception:
        pass

    return ""


existing = set(df["isin"].str.upper())
new_rows = []

for isin, known_name in TO_ADD:
    if isin.upper() in existing:
        print(f"SKIP {isin} (già presente)")
        continue
    name = known_name
    if not name:
        name = fetch_name_justetf(isin)
        time.sleep(0.5)
    status = f"'{name}'" if name else "NOT FOUND"
    print(f"{isin}: {status}")
    new_rows.append({
        "isin": isin,
        "ticker": isin,
        "name": name,
        "provider": "",
        "ter_pct": "",
        "domicile": "IE" if isin.startswith("IE") else "LU",
    })

if new_rows:
    df_new = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    df_new.to_csv(CSV, index=False)
    print(f"\nCSV aggiornato: +{len(new_rows)} righe")
    print(f"Totale righe: {len(df_new)}")
else:
    print("Nessuna nuova riga da aggiungere.")
