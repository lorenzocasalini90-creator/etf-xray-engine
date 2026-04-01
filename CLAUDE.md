# ETF X-Ray Engine

## Architettura

6 layer pipeline:
1. **Ingestion** — fetch holdings da provider (ETF issuer sites, API)
2. **Normalization** — pulizia, dedup, schema uniforme
3. **Analytics** — calcolo metriche (overlap, concentration, exposure)
4. **Factor Engine** — decomposizione fattoriale (value, growth, momentum, quality, size, volatility)
5. **Orchestration** — scheduling e pipeline management (Prefect)
6. **Presentation** — dashboard interattiva (Streamlit + Plotly)

## Data Ingestion Architecture (aggiornato 1 Aprile 2026)

### FetchOrchestrator (src/ingestion/orchestrator.py)
Coordina il fetch di holdings con cascade try-first:
1. Cache check → se fresca (<7 giorni), ritorna subito
2. Metadata resolution → justetf-scraping per ISIN → emittente
3. Fetcher specifico emittente → iShares/Xtrackers/Amundi/Invesco/SPDR
4. Brute force → prova tutti i fetcher registrati
5. JustETF fallback → top 10 holdings (status="partial")
6. Cache stale → se fetch fallisce ma cache vecchia esiste
7. Errore chiaro → messaggio user-friendly

### FetchResult (src/ingestion/base_fetcher.py)
Ogni fetch ritorna FetchResult:
- status: "success" | "cached" | "partial" | "failed"
- holdings: pd.DataFrame | None
- message: stringa user-friendly
- coverage_pct: float (100.0 = completo, 35.0 = top 10)
- source: stringa (es. "ishares_direct", "justetf_top10", "cache")

### can_handle() → float
Ritorna probabilità 0.0-1.0, non bool. Usato per routing:
- 0.95: ISIN/ticker noto dell'emittente
- 0.8-0.9: ISIN con prefisso paese compatibile
- 0.3-0.5: ticker sconosciuto
- 0.1: JustETF fallback (sempre ultimo)
- 0.0: fetcher disabilitato

### Fetcher Status
| Fetcher | File | Metodo | UCITS | US |
|---|---|---|---|---|
| iShares | ishares.py | etf-scraper + API interna | ✅ | ✅ |
| Xtrackers | xtrackers.py | Excel/JSON da etf.dws.com | ✅ | — |
| Amundi | amundi.py | POST API amundietf.fr | ✅ | — |
| Invesco | invesco.py | etf-scraper wrapper | ❌ | ✅ |
| SPDR | spdr.py | etf-scraper wrapper | ❌ | ✅ |
| JustETF | justetf.py | justetf-scraping (top 10) | ✅ partial | — |

### Cross-ETF Matching (src/analytics/_match_key.py)
Il matching tra holdings di ETF diversi usa un sistema a 3 livelli:
1. Static lookup table (src/data/ticker_isin_map.json) — 505 mappings ticker↔ISIN
2. Dynamic lookup — impara da ETF che hanno sia ticker che ISIN (es. Amundi)
3. Bloomberg ticker normalization — rimuove suffissi (AAPL US → AAPL)
NO FIGI nella dashboard. Il FigiResolver è solo per uso futuro con Prefect (Fase 7).

### HoldingsCache (src/storage/cache.py)
- TTL 7 giorni, cache-first
- Stale fallback se fetch live fallisce
- force_refresh=True per bypassare
- Holdings salvate CON match_key già calcolato

### Convenzioni fetcher
- Ogni fetcher implementa BaseFetcher con try_fetch() che MAI alza exception
- try_fetch() ritorna sempre FetchResult
- Output normalizzato nello schema standardizzato (vedi sezione Fase 1)
- Test con mock HTTP, mai chiamate di rete nei test
- Regola 3 tentativi per operazioni di rete, max 10 minuti, poi documenta e fermati

## Principi

- **Determinismo first**: no LLM nei Layer 1-4, risultati riproducibili
- **Composite FIGI** come primary key universale per security resolution
- **Coverage disclosure obbligatorio**: ogni output deve dichiarare % di holdings risolte
- Type hints obbligatori su ogni funzione pubblica
- Google-style docstring
- pytest per ogni modulo
- Librerie approvate: pandas, requests, sqlalchemy, yfinance, scipy, plotly, streamlit, prefect

### API Commerciali — Valutazione (1 Aprile 2026)
- API Ninjas: $39/mese minimo, no caching su Developer plan. Riconosce UCITS ma holdings gated.
- Finnhub: $50/mese per mercato, non riconosce ticker UCITS. Scartato.
- Vanguard API: geo-bloccata sia da IT che da AWS US. Sito UK è SPA Angular. Skippato.
- Decisione: fetcher diretti gratis per MVP. API commerciale solo quando il prodotto genera revenue.

## Schema DB

8 tabelle:
- `etf_metadata` — anagrafica ETF
- `holdings` — posizioni per ETF + data
- `figi_mapping` — mapping identificativi → composite FIGI
- `security_fundamentals` — dati fondamentali per security
- `sector_factor_proxies` — proxy settore/fattore
- `benchmarks` — benchmark di riferimento
- `portfolios` — portafogli utente
- `portfolio_positions` — posizioni nei portafogli utente
- `holdings_cache` — cache holdings scaricate (nuova)

### Tabella holdings_cache (nuova)
| Colonna | Tipo | Note |
|---|---|---|
| etf_identifier | String | PK composita con source |
| source | String | "ishares", "xtrackers", etc. |
| holdings_json | Text | DataFrame serializzato JSON |
| fetched_at | DateTime | Timestamp del fetch |
| stale_after | DateTime | fetched_at + 7 giorni |
| coverage_pct | Float | 100.0 o 35.0 per top-10 |
| num_holdings | Integer | Conteggio holdings |
| status | String | "success" o "partial" |

## Schema output standardizzato fetcher

Colonne obbligatorie:
`etf_ticker`, `holding_name`, `holding_isin`, `holding_ticker`, `holding_sedol`, `holding_cusip`, `weight_pct`, `market_value`, `shares`, `sector`, `country`, `currency`, `as_of_date`

## Struttura cartelle

```
src/
├── ingestion/       # Layer 1: fetch holdings
├── resolution/      # Layer 2: FIGI resolution e normalization
├── analytics/       # Layer 3: metriche di analisi
├── factors/         # Layer 4: factor engine
├── storage/         # DB models e session management
├── dashboard/       # Layer 6: Streamlit UI
├── flows/           # Layer 5: Prefect orchestration
└── interface/       # CLI e API entry points
```
