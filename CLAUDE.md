# ETF X-Ray Engine

## Architettura

6 layer pipeline:
1. **Ingestion** — fetch holdings da provider (ETF issuer sites, API)
2. **Normalization** — pulizia, dedup, schema uniforme
3. **Analytics** — calcolo metriche (overlap, concentration, exposure)
4. **Factor Engine** — decomposizione fattoriale (value, growth, momentum, quality, size, volatility)
5. **Orchestration** — scheduling e pipeline management (Prefect)
6. **Presentation** — dashboard interattiva (Streamlit + Plotly)

## Principi

- **Determinismo first**: no LLM nei Layer 1-4, risultati riproducibili
- **Composite FIGI** come primary key universale per security resolution
- **Coverage disclosure obbligatorio**: ogni output deve dichiarare % di holdings risolte
- Type hints obbligatori su ogni funzione pubblica
- Google-style docstring
- pytest per ogni modulo
- Librerie approvate: pandas, requests, sqlalchemy, yfinance, scipy, plotly, streamlit, prefect

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
