# ETF X-Ray Engine

Transparent portfolio analytics through holdings-level decomposition.

ETF X-Ray Engine fetches, normalises, and analyses ETF holdings data to provide factor exposure, sector overlap, and concentration metrics across multi-ETF portfolios.

## Architecture

6-layer pipeline:

| Layer | Package | Purpose |
|-------|---------|---------|
| 1 — Ingestion | `src/ingestion/` | Fetch holdings from ETF providers |
| 2 — Normalization | `src/resolution/` | FIGI resolution and identifier mapping |
| 3 — Analytics | `src/analytics/` | Overlap, concentration, exposure metrics |
| 4 — Factor Engine | `src/factors/` | Value, growth, momentum, quality, size, volatility |
| 5 — Orchestration | `src/flows/` | Pipeline scheduling via Prefect |
| 6 — Presentation | `src/dashboard/` | Interactive Streamlit dashboard |

## Setup

```bash
# Clone and install
git clone <repo-url> && cd etf-xray-engine
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Initialize the database
python -c "from src.storage.db import init_db; init_db()"

# Run tests
pytest
```

### Optional extras

```bash
pip install -e ".[dashboard]"       # Streamlit + Plotly
pip install -e ".[orchestration]"   # Prefect
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///etf_xray.db` | SQLAlchemy connection string |

## Repository structure

```
src/
├── ingestion/       # Fetcher base class + registry
├── resolution/      # FIGI resolution
├── analytics/       # Portfolio analytics
├── factors/         # Factor decomposition
├── storage/         # SQLAlchemy models + DB session
├── dashboard/       # Streamlit UI
├── flows/           # Prefect flows
├── interface/       # API layer
└── cli.py           # CLI entry point
```
