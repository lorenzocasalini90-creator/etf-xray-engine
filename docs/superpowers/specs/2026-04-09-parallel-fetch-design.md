# Parallel ETF Fetch Design

**Goal:** Replace sequential ETF holdings fetch loop with parallel execution using ThreadPoolExecutor(max_workers=6). Target: 12 ETF portfolio under 60 seconds (currently ~3 minutes).

## Thread-Safety Analysis

| Component | Thread-safe? | Reason |
|---|---|---|
| HoldingsCacheManager | Yes | session_factory() creates new session per operation |
| SQLite engine | Yes | `check_same_thread=False`, writes serialized by SQLite |
| FetchOrchestrator | **No** | Holds FetcherRegistry with shared requests.Session per fetcher |
| requests.Session | No | Not thread-safe by design |

## Approach

Each worker thread creates its own `FetchOrchestrator` (which auto-creates its own `FetcherRegistry` → own fetcher instances → own `requests.Session`). The shared `HoldingsCacheManager` is passed to each orchestrator — safe because each DB operation opens/commits its own session.

## Changes

**Single file:** `src/dashboard/pages/01_portfolio_input.py` lines 379-405

### Before (sequential)
```python
for i, pos in enumerate(positions):
    ticker = pos["ticker"]
    # ... fetch one by one ...
```

### After (parallel)
```python
# 1. Split cached vs to-fetch
to_fetch = [(i, pos) for i, pos in enumerate(positions)
            if pos["ticker"] not in holdings_db or force_refresh]

# 2. Worker function — creates own orchestrator per thread
def _fetch_one(ticker):
    orch = FetchOrchestrator(cache=cache_manager)
    return orch.fetch(ticker, force_refresh=force_refresh)

# 3. Submit all to ThreadPoolExecutor(max_workers=6)
with ThreadPoolExecutor(max_workers=6) as pool:
    futures = {pool.submit(_fetch_one, pos["ticker"]): pos for _, pos in to_fetch}
    for future in as_completed(futures):
        # 4. Same result handling (cached/success/partial/failed)
```

## Constraints
- max_workers=6 (rate limiting)
- No rate limiting on JustETF beyond worker count
- st.write() calls from within the loop are fine (Streamlit handles concurrent writes)
- No changes outside the fetch loop
