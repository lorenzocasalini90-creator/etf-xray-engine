"""Benchmarks endpoint."""

from fastapi import APIRouter

router = APIRouter()

# Hardcoded standard benchmarks (matches src/analytics/benchmark.py BENCHMARK_PROXIES)
_DEFAULT_BENCHMARKS = [
    {"id": "MSCI_WORLD", "name": "MSCI World", "proxy_etf": "SWDA", "description": "Developed markets large & mid cap"},
    {"id": "SP500", "name": "S&P 500", "proxy_etf": "CSPX", "description": "US large cap 500 companies"},
    {"id": "MSCI_EM", "name": "MSCI Emerging Markets", "proxy_etf": "EIMI", "description": "Emerging markets large & mid cap"},
    {"id": "FTSE_ALL_WORLD", "name": "FTSE All-World", "proxy_etf": "VWCE", "description": "Global all cap (developed + emerging)"},
    {"id": "MSCI_ACWI", "name": "MSCI ACWI", "proxy_etf": "ISAC", "description": "All Country World Index"},
]


@router.get("/benchmarks")
async def list_benchmarks():
    """List available benchmark indices."""
    try:
        from src.analytics.benchmark import BenchmarkManager
        bmgr = BenchmarkManager()
        db_benchmarks = bmgr.list_benchmarks()
        if db_benchmarks:
            return [
                {
                    "id": b["name"],
                    "name": b["index_name"],
                    "proxy_etf": b["ticker"],
                    "description": "",
                }
                for b in db_benchmarks
            ]
    except Exception:
        pass

    return _DEFAULT_BENCHMARKS
