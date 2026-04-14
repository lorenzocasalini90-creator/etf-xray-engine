"""CheckMyETFs FastAPI application."""

import asyncio
import os
import time

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from api.routes import analyze, benchmarks, etf_search, health, holdings, premium


# ---------------------------------------------------------------------------
# Middleware: request timeout
# ---------------------------------------------------------------------------

class TimeoutMiddleware(BaseHTTPMiddleware):
    """Return 504 if a request takes longer than 120 seconds."""

    async def dispatch(self, request, call_next):
        try:
            return await asyncio.wait_for(call_next(request), timeout=120.0)
        except asyncio.TimeoutError:
            return JSONResponse(
                {"detail": "Analisi timeout. Riprova \u2014 la cache si popola al primo tentativo."},
                status_code=504,
            )


# ---------------------------------------------------------------------------
# Middleware: in-memory rate limiting (per IP, /api/analyze only)
# ---------------------------------------------------------------------------

_RATE_LIMIT_MAX = 3       # max requests
_RATE_LIMIT_WINDOW = 60   # seconds
_rate_log: dict[str, list[float]] = {}


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Limit /api/analyze to 3 requests per minute per IP."""

    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/api/analyze" and request.method == "POST":
            ip = request.client.host if request.client else "unknown"
            if ip == "testclient":
                return await call_next(request)
            now = time.time()

            # Prune old timestamps
            timestamps = _rate_log.get(ip, [])
            timestamps = [t for t in timestamps if now - t < _RATE_LIMIT_WINDOW]

            if len(timestamps) >= _RATE_LIMIT_MAX:
                return JSONResponse(
                    {"detail": "Troppe richieste. Attendi 1 minuto."},
                    status_code=429,
                )

            timestamps.append(now)
            _rate_log[ip] = timestamps

        return await call_next(request)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="CheckMyETFs API", version="1.0.0")

app.add_middleware(TimeoutMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Validation error handler: return Italian messages
# ---------------------------------------------------------------------------

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Return the first user-facing validation message."""
    messages = []
    for err in exc.errors():
        msg = err.get("msg", "")
        # Pydantic prefixes custom messages with "Value error, "
        if msg.startswith("Value error, "):
            msg = msg[len("Value error, "):]
        messages.append(msg)
    detail = messages[0] if messages else "Dati non validi."
    return JSONResponse(status_code=422, content={"detail": detail})


app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(analyze.router, prefix="/api", tags=["analyze"])
app.include_router(holdings.router, prefix="/api", tags=["holdings"])
app.include_router(etf_search.router, prefix="/api", tags=["etf_search"])
app.include_router(benchmarks.router, prefix="/api", tags=["benchmarks"])
app.include_router(premium.router, prefix="/api", tags=["premium"])

if os.path.exists("frontend"):
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
