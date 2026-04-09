"""CheckMyETFs FastAPI application."""

import asyncio
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from api.routes import analyze, benchmarks, etf_search, health, holdings


class TimeoutMiddleware(BaseHTTPMiddleware):
    """Return 504 if a request takes longer than 55 seconds."""

    async def dispatch(self, request, call_next):
        try:
            return await asyncio.wait_for(call_next(request), timeout=55.0)
        except asyncio.TimeoutError:
            return JSONResponse(
                {"error": "Analisi timeout. Riprova \u2014 la cache si popola al primo tentativo."},
                status_code=504,
            )


app = FastAPI(title="CheckMyETFs API", version="1.0.0")

app.add_middleware(TimeoutMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(analyze.router, prefix="/api", tags=["analyze"])
app.include_router(holdings.router, prefix="/api", tags=["holdings"])
app.include_router(etf_search.router, prefix="/api", tags=["etf_search"])
app.include_router(benchmarks.router, prefix="/api", tags=["benchmarks"])

if os.path.exists("frontend"):
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
