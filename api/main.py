"""CheckMyETFs FastAPI application."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import analyze, benchmarks, etf_search, health, holdings

app = FastAPI(title="CheckMyETFs API", version="1.0.0")

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

import os

from fastapi.staticfiles import StaticFiles

if os.path.exists("frontend"):
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
