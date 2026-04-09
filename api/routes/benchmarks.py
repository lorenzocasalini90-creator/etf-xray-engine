"""Benchmarks endpoint (stub)."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/benchmarks")
async def list_benchmarks():
    """List available benchmark indices."""
    raise NotImplementedError("Coming in M1-b")
