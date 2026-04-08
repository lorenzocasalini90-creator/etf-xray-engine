"""Lightweight visit tracking — file-based, no external deps."""

import json
from pathlib import Path
from datetime import datetime

ANALYTICS_FILE = Path(__file__).parent.parent.parent.parent / "analytics_data.json"


def track_visit(page_name: str):
    """Track a page visit. Lightweight, no external deps."""
    try:
        data = json.loads(ANALYTICS_FILE.read_text()) if ANALYTICS_FILE.exists() else {"visits": [], "total": 0}
        data["total"] += 1
        data["visits"].append({"page": page_name, "ts": datetime.now().isoformat()})
        # Keep only last 1000 visits to avoid file bloat
        data["visits"] = data["visits"][-1000:]
        ANALYTICS_FILE.write_text(json.dumps(data, indent=2))
    except Exception:
        pass  # Never crash the app for analytics


def get_stats():
    """Get visit stats."""
    try:
        data = json.loads(ANALYTICS_FILE.read_text()) if ANALYTICS_FILE.exists() else {"visits": [], "total": 0}
        return data
    except Exception:
        return {"visits": [], "total": 0}
