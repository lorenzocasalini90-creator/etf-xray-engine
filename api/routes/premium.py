"""Premium endpoints — whitelist check + AI analysis."""

import json
import logging
import os
import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Whitelist from env
# ---------------------------------------------------------------------------

def _get_whitelist() -> set[str]:
    raw = os.getenv("PREMIUM_WHITELIST", "")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


# ---------------------------------------------------------------------------
# POST /api/check-premium
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class CheckPremiumRequest(BaseModel):
    email: str

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip().lower()
        if not _EMAIL_RE.match(v):
            raise ValueError("Email non valida.")
        return v


@router.post("/check-premium")
async def check_premium(req: CheckPremiumRequest):
    email = req.email.strip().lower()
    whitelist = _get_whitelist()

    if email not in whitelist:
        return {"access": False, "reason": "not_in_whitelist"}

    # Send notification via Resend (best-effort)
    _send_notification(email)

    return {"access": True}


def _send_notification(email: str) -> None:
    """Best-effort email notification via Resend. Never raises."""
    try:
        import resend

        resend.api_key = os.getenv("RESEND_API_KEY", "")
        if not resend.api_key:
            return
        resend.Emails.send({
            "from": "onboarding@resend.dev",
            "to": "lorenzo.casalini90@gmail.com",
            "subject": "CheckMyETFs Pro — Accesso AI",
            "text": f"L'utente {email} ha appena usato l'Analisi AI.",
        })
    except Exception as exc:
        logger.warning("Resend notification failed: %s", exc)


# ---------------------------------------------------------------------------
# POST /api/ai-analysis
# ---------------------------------------------------------------------------

class PortfolioSummary(BaseModel):
    unique_securities: int | None = None
    hhi: float | None = None
    active_share: float | None = None
    high_redundancy_etfs: list[str] | None = None
    top_holding: str | None = None
    top_weight: float | None = None
    us_weight: float | None = None
    factor_profile: str | None = None


class AIAnalysisRequest(BaseModel):
    email: str
    portfolio_summary: PortfolioSummary

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip().lower()
        if not _EMAIL_RE.match(v):
            raise ValueError("Email non valida.")
        return v


@router.post("/ai-analysis")
async def ai_analysis(req: AIAnalysisRequest):
    import traceback

    email = req.email.strip().lower()
    whitelist = _get_whitelist()

    if email not in whitelist:
        raise HTTPException(status_code=403, detail="Accesso non autorizzato.")

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=503, detail="Servizio AI non configurato.")

    try:
        import anthropic
    except ImportError:
        raise HTTPException(status_code=503, detail="Servizio AI non disponibile.")

    try:
        client = anthropic.Anthropic(api_key=api_key)

        summary = req.portfolio_summary
        user_prompt = json.dumps(summary.model_dump(), ensure_ascii=False)

        system_prompt = (
            "Sei un consulente finanziario esperto di ETF. "
            "L'utente ti fornisce un riepilogo del suo portafoglio ETF. "
            "Analizzalo e rispondi SOLO con un JSON valido (niente markdown, niente ```), "
            "con questa struttura:\n"
            '{"summary": "Una frase semplice che descrive il portafoglio", '
            '"actions": [{"title": "...", "detail": "...", "priority": "alta|media|bassa"}]}\n'
            "Massimo 4 azioni. Sii concreto e specifico. Rispondi in italiano."
        )

        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )

        raw = message.content[0].text.strip()

        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            result = {"summary": raw, "actions": []}

        return result

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("ai-analysis error: %s", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(exc))
