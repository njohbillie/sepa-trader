"""
Market Tape Analysis
====================
Computes 6 broad-market signals from Yahoo Finance data, calls the configured
AI provider for a structured verdict, and caches the result per user per day.

Cache behaviour
---------------
- One LLM call per user per trading day (lazily computed on first request).
- Cached in `market_tape_cache` table (keyed on user_id + date).
- Manual refresh clears today's row and recomputes.
- The verdict is returned instantly for any subsequent request that day.

Verdict format (JSON from LLM)
-------------------------------
{
  "condition": "favorable" | "caution" | "unfavorable",
  "summary":   "1–2 sentence plain-English explanation",
  "key_risk":  "single biggest concern right now"
}

If the AI call fails (no key, API error), the raw signals are still returned
so the UI can display them; condition defaults to "caution".
"""
import json
import logging
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Sector ETFs used as breadth proxy (% above their 50MA)
_SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLB", "XLU", "XLRE"]


# ── Public API ────────────────────────────────────────────────────────────────

def get_tape_check(db: Session, user_id: int, force_refresh: bool = False) -> dict:
    """
    Return today's tape verdict for the given user.
    Computes and caches on first call; returns cached result on subsequent calls.
    force_refresh=True clears the cache first.
    """
    today_str = date.today().isoformat()

    if force_refresh:
        db.execute(
            text("DELETE FROM market_tape_cache WHERE user_id = :uid AND cache_date = :d"),
            {"uid": user_id, "d": today_str},
        )
        db.commit()

    # Check cache
    row = db.execute(
        text("""
            SELECT signals, verdict, summary, key_risk, refreshed_at
            FROM market_tape_cache
            WHERE user_id = :uid AND cache_date = :d
        """),
        {"uid": user_id, "d": today_str},
    ).fetchone()

    if row:
        signals = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        return {
            "condition":    row[1],
            "summary":      row[2],
            "key_risk":     row[3],
            "refreshed_at": row[4].isoformat() if row[4] else None,
            "signals":      signals,
            "cached":       True,
        }

    # Compute fresh
    return _compute_and_cache(db, user_id, today_str)


# ── Signal computation ────────────────────────────────────────────────────────

def _compute_signals() -> dict:
    """
    Compute all 6 broad-market signals. Returns a dict with raw values.
    Never raises — returns partial data on individual failures.
    """
    from .strategies.yf_client import fetch_history

    import math as _math
    from .utils import sf as _sf

    def _safe_round(v, digits=2):
        """Round a value, returning None if it's nan/inf/None."""
        f = _sf(v)
        return round(f, digits) if f is not None else None

    signals: dict = {}

    # ── 1 & 2: SPY trend + 20-day return ─────────────────────────────────────
    try:
        spy = fetch_history("SPY", period_days=260)
        if not spy.empty and len(spy) >= 20:
            close    = spy["Close"]
            sma200   = _sf(close.rolling(200).mean().iloc[-1])
            spy_last = _sf(close.iloc[-1])
            spy_20d  = _sf(close.iloc[-20]) if len(close) >= 20 else spy_last
            if spy_last is not None:
                signals["spy_price"]     = _safe_round(spy_last)
                signals["spy_sma200"]    = _safe_round(sma200)
                signals["spy_above_200"] = (spy_last > sma200) if sma200 is not None else None
                if spy_20d:
                    signals["spy_20d_return"] = _safe_round((spy_last - spy_20d) / spy_20d * 100)
                w52_high = _sf(close.tail(252).max())
                signals["spy_52w_high"] = _safe_round(w52_high)
                if w52_high:
                    signals["spy_drawdown"] = _safe_round((w52_high - spy_last) / w52_high * 100)
    except Exception as exc:
        logger.warning("market_analysis: SPY signals failed: %s", exc)

    # ── 3: VIX level ──────────────────────────────────────────────────────────
    try:
        vix = fetch_history("^VIX", period_days=5)
        if not vix.empty:
            signals["vix"] = _safe_round(_sf(vix["Close"].iloc[-1]))
    except Exception as exc:
        logger.warning("market_analysis: VIX failed: %s", exc)

    # ── 4: Market breadth — % of sector ETFs above their 50MA ────────────────
    try:
        above50 = 0
        total   = 0
        for etf in _SECTOR_ETFS:
            df = fetch_history(etf, period_days=60)
            if not df.empty and len(df) >= 50:
                sma50 = _sf(df["Close"].rolling(50).mean().iloc[-1])
                last  = _sf(df["Close"].iloc[-1])
                if sma50 is not None and last is not None:
                    if last > sma50:
                        above50 += 1
                    total += 1
        if total > 0:
            signals["breadth_pct"]   = round(above50 / total * 100, 1)
            signals["breadth_above"] = above50
            signals["breadth_total"] = total
    except Exception as exc:
        logger.warning("market_analysis: breadth failed: %s", exc)

    # ── 5: Risk-on/off — SPY 5d vs TLT 5d ────────────────────────────────────
    try:
        tlt = fetch_history("TLT", period_days=10)
        if not tlt.empty and len(tlt) >= 5:
            tlt_5d  = _sf(tlt["Close"].iloc[-5])
            tlt_now = _sf(tlt["Close"].iloc[-1])
            if tlt_5d and tlt_now:
                signals["tlt_5d_return"] = _safe_round((tlt_now - tlt_5d) / tlt_5d * 100)
    except Exception as exc:
        logger.warning("market_analysis: TLT failed: %s", exc)

    return signals


# ── LLM call ──────────────────────────────────────────────────────────────────

def _ask_ai(db: Session, signals: dict, user_id: int) -> tuple[str, str, str]:
    """
    Ask the configured AI for a tape verdict.
    Returns (condition, summary, key_risk).
    Defaults to 'caution' on any failure.
    """
    from .claude_analyst import _call_ai

    # Build a compact, structured prompt
    breadth_str = (
        f"{signals.get('breadth_pct', '?')}% of sector ETFs above 50MA "
        f"({signals.get('breadth_above', '?')}/{signals.get('breadth_total', 10)})"
        if "breadth_pct" in signals else "breadth data unavailable"
    )

    tlt_str = (
        f"TLT 5-day return: {signals.get('tlt_5d_return', '?')}% "
        f"({'bonds rallying = risk-off' if signals.get('tlt_5d_return', 0) > 0 else 'bonds selling = risk-on'})"
        if "tlt_5d_return" in signals else "TLT data unavailable"
    )

    prompt = f"""You are a professional equity market analyst. Based on the following data, provide a brief market tape assessment.

MARKET DATA ({date.today().isoformat()}):
- SPY: ${signals.get('spy_price','?')} | {'ABOVE' if signals.get('spy_above_200') else 'BELOW'} 200-day SMA (${signals.get('spy_sma200','?')})
- SPY 20-day return: {signals.get('spy_20d_return','?')}%
- SPY drawdown from 52-week high: {signals.get('spy_drawdown','?')}%
- VIX: {signals.get('vix','?')} {'(fear elevated)' if signals.get('vix',0) > 25 else '(calm)'}
- Breadth: {breadth_str}
- Risk-on/off: {tlt_str}

Respond ONLY with valid JSON (no markdown, no explanation):
{{
  "condition": "favorable" or "caution" or "unfavorable",
  "summary": "1-2 sentence plain-English assessment of current market conditions for swing trading",
  "key_risk": "the single biggest risk to watch right now in 10 words or fewer"
}}"""

    try:
        raw = _call_ai(db, prompt, max_tokens=200, user_id=user_id)
        if not raw:
            return "caution", "AI analysis unavailable — configure an API key in Settings.", "No AI key configured"

        # Strip markdown fences if present
        text = raw.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        parsed = json.loads(text.strip())
        condition = parsed.get("condition", "caution").lower()
        if condition not in ("favorable", "caution", "unfavorable"):
            condition = "caution"
        return condition, parsed.get("summary", ""), parsed.get("key_risk", "")

    except Exception as exc:
        logger.warning("market_analysis: AI call failed: %s", exc)
        return "caution", f"AI analysis failed: {str(exc)[:100]}", "Check AI configuration"


# ── Cache write ───────────────────────────────────────────────────────────────

def _compute_and_cache(db: Session, user_id: int, today_str: str) -> dict:
    """Compute signals, call AI, persist to cache, return result."""
    logger.info("market_analysis: computing fresh tape check for user %d", user_id)

    signals   = _compute_signals()
    condition, summary, key_risk = _ask_ai(db, signals, user_id)

    try:
        db.execute(
            text("""
                INSERT INTO market_tape_cache
                    (user_id, cache_date, signals, verdict, summary, key_risk)
                VALUES (:uid, :d, CAST(:sig AS jsonb), :v, :s, :kr)
                ON CONFLICT (user_id, cache_date)
                DO UPDATE SET
                    signals      = EXCLUDED.signals,
                    verdict      = EXCLUDED.verdict,
                    summary      = EXCLUDED.summary,
                    key_risk     = EXCLUDED.key_risk,
                    refreshed_at = NOW()
            """),
            {
                "uid": user_id,
                "d":   today_str,
                "sig": json.dumps(signals),
                "v":   condition,
                "s":   summary,
                "kr":  key_risk,
            },
        )
        db.commit()
    except Exception as exc:
        logger.warning("market_analysis: cache write failed: %s", exc)

    return {
        "condition":    condition,
        "summary":      summary,
        "key_risk":     key_risk,
        "refreshed_at": date.today().isoformat(),
        "signals":      signals,
        "cached":       False,
    }
