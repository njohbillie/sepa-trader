"""
Claude AI analyst — pre-trade safety gate + post-close evaluation + weekly pick review.
"""
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from .database import get_setting

logger = logging.getLogger(__name__)


def _client(api_key: str):
    import anthropic
    return anthropic.Anthropic(api_key=api_key)


# ── Pre-trade analysis ────────────────────────────────────────────────────────

def pre_trade_analysis(
    db: Session,
    symbol: str,
    side: str,                  # "BUY" or "SELL"
    qty: float,
    entry_price: float,
    stop_price: float,
    target_price: float,
    trigger: str,               # "MONDAY_OPEN", "POST_CLOSE", "BREAKOUT", etc.
    portfolio_value: float,
    cash: float,
    buying_power: float,
    mode: str,
) -> dict:
    """
    Ask Claude to evaluate a proposed trade before it is placed.

    Returns:
        {
            "proceed":  bool,       # True = safe to place, False = abort
            "verdict":  str,        # "PROCEED" | "ABORT" | "WARN"
            "reason":   str,        # one-line explanation
            "warnings": [str],      # any flags even if proceeding
            "analysis": str,        # full Claude response text
        }

    If Claude API key is not set, returns proceed=True with a warning so
    trading is never silently blocked by a missing config.
    """
    api_key = get_setting(db, "claude_api_key", "")
    if not api_key:
        logger.warning("pre_trade_analysis: no Claude API key — skipping gate, proceeding.")
        return {
            "proceed":  True,
            "verdict":  "PROCEED",
            "reason":   "Claude API key not configured — gate bypassed.",
            "warnings": ["Claude API key not set — pre-trade analysis unavailable."],
            "analysis": "",
        }

    model = get_setting(db, "claude_model", "claude-sonnet-4-5")

    # Position sizing metrics
    trade_cost       = round(qty * entry_price, 2)
    risk_per_share   = round(entry_price - stop_price, 4) if stop_price > 0 else 0
    risk_dollars     = round(qty * risk_per_share, 2)
    reward_dollars   = round(qty * (target_price - entry_price), 2) if target_price > 0 else 0
    rr               = round((target_price - entry_price) / risk_per_share, 2) if risk_per_share > 0 else 0
    pct_of_portfolio = round((trade_cost / portfolio_value) * 100, 2) if portfolio_value > 0 else 0
    pct_of_cash      = round((trade_cost / cash) * 100, 2) if cash > 0 else 999
    risk_pct_port    = round((risk_dollars / portfolio_value) * 100, 2) if portfolio_value > 0 else 0

    prompt = f"""You are a risk management AI for a Minervini SEPA swing trading system.
Evaluate this proposed trade and respond in EXACTLY this format — no other text:

VERDICT: <PROCEED|WARN|ABORT>
REASON: <one sentence ≤20 words>
WARNINGS: <comma-separated flags, or "none">

--- PROPOSED TRADE ---
Mode:            {mode.upper()}
Symbol:          {symbol}
Side:            {side}
Trigger:         {trigger}
Qty:             {qty:.0f} shares
Entry:           ${entry_price:.2f}
Stop:            ${stop_price:.2f}  (risk/share: ${risk_per_share:.2f})
Target:          ${target_price:.2f}
R:R ratio:       {rr:.2f}x
Trade cost:      ${trade_cost:,.2f}
Risk $:          ${risk_dollars:,.2f}  ({risk_pct_port:.2f}% of portfolio)
% of portfolio:  {pct_of_portfolio:.1f}%
% of cash:       {pct_of_cash:.1f}%

--- ACCOUNT STATE ---
Portfolio value: ${portfolio_value:,.2f}
Cash available:  ${cash:,.2f}
Buying power:    ${buying_power:,.2f}

--- RULES (abort if violated) ---
1. R:R must be >= 2.0x. Current: {rr:.2f}x
2. Risk per trade must be <= 2% of portfolio. Current: {risk_pct_port:.2f}%
3. Trade cost must not exceed buying power. Cost: ${trade_cost:,.2f}, BP: ${buying_power:,.2f}
4. Single position must not exceed 20% of portfolio. Current: {pct_of_portfolio:.1f}%
5. Cash after trade must remain >= 10% of portfolio.

Use WARN (not ABORT) for borderline cases (R:R between 1.5-2.0, size 15-20%).
Use ABORT only for clear rule violations.
Use PROCEED when all rules pass."""

    try:
        resp = _client(api_key).messages.create(
            model=model,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        text_raw = resp.content[0].text.strip()
        return _parse_pre_trade_response(text_raw)

    except Exception as exc:
        logger.error("pre_trade_analysis failed for %s: %s", symbol, exc)
        # Fail open — don't block trades if Claude is unreachable
        return {
            "proceed":  True,
            "verdict":  "PROCEED",
            "reason":   f"Claude API error — gate bypassed: {str(exc)[:80]}",
            "warnings": [f"Pre-trade analysis error: {str(exc)[:80]}"],
            "analysis": "",
        }


def _parse_pre_trade_response(text: str) -> dict:
    """Parse Claude's structured pre-trade response."""
    verdict  = "PROCEED"
    reason   = ""
    warnings = []

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("VERDICT:"):
            v = line.split(":", 1)[1].strip().upper()
            if v in ("PROCEED", "WARN", "ABORT"):
                verdict = v
        elif line.startswith("REASON:"):
            reason = line.split(":", 1)[1].strip()
        elif line.startswith("WARNINGS:"):
            w = line.split(":", 1)[1].strip()
            if w.lower() != "none":
                warnings = [x.strip() for x in w.split(",") if x.strip()]

    proceed = verdict in ("PROCEED", "WARN")
    return {
        "proceed":  proceed,
        "verdict":  verdict,
        "reason":   reason,
        "warnings": warnings,
        "analysis": text,
    }


def log_pre_trade(
    db: Session,
    symbol: str,
    trigger: str,
    verdict: str,
    reason: str,
    analysis: str,
    mode: str,
):
    """Persist pre-trade analysis result to ai_analysis_log."""
    db.execute(
        text("""
            INSERT INTO ai_analysis_log (trigger, symbol, analysis, mode)
            VALUES (:trigger, :symbol, :analysis, :mode)
        """),
        {
            "trigger":  f"pre_trade_{trigger.lower()}",
            "symbol":   symbol,
            "analysis": f"VERDICT: {verdict}\nREASON: {reason}\n\n{analysis}",
            "mode":     mode,
        },
    )
    db.commit()


# ── Post-close / weekly analysis ─────────────────────────────────────────────

def analyze_picks(db: Session, picks: list[dict], closed_position: dict | None = None) -> str:
    api_key = get_setting(db, "claude_api_key", "")
    if not api_key:
        raise ValueError("Claude API key not configured in Settings.")

    model = get_setting(db, "claude_model", "claude-sonnet-4-5")

    parts = []
    if closed_position:
        parts.append(
            f"A position was just closed:\n"
            f"  Symbol: {closed_position['symbol']}\n"
            f"  Entry:  ${closed_position.get('entry_price') or 'N/A'}\n"
            f"  Reason: {closed_position.get('reason', 'position closed')}"
        )

    lines = []
    for i, p in enumerate(picks, 1):
        ep = p.get("entry_price") or 0
        sp = p.get("stop_price") or 0
        t1 = p.get("target1") or 0
        rr = round((t1 - ep) / (ep - sp), 2) if ep > sp > 0 and t1 > ep else "N/A"
        lines.append(
            f"{i}. {p['symbol']}  score={p.get('score','?')}/6  signal={p.get('signal','?')}"
            f"  entry=${ep:.2f}  stop=${sp:.2f}  t1=${t1:.2f}  R:R={rr}"
            f"  status={p.get('status','?')}  note: {p.get('rationale','')}"
        )
    parts.append("Current week's top picks:\n" + "\n".join(lines))
    parts.append(
        "You are a professional swing-trader assistant using Minervini SEPA criteria.\n"
        "For each PENDING pick above give a one-line recommendation: EXECUTE, WAIT, or SKIP "
        "with a brief reason (≤15 words). Consider score, R:R ratio, and signal quality.\n"
        "Output a numbered list only — no preamble."
    )

    resp = _client(api_key).messages.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": "\n\n".join(parts)}],
    )
    return resp.content[0].text


def log_analysis(db: Session, trigger: str, symbol: str | None, analysis_text: str, mode: str):
    db.execute(
        text("""
            INSERT INTO ai_analysis_log (trigger, symbol, analysis, mode)
            VALUES (:trigger, :symbol, :analysis, :mode)
        """),
        {"trigger": trigger, "symbol": symbol, "analysis": analysis_text, "mode": mode},
    )
    db.commit()


def get_latest_analyses(db: Session, limit: int = 20, mode: str | None = None) -> list[dict]:
    if mode:
        rows = db.execute(
            text("""
                SELECT id, trigger, symbol, analysis, mode, created_at
                FROM ai_analysis_log
                WHERE mode = :mode
                ORDER BY created_at DESC
                LIMIT :l
            """),
            {"l": limit, "mode": mode},
        ).fetchall()
    else:
        rows = db.execute(
            text("""
                SELECT id, trigger, symbol, analysis, mode, created_at
                FROM ai_analysis_log
                ORDER BY created_at DESC
                LIMIT :l
            """),
            {"l": limit},
        ).fetchall()
    return [dict(r._mapping) for r in rows]