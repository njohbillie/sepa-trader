"""
AI analyst — pre-trade safety gate + post-close evaluation +
weekly pick review + midweek slot-refill analysis.

Supports multiple AI providers:
  • anthropic        — Claude via Anthropic SDK
  • openai           — GPT-4 / o-series via OpenAI SDK
  • openai_compatible — any OpenAI-compatible endpoint (xAI, DeepSeek,
                         Mistral, Groq, Gemini, Ollama, …)
"""
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from .database import get_setting, get_user_setting

logger = logging.getLogger(__name__)

# ── Provider-aware call helper ────────────────────────────────────────────────

def _call_ai(
    db: Session,
    prompt: str,
    max_tokens: int,
    user_id: int = None,
) -> str | None:
    """
    Send *prompt* to whichever AI provider the user has configured.
    Returns the response text, or None if no credentials are set.
    Raises on hard API errors (caller decides whether to fail-open).
    """
    provider = get_user_setting(db, "ai_provider", "anthropic", user_id)
    api_key  = get_user_setting(db, "ai_api_key",  "",          user_id)
    model    = get_user_setting(db, "ai_model",    "",          user_id)
    base_url = get_user_setting(db, "ai_base_url", "",          user_id)

    if not api_key:
        return None

    if provider == "anthropic":
        import anthropic
        default_model = "claude-3-5-sonnet-20241022"
        resp = anthropic.Anthropic(api_key=api_key).messages.create(
            model=model or default_model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text

    # openai or openai_compatible (xAI, DeepSeek, Mistral, Groq, Gemini, …)
    if provider in ("openai", "openai_compatible"):
        from openai import OpenAI
        kwargs: dict = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        default_model = "gpt-4o" if provider == "openai" else ""
        resp = OpenAI(**kwargs).chat.completions.create(
            model=model or default_model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content

    raise ValueError(f"Unknown AI provider: {provider!r}")


# ── Pre-trade analysis ────────────────────────────────────────────────────────

def pre_trade_analysis(
    db: Session,
    symbol: str,
    side: str,
    qty: float,
    entry_price: float,
    stop_price: float,
    target_price: float,
    trigger: str,
    portfolio_value: float,
    cash: float,
    buying_power: float,
    mode: str,
    user_id: int = None,
) -> dict:
    """
    Ask Claude to evaluate a proposed trade before it is placed.
    Live accounts use graduated tier thresholds from get_live_account_limits().
    Paper accounts always use standard thresholds regardless of balance.

    Returns:
        {
            "proceed":  bool,
            "verdict":  str,    # "PROCEED" | "ABORT" | "WARN"
            "reason":   str,
            "warnings": [str],
            "analysis": str,
        }
    Fails open — never silently blocks trading due to API errors.
    """
    # Check credentials without fetching the full key for the log message
    api_key_set = bool(get_user_setting(db, "ai_api_key", "", user_id))
    if not api_key_set:
        logger.warning("pre_trade_analysis: no AI API key configured — skipping gate, proceeding.")
        return {
            "proceed":  True,
            "verdict":  "PROCEED",
            "reason":   "AI API key not configured — gate bypassed.",
            "warnings": ["AI API key not set — pre-trade analysis unavailable."],
            "analysis": "",
        }

    # Live accounts get graduated tier thresholds — paper always uses standard
    if mode == "live":
        from .database import get_live_account_limits
        limits       = get_live_account_limits(portfolio_value)
        account_tier = f"LIVE — {limits['tier']}"
    else:
        limits = {
            "max_position_pct": 20.0,
            "min_cash_pct":     10.0,
            "max_risk_pct":     2.0,
            "tier":             "PAPER",
        }
        account_tier = "PAPER — standard limits (practice account)"

    max_position_pct = limits["max_position_pct"]
    min_cash_pct     = limits["min_cash_pct"]
    max_risk_pct     = limits["max_risk_pct"]

    # Position sizing metrics
    trade_cost       = round(qty * entry_price, 2)
    risk_per_share   = round(entry_price - stop_price, 4) if stop_price > 0 else 0
    risk_dollars     = round(qty * risk_per_share, 2)
    rr               = round((target_price - entry_price) / risk_per_share, 2) if risk_per_share > 0 else 0
    pct_of_portfolio = round((trade_cost / portfolio_value) * 100, 2) if portfolio_value > 0 else 0
    pct_of_cash      = round((trade_cost / cash) * 100, 2) if cash > 0 else 999
    risk_pct_port    = round((risk_dollars / portfolio_value) * 100, 2) if portfolio_value > 0 else 0
    cash_after_pct   = round(((cash - trade_cost) / portfolio_value) * 100, 2) if portfolio_value > 0 else 0

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
Cash after trade:{cash_after_pct:.1f}% of portfolio

--- ACCOUNT STATE ---
Portfolio value: ${portfolio_value:,.2f}
Cash available:  ${cash:,.2f}
Buying power:    ${buying_power:,.2f}
Account tier:    {account_tier}

--- RULES (thresholds set by account tier) ---
1. R:R must be >= 2.0x. Current: {rr:.2f}x
2. Risk per trade must be <= {max_risk_pct}% of portfolio. Current: {risk_pct_port:.2f}%
3. Trade cost must not exceed buying power. Cost: ${trade_cost:,.2f}, BP: ${buying_power:,.2f}
4. Single position must not exceed {max_position_pct}% of portfolio. Current: {pct_of_portfolio:.1f}%
5. Cash after trade must remain >= {min_cash_pct}% of portfolio. After trade: {cash_after_pct:.1f}%

Use WARN for borderline cases (R:R 1.5–2.0, size near the limit).
Use ABORT only for clear rule violations.
Use PROCEED when all rules pass."""

    try:
        text = _call_ai(db, prompt, max_tokens=256, user_id=user_id)
        if text is None:
            return {
                "proceed":  True,
                "verdict":  "PROCEED",
                "reason":   "AI API key not configured — gate bypassed.",
                "warnings": ["AI API key not set — pre-trade analysis unavailable."],
                "analysis": "",
            }
        return _parse_pre_trade_response(text.strip())

    except Exception as exc:
        logger.error("pre_trade_analysis failed for %s: %s", symbol, exc)
        return {
            "proceed":  True,
            "verdict":  "PROCEED",
            "reason":   f"AI API error — gate bypassed: {str(exc)[:80]}",
            "warnings": [f"Pre-trade analysis error: {str(exc)[:80]}"],
            "analysis": "",
        }


def _parse_pre_trade_response(text: str) -> dict:
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

    return {
        "proceed":  verdict in ("PROCEED", "WARN"),
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
    user_id: int = None,
):
    db.execute(
        text("""
            INSERT INTO ai_analysis_log (trigger, symbol, analysis, mode, user_id)
            VALUES (:trigger, :symbol, :analysis, :mode, :uid)
        """),
        {
            "trigger":  f"pre_trade_{trigger.lower()}",
            "symbol":   symbol,
            "analysis": f"VERDICT: {verdict}\nREASON: {reason}\n\n{analysis}",
            "mode":     mode,
            "uid":      user_id,
        },
    )
    db.commit()


# ── Slot-refill analysis ──────────────────────────────────────────────────────

def analyze_slot_refill(
    db: Session,
    closed_symbol: str,
    close_reason: str,
    entry_price: float | None,
    close_price: float | None,
    portfolio_value: float,
    cash: float,
    buying_power: float,
    open_positions: list[str],
    pending_picks: list[dict],
    mode: str,
    user_id: int = None,
) -> dict:
    """
    Midweek slot-refill analysis. Decides whether to open a replacement
    position after a close, and which pick to prioritize if so.

    Returns:
        {
            "should_open": bool,
            "symbol":      str | None,
            "verdict":     str,   # "OPEN" | "WAIT" | "SKIP_WEEK"
            "reason":      str,
            "analysis":    str,
        }
    """
    if not get_user_setting(db, "ai_api_key", "", user_id):
        logger.warning("analyze_slot_refill: no AI API key — defaulting to auto-execute.")
        return {
            "should_open": True,
            "symbol":      pending_picks[0]["symbol"] if pending_picks else None,
            "verdict":     "OPEN",
            "reason":      "AI API key not configured — defaulting to next PENDING pick.",
            "analysis":    "",
        }

    pnl     = None
    pnl_pct = None
    if entry_price and close_price:
        pnl     = round(close_price - entry_price, 2)
        pnl_pct = round((pnl / entry_price) * 100, 2)

    closed_block = f"Symbol:       {closed_symbol}\nClose reason: {close_reason.replace('_', ' ').title()}\n"
    if entry_price:
        closed_block += f"Entry:        ${entry_price:.2f}\n"
    if close_price:
        closed_block += f"Close price:  ${close_price:.2f}\n"
    if pnl is not None:
        closed_block += f"P&L:          ${pnl:+.2f} ({pnl_pct:+.2f}%)\n"

    if pending_picks:
        pick_lines = []
        for i, p in enumerate(pending_picks, 1):
            ep = float(p.get("entry_price") or 0)
            sp = float(p.get("stop_price")  or 0)
            t1 = float(p.get("target1")     or 0)
            rr = round((t1 - ep) / (ep - sp), 2) if ep > sp > 0 and t1 > ep else "N/A"
            pick_lines.append(
                f"  {i}. {p['symbol']:6s}  score={p.get('score','?')}/8  "
                f"signal={p.get('signal','?'):20s}  entry=${ep:.2f}  "
                f"stop=${sp:.2f}  target=${t1:.2f}  R:R={rr}x  "
                f"note: {str(p.get('rationale',''))[:80]}"
            )
        picks_block = "\n".join(pick_lines)
    else:
        picks_block = "  (none remaining)"

    prompt = f"""You are a risk management AI for a Minervini SEPA swing trading system.
A position just closed midweek. Decide whether to open a replacement position.
Respond in EXACTLY this format — no other text:

VERDICT: <OPEN|WAIT|SKIP_WEEK>
SYMBOL: <ticker or NONE>
REASON: <one sentence ≤25 words>

--- CLOSED POSITION ---
{closed_block}
--- ACCOUNT STATE ---
Portfolio value:  ${portfolio_value:,.2f}
Cash available:   ${cash:,.2f}
Buying power:     ${buying_power:,.2f}
Open positions:   {', '.join(open_positions) if open_positions else 'none'} ({len(open_positions)} held)

--- PENDING PICKS THIS WEEK ---
{picks_block}

--- DECISION RULES ---
Use OPEN when:
  - Close was a target hit AND a quality pick exists (score ≥5, R:R ≥2)
  - Cash covers the next pick's entry with ≥15% cash buffer remaining
  - Not Thursday or Friday (insufficient time for setup to work)

Use WAIT when:
  - Close was a stop hit — wait for a better setup rather than chasing
  - Best available pick has score <5 or R:R <2
  - Cash would drop below 10% of portfolio after the trade
  - It is Thursday or Friday

Use SKIP_WEEK when:
  - All remaining picks are low quality (score <4)
  - Multiple stops hit this week — capital preservation mode
  - No picks remain

If OPEN: choose the highest-ranked pick with score ≥5 and R:R ≥2.
If WAIT or SKIP_WEEK: SYMBOL must be NONE."""

    try:
        text = _call_ai(db, prompt, max_tokens=200, user_id=user_id)
        if text is None:
            return {
                "should_open": True,
                "symbol":      pending_picks[0]["symbol"] if pending_picks else None,
                "verdict":     "OPEN",
                "reason":      "AI API key not configured — defaulting to next PENDING pick.",
                "analysis":    "",
            }
        return _parse_slot_refill_response(text.strip(), pending_picks)

    except Exception as exc:
        logger.error("analyze_slot_refill failed: %s — defaulting to next pick.", exc)
        return {
            "should_open": True,
            "symbol":      pending_picks[0]["symbol"] if pending_picks else None,
            "verdict":     "OPEN",
            "reason":      f"AI error — defaulting to next pick: {str(exc)[:60]}",
            "analysis":    "",
        }


def _parse_slot_refill_response(text: str, pending_picks: list[dict]) -> dict:
    verdict       = "WAIT"
    symbol        = None
    reason        = ""
    valid_symbols = {p["symbol"] for p in pending_picks}

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("VERDICT:"):
            v = line.split(":", 1)[1].strip().upper()
            if v in ("OPEN", "WAIT", "SKIP_WEEK"):
                verdict = v
        elif line.startswith("SYMBOL:"):
            s = line.split(":", 1)[1].strip().upper()
            if s != "NONE" and s in valid_symbols:
                symbol = s
        elif line.startswith("REASON:"):
            reason = line.split(":", 1)[1].strip()

    return {
        "should_open": verdict == "OPEN" and symbol is not None,
        "symbol":      symbol,
        "verdict":     verdict,
        "reason":      reason,
        "analysis":    text,
    }


# ── Post-close / weekly pick analysis ────────────────────────────────────────

def analyze_picks(db: Session, picks: list[dict], closed_position: dict | None = None, user_id: int = None) -> str:
    if not get_user_setting(db, "ai_api_key", "", user_id):
        raise ValueError("AI API key not configured in Settings.")

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
        sp = p.get("stop_price")  or 0
        t1 = p.get("target1")     or 0
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

    result = _call_ai(db, "\n\n".join(parts), max_tokens=1024, user_id=user_id)
    if result is None:
        raise ValueError("AI API key not configured in Settings.")
    return result


def generate_analyst_summary(
    db: Session,
    symbol: str,
    dd: dict,
    user_id: int = None,
) -> str:
    """
    Generate a concise AI summary of the DD data for a symbol.
    Returns a 2–3 sentence plain-English summary, or a fallback string on failure.
    """
    if not get_user_setting(db, "ai_api_key", "", user_id):
        return ""

    def fmt(val, pct=False, dollar=False):
        if val is None:
            return "N/A"
        if pct:
            return f"{val * 100:.1f}%"
        if dollar:
            return f"${val:,.2f}"
        return str(val)

    def fmt_cap(n):
        if n is None:
            return "N/A"
        if n >= 1e12:
            return f"${n / 1e12:.1f}T"
        if n >= 1e9:
            return f"${n / 1e9:.1f}B"
        if n >= 1e6:
            return f"${n / 1e6:.0f}M"
        return f"${n}"

    target_block = ""
    if dd.get("target_mean"):
        target_block = (
            f"Price targets — Mean: ${dd['target_mean']:.2f}  "
            f"High: ${dd.get('target_high') or '?'}  "
            f"Low: ${dd.get('target_low') or '?'}"
        )

    prompt = f"""You are a sell-side equity analyst. Write a 2–3 sentence summary of this stock's investment case for a swing trader, focusing on growth trajectory, valuation, and analyst sentiment. Be direct and concise — no fluff.

Symbol: {symbol}
Name: {dd.get('name', symbol)}
Sector: {dd.get('sector', 'N/A')} | Industry: {dd.get('industry', 'N/A')}
Market cap: {fmt_cap(dd.get('market_cap'))}
P/E (TTM): {fmt(dd.get('pe_ttm'))} | Forward P/E: {fmt(dd.get('forward_pe'))}
Revenue growth: {fmt(dd.get('revenue_growth'), pct=True)} | Earnings growth: {fmt(dd.get('earnings_growth'), pct=True)}
Gross margin: {fmt(dd.get('gross_margin'), pct=True)} | Net margin: {fmt(dd.get('net_margin'), pct=True)}
ROE: {fmt(dd.get('roe'), pct=True)} | D/E: {fmt(dd.get('debt_to_equity'))}
Analyst consensus: {dd.get('analyst_label', 'N/A')} ({dd.get('analyst_count', 0)} analysts)
{target_block}

Respond with only the 2–3 sentence summary — no headings, no bullet points."""

    try:
        result = _call_ai(db, prompt, max_tokens=150, user_id=user_id)
        return (result or "").strip()
    except Exception as exc:
        logger.warning("generate_analyst_summary failed for %s: %s", symbol, exc)
        return ""


def log_analysis(db: Session, trigger: str, symbol: str | None, analysis_text: str, mode: str, user_id: int = None):
    db.execute(
        text("""
            INSERT INTO ai_analysis_log (trigger, symbol, analysis, mode, user_id)
            VALUES (:trigger, :symbol, :analysis, :mode, :uid)
        """),
        {"trigger": trigger, "symbol": symbol, "analysis": analysis_text, "mode": mode, "uid": user_id},
    )
    db.commit()


def get_latest_analyses(db: Session, limit: int = 20, mode: str | None = None, user_id: int = None) -> list[dict]:
    filters = []
    params: dict = {"l": limit}
    if mode:
        filters.append("mode = :mode")
        params["mode"] = mode
    if user_id:
        filters.append("user_id = :uid")
        params["uid"] = user_id
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    rows = db.execute(
        text(f"""
            SELECT id, trigger, symbol, analysis, mode, created_at
            FROM ai_analysis_log
            {where}
            ORDER BY created_at DESC
            LIMIT :l
        """),
        params,
    ).fetchall()
    return [dict(r._mapping) for r in rows]