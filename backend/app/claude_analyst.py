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

# ── Alpaca news fetcher ───────────────────────────────────────────────────────

def _fetch_alpaca_news(
    symbols: list[str],
    db: Session,
    user_id: int = None,
    mode: str = "paper",
    limit: int = 3,
    hours: int = 48,
) -> dict[str, list[str]]:
    """
    Fetch recent news headlines from Alpaca for a batch of symbols.
    Returns {symbol: ["Headline (Source, Date)", ...]} — empty list if none found.
    Falls back silently on any error so analysis still runs without news.
    News content is mode-agnostic but credentials are resolved per mode so
    users with only live keys (no paper keys) still get news in live mode.
    """
    import httpx
    from datetime import datetime, timezone, timedelta
    from .config import settings as _cfg
    from .database import get_user_setting as _gus

    try:
        # Resolve credentials matching the active mode — same logic as _get_portfolio_value
        if user_id:
            from sqlalchemy import text as _text
            is_admin = db.execute(
                _text("SELECT role FROM users WHERE id = :id"), {"id": user_id}
            ).scalar() == "admin"
            if mode == "live":
                key    = _gus(db, "alpaca_live_key",    "", user_id) or (
                    _cfg.alpaca_live_key if is_admin else ""
                )
                secret = _gus(db, "alpaca_live_secret", "", user_id) or (
                    _cfg.alpaca_live_secret if is_admin else ""
                )
            else:
                key    = _gus(db, "alpaca_paper_key",    "", user_id) or (
                    _cfg.alpaca_paper_key if is_admin else ""
                )
                secret = _gus(db, "alpaca_paper_secret", "", user_id) or (
                    _cfg.alpaca_paper_secret if is_admin else ""
                )
            # Fall back to the other mode's keys if primary mode has none
            if not key or not secret:
                alt = "paper" if mode == "live" else "live"
                key    = _gus(db, f"alpaca_{alt}_key",    "", user_id) or (
                    getattr(_cfg, f"alpaca_{alt}_key", "") if is_admin else ""
                )
                secret = _gus(db, f"alpaca_{alt}_secret", "", user_id) or (
                    getattr(_cfg, f"alpaca_{alt}_secret", "") if is_admin else ""
                )
        else:
            if mode == "live":
                key    = (_cfg.alpaca_live_key    or "").strip()
                secret = (_cfg.alpaca_live_secret or "").strip()
            else:
                key    = (_cfg.alpaca_paper_key    or "").strip()
                secret = (_cfg.alpaca_paper_secret or "").strip()
            # Fall back to the other mode's keys
            if not key or not secret:
                key    = (_cfg.alpaca_paper_key or _cfg.alpaca_live_key    or "").strip()
                secret = (_cfg.alpaca_paper_secret or _cfg.alpaca_live_secret or "").strip()

        if not key or not secret:
            return {}

        start = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = httpx.get(
            "https://data.alpaca.markets/v1beta1/news",
            params={
                "symbols": ",".join(symbols),
                "limit":   min(limit * len(symbols), 50),
                "start":   start,
                "sort":    "desc",
                "include_content": "false",
            },
            headers={"APCA-API-KEY-ID": key.strip(), "APCA-API-SECRET-KEY": secret.strip()},
            timeout=10,
        )
        resp.raise_for_status()

        news_map: dict[str, list[str]] = {s: [] for s in symbols}
        for article in resp.json().get("news", []):
            headline  = article.get("headline", "").strip()
            source    = article.get("source", "").strip()
            published = article.get("created_at", "")[:10]   # YYYY-MM-DD
            for sym in article.get("symbols", []):
                if sym in news_map and len(news_map[sym]) < limit:
                    news_map[sym].append(f'"{headline}" ({source}, {published})')

        return news_map

    except Exception as exc:
        logger.warning("_fetch_alpaca_news: failed (%s) — continuing without news.", exc)
        return {}


def _news_block(news: list[str]) -> str:
    if not news:
        return ""
    return "\n   news: " + " | ".join(news)


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
    tape_context: dict | None = None,
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
    Blocks trades when no AI key is configured — the gate must be actively
    enabled, not silently bypassed.  API errors produce WARN (log + proceed)
    so a temporary outage doesn't freeze all trading.
    """
    # No AI key → hard block.  User must configure a key to use the gate.
    api_key_set = bool(get_user_setting(db, "ai_api_key", "", user_id))
    if not api_key_set:
        logger.warning("pre_trade_analysis: no AI API key configured — trade blocked.")
        return {
            "proceed":  False,
            "verdict":  "HOLD",
            "reason":   "AI API key not configured — configure one in Settings to enable pre-trade analysis.",
            "warnings": ["No AI API key set. Go to Settings → AI to add your key."],
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

    # Optional tape context block
    tape_block = ""
    if tape_context:
        tc = tape_context.get("condition", "caution").upper()
        ts = tape_context.get("summary", "")
        tr = tape_context.get("key_risk", "")
        sigs = tape_context.get("signals", {})
        vix = sigs.get("vix")
        breadth = sigs.get("breadth_pct")
        tape_block = f"""
--- MARKET TAPE (today's broad-market context) ---
Condition : {tc}
Summary   : {ts}
Key risk  : {tr}
VIX       : {vix if vix is not None else 'N/A'}
Breadth   : {f"{breadth}% sector ETFs above 50MA" if breadth is not None else 'N/A'}
Note: Tape is context only — it does not override position-sizing rules. A CAUTION/UNFAVORABLE tape warrants a WARN unless rules are clearly violated (ABORT).
"""

    is_rs = "rs_momentum" in (trigger or "").lower()
    min_rr    = 1.2 if is_rs else 2.0
    rs_rr_note = " (RS Momentum strategy uses EMA50 structural stops — minimum is 1.2x)" if is_rs else ""

    symbol_news = _fetch_alpaca_news([symbol], db, user_id, mode=mode).get(symbol, [])
    news_block  = (
        "\n--- RECENT NEWS (last 48h) ---\n" + "\n".join(f"• {h}" for h in symbol_news)
        if symbol_news else ""
    )

    prompt = f"""You are a risk management AI for a Minervini SEPA swing trading system.
Evaluate this proposed trade and respond in EXACTLY this format — no other text:

VERDICT: <PROCEED|WARN|ABORT>
REASON: <one sentence ≤20 words>
WARNINGS: <comma-separated flags, or "none">
{tape_block}
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
1. R:R must be >= {min_rr:.1f}x. Current: {rr:.2f}x{rs_rr_note}
2. Risk per trade must be <= {max_risk_pct}% of portfolio. Current: {risk_pct_port:.2f}%
3. Trade cost must not exceed buying power. Cost: ${trade_cost:,.2f}, BP: ${buying_power:,.2f}
4. Single position must not exceed {max_position_pct}% of portfolio. Current: {pct_of_portfolio:.1f}%
5. Cash after trade must remain >= {min_cash_pct}% of portfolio. After trade: {cash_after_pct:.1f}%
{news_block}
Use WARN for borderline cases (R:R within 0.3 of minimum, size near the limit).
If recent news reveals earnings miss, guidance cut, FDA rejection, or major legal/regulatory risk — use WARN or ABORT even if position sizing rules pass.
Use ABORT only for clear rule violations.
Use PROCEED when all rules pass."""

    try:
        text = _call_ai(db, prompt, max_tokens=256, user_id=user_id)
        if text is None:
            # _call_ai returns None when no key is set — already caught above,
            # but guard here too in case the key was removed mid-request.
            return {
                "proceed":  False,
                "verdict":  "HOLD",
                "reason":   "AI API key not configured — configure one in Settings to enable pre-trade analysis.",
                "warnings": ["No AI API key set. Go to Settings → AI to add your key."],
                "analysis": "",
            }
        return _parse_pre_trade_response(text.strip())

    except Exception as exc:
        # Transient API error — warn but allow trade to continue so a
        # temporary outage doesn't freeze all automated trading.
        logger.error("pre_trade_analysis failed for %s: %s", symbol, exc)
        return {
            "proceed":  True,
            "verdict":  "WARN",
            "reason":   f"AI analysis error — proceeding with caution: {str(exc)[:80]}",
            "warnings": [f"Pre-trade AI error (trade allowed): {str(exc)[:80]}"],
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

def analyze_picks(db: Session, picks: list[dict], closed_position: dict | None = None, user_id: int = None, mode: str = "paper") -> str:
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

    all_symbols = [p["symbol"] for p in picks]
    news_map    = _fetch_alpaca_news(all_symbols, db, user_id, mode=mode)

    lines = []
    for i, p in enumerate(picks, 1):
        ep = p.get("entry_price") or 0
        sp = p.get("stop_price")  or 0
        t1 = p.get("target1")     or 0
        rr = round((t1 - ep) / (ep - sp), 2) if ep > sp > 0 and t1 > ep else "N/A"
        src = p.get("screener_type", "minervini")
        score_raw = p.get("score", "?")
        score_str = f"RS-pct={score_raw}/99" if src == "rs_momentum" else f"score={score_raw}/6"
        lines.append(
            f"{i}. {p['symbol']}  {score_str}  signal={p.get('signal','?')}"
            f"  entry=${ep:.2f}  stop=${sp:.2f}  t1=${t1:.2f}  R:R={rr}"
            f"  status={p.get('status','?')}  note: {p.get('rationale','')}"
            + _news_block(news_map.get(p["symbol"], []))
        )
    parts.append("Current week's top picks:\n" + "\n".join(lines))
    parts.append(
        "You are a professional swing-trader assistant using Minervini SEPA criteria.\n"
        "For each PENDING pick above give a one-line recommendation: EXECUTE, WAIT, or SKIP "
        "with a brief reason (≤15 words). Consider score, R:R ratio, signal quality, and any recent news.\n"
        "If news reveals an earnings miss, guidance cut, FDA rejection, or legal/regulatory risk, flag it.\n"
        "RS Momentum picks (signal=RS_MOMENTUM) use EMA50 structural stops — R:R ≥1.2 is acceptable for them.\n"
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


def analyze_picks_structured(
    db: Session,
    picks: list[dict],
    tape_context: dict | None = None,
    user_id: int = None,
    mode: str = "paper",
) -> list[dict]:
    """
    Structured per-stock AI analysis for the weekly plan.

    Returns a list of dicts (one per PENDING pick):
    {
      "symbol":        str,
      "decision":      "EXECUTE" | "WAIT" | "SKIP",
      "entry_zone":    str,   # e.g. "Buy $152-$155 on EMA20 tag with volume"
      "exit_strategy": str,   # e.g. "50% at 2R, trail stop to BE; full exit 3R"
      "guardrails":    str,   # e.g. "Cut on daily close below $148"
      "rationale":     str,   # one sentence why
    }

    Non-PENDING picks are returned with decision='N/A' and empty fields.
    Raises ValueError if no API key configured.
    """
    if not get_user_setting(db, "ai_api_key", "", user_id):
        raise ValueError("AI API key not configured in Settings.")

    import json as _json

    pending = [p for p in picks if str(p.get("status", "")).upper() == "PENDING"]
    if not pending:
        return []

    news_map = _fetch_alpaca_news([p["symbol"] for p in pending], db, user_id, mode=mode)

    # Build pick context lines
    pick_lines = []
    for i, p in enumerate(pending, 1):
        ep   = float(p.get("entry_price") or 0)
        sp   = float(p.get("stop_price")  or 0)
        t1   = float(p.get("target1")     or 0)
        t2   = float(p.get("target2")     or 0)
        rr   = round((t1 - ep) / (ep - sp), 2) if ep > sp > 0 and t1 > ep else "N/A"
        src  = p.get("screener_type", "minervini")
        src_label = {
            "minervini":  "Minervini/SEPA",
            "pullback":   "Pullback-to-MA (PPST+EMA)",
            "rs_momentum": "RS Momentum (IBD-style)",
            "both":       "Both screeners",
        }.get(src, src)
        score_raw = p.get("score", "?")
        if src == "rs_momentum":
            score_str = f"RS-percentile={score_raw}/99 (plan rank, not SEPA score)"
        else:
            score_str = f"SEPA-score={score_raw}/6"
        pick_lines.append(
            f"{i}. [{p['symbol']}]  source={src_label}  {score_str}"
            f"  signal={p.get('signal','?')}"
            f"  entry=${ep:.2f}  stop=${sp:.2f}  t1=${t1:.2f}  t2=${t2:.2f}  R:R={rr}x"
            f"  note: {str(p.get('rationale',''))[:120]}"
            + _news_block(news_map.get(p["symbol"], []))
        )

    tape_block = ""
    if tape_context:
        c = tape_context.get("condition", "caution").upper()
        s = tape_context.get("summary", "")
        r = tape_context.get("key_risk", "")
        sigs = tape_context.get("signals", {})
        vix = sigs.get("vix")
        breadth = sigs.get("breadth_pct")
        tape_block = f"""
CURRENT MARKET TAPE:
  Condition : {c}
  Summary   : {s}
  Key risk  : {r}
  VIX       : {vix if vix is not None else 'N/A'}
  Breadth   : {f"{breadth}% sector ETFs above 50MA" if breadth is not None else 'N/A'}
"""

    symbols_list = ", ".join(p["symbol"] for p in pending)
    prompt = f"""You are a professional swing-trading analyst. Evaluate each pending pick and return a JSON array.

PICKS TO EVALUATE:
{chr(10).join(pick_lines)}
{tape_block}
INSTRUCTIONS:
- For each pick provide a DECISION (EXECUTE, WAIT, or SKIP) with specific, actionable guidance.
- entry_zone: exact price zone and condition for entry (e.g. "Buy $152-155 on EMA20 touch with vol surge")
- exit_strategy: scale-out plan using the provided targets (e.g. "Take 50% at 2R ${'{t1}'}, trail stop to BE at 3R")
- guardrails: concrete cut rule + any condition to avoid the trade (e.g. "Cut on daily close below ${'{stop}'}; skip if VIX > 30")
- rationale: one sentence max (≤20 words) explaining the decision. If news influenced the decision, briefly reference it.

Use EXECUTE for high-quality setups (SEPA score ≥5 and R:R ≥2 for Minervini/Pullback; RS Momentum picks require R:R ≥1.2 — their stop is structural EMA50 support, not a raw % stop).
Use WAIT for borderline setups worth monitoring.
Use SKIP for low-quality setups or when tape is unfavorable for that setup type. Do NOT penalise RS Momentum picks for R:R < 2 — they use a tighter stop methodology.
If recent news (provided in the pick's "news:" field) reveals an earnings miss, guidance cut, FDA rejection, analyst downgrade, or legal/regulatory risk — downgrade the decision by one level (EXECUTE→WAIT, WAIT→SKIP) and flag it in guardrails.

Respond ONLY with a valid JSON array. No markdown fences, no explanation.
[
  {{
    "symbol": "TICKER",
    "decision": "EXECUTE",
    "entry_zone": "...",
    "exit_strategy": "...",
    "guardrails": "...",
    "rationale": "..."
  }}
]

Symbols to include: {symbols_list}"""

    try:
        # 300 tokens per pick is comfortable with news headlines included
        max_tok = max(1500, len(pending) * 300)
        raw = _call_ai(db, prompt, max_tokens=max_tok, user_id=user_id)
        if raw is None:
            raise ValueError("AI API key not configured.")

        text = raw.strip()
        # Strip markdown fences
        if text.startswith("```"):
            parts = text.split("```")
            text = parts[1] if len(parts) > 1 else text
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()

        # Recover from truncated response: close the array if cut off mid-stream
        if not text.endswith("]"):
            last_complete = text.rfind("},")
            if last_complete == -1:
                last_complete = text.rfind("}")
            if last_complete != -1:
                text = text[: last_complete + 1] + "\n]"
                logger.warning("analyze_picks_structured: response truncated — recovered %d chars", len(text))

        parsed: list = _json.loads(text)
        if not isinstance(parsed, list):
            raise ValueError("AI did not return a JSON array")

        # Normalise
        result = []
        valid_syms = {p["symbol"] for p in pending}
        for item in parsed:
            sym = str(item.get("symbol", "")).upper()
            if sym not in valid_syms:
                continue
            result.append({
                "symbol":        sym,
                "decision":      str(item.get("decision", "WAIT")).upper(),
                "entry_zone":    str(item.get("entry_zone",    "") or ""),
                "exit_strategy": str(item.get("exit_strategy", "") or ""),
                "guardrails":    str(item.get("guardrails",    "") or ""),
                "rationale":     str(item.get("rationale",     "") or ""),
            })
        return result

    except Exception as exc:
        logger.error("analyze_picks_structured failed: %s", exc)
        raise


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