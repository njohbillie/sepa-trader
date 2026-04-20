"""
Sunday screener: scans a configurable universe of stocks using the Minervini
8-point SEPA criteria, selects top 10 candidates, generates a weekly trading
plan, and saves it to the weekly_plan table.

Uses TradingView's scanner API — all symbols fetched in one batch request.
"""
import logging
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from .tv_analyzer import batch_analyze
from .database import get_setting, set_setting

logger = logging.getLogger(__name__)

# Default universe: top ~120 liquid US stocks across S&P 500 / NASDAQ 100.
# Users can override via the screener_universe setting (comma-separated).
DEFAULT_UNIVERSE = [
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO",
    # Semiconductors
    "AMD", "QCOM", "MU", "TXN", "KLAC", "LRCX", "AMAT", "MRVL", "ON", "MPWR",
    # Software / Cloud
    "CRM", "ADBE", "ORCL", "NOW", "INTU", "PANW", "CRWD", "SNOW", "DDOG", "ZS",
    "FTNT", "TEAM", "ANSS", "CDNS", "VEEV", "WDAY", "PCTY",
    # Financials
    "JPM", "BAC", "GS", "MS", "V", "MA", "WFC", "BX", "AXP", "SPGI",
    # Healthcare / Biotech
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "TMO", "ISRG", "REGN", "VRTX",
    "DXCM", "IDXX", "MRNA",
    # Consumer / Retail
    "COST", "WMT", "HD", "NKE", "MCD", "SBUX", "TJX", "LULU", "DECK", "ONON",
    # Energy
    "XOM", "CVX", "COP", "SLB",
    # Industrials
    "CAT", "DE", "HON", "LMT", "RTX", "GE", "UNP", "CSX",
    # Communications / Media
    "NFLX", "DIS", "CMCSA", "TMUS", "GOOGL",
    # High-growth / Breakout candidates
    "UBER", "ABNB", "MELI", "SHOP", "TTD", "ENPH", "FSLR", "CELH", "AXON",
    "SMCI", "APP", "PLTR", "HIMS",
]
# Deduplicate preserving order
DEFAULT_UNIVERSE = list(dict.fromkeys(DEFAULT_UNIVERSE))


def _next_monday() -> date:
    today = date.today()
    days_ahead = 7 - today.weekday()  # Monday = 0, so 7 - 0 = 7 next Monday
    if days_ahead == 7 and today.weekday() == 0:
        days_ahead = 7  # already Monday, plan for next Monday
    # If Sunday (6), days_ahead = 1 → next Monday is tomorrow
    days_ahead = (7 - today.weekday()) % 7 or 7
    return today + timedelta(days=days_ahead)



def _generate_rationale(symbol: str, result: dict) -> str:
    score  = result.get("score", 0)
    signal = result.get("signal", "")
    price  = result.get("price") or 0
    w52h   = result.get("week52_high") or 0
    w52l   = result.get("week52_low") or 0

    parts = [f"Score {score}/8 — {signal}."]
    if price and w52h:
        pct_below = (w52h - price) / w52h * 100
        parts.append(f"${price:.2f}, {pct_below:.1f}% below 52w high.")
    if price and w52l:
        pct_above = (price - w52l) / w52l * 100
        parts.append(f"Up {pct_above:.1f}% from 52w low.")
    if result.get("vol_surge"):
        parts.append("Volume surge detected.")
    if result.get("above_pivot"):
        parts.append("Trading above 20-day pivot.")
    return " ".join(parts)


def run_screener(db: Session, mode: str = None) -> list[dict]:
    """
    Scan the stock universe, select top-N SEPA candidates, save to
    weekly_plan table, and update the watchlist setting.
    Returns a list of plan row dicts.
    """
    if mode is None:
        mode = get_setting(db, "trading_mode", "paper")

    risk_pct = float(get_setting(db, "risk_pct", "2.0"))
    stop_pct = float(get_setting(db, "stop_loss_pct", "8.0"))

    # --- Screener filter settings ---
    price_min        = float(get_setting(db, "screener_price_min",      "0")   or "0")
    price_max        = float(get_setting(db, "screener_price_max",      "0")   or "0")
    top_n            = int(  get_setting(db, "screener_top_n",          "10")  or "10")
    min_score_floor  = int(  get_setting(db, "screener_min_score",      "0")   or "0")
    vol_surge_pct    = float(get_setting(db, "screener_vol_surge_pct",  "40")  or "40")
    ema20_pct        = float(get_setting(db, "screener_ema20_pct",      "2.0") or "2.0")
    ema50_pct        = float(get_setting(db, "screener_ema50_pct",      "3.0") or "3.0")

    account = _get_portfolio_value(mode)

    universe_raw = get_setting(db, "screener_universe", "")
    universe = (
        [s.strip().upper() for s in universe_raw.split(",") if s.strip()]
        if universe_raw
        else DEFAULT_UNIVERSE
    )

    logger.info("Screener: scanning %d symbols via TradingView (mode=%s)...", len(universe), mode)

    # Single batch call — all symbols in one TradingView scanner request
    results_map = batch_analyze(universe, vol_surge_pct=vol_surge_pct, ema20_pct=ema20_pct, ema50_pct=ema50_pct)

    # Build scored list; apply price filter
    all_scored = []
    for sym, result in results_map.items():
        if not result.get("price") or result.get("signal") in ("ERROR", "INSUFFICIENT_DATA"):
            continue
        price = float(result["price"])
        if price_min > 0 and price < price_min:
            continue
        if price_max > 0 and price > price_max:
            continue
        all_scored.append({"symbol": sym, **result})

    all_scored.sort(
        key=lambda x: (x["score"], int(bool(x.get("vol_surge"))), int(bool(x.get("above_pivot")))),
        reverse=True,
    )

    errors    = sum(1 for r in results_map.values() if r.get("signal") in ("ERROR", "INSUFFICIENT_DATA"))
    top_score = all_scored[0]["score"] if all_scored else 0

    # Adaptive threshold — max score via TV scanner is 6/8.
    # If user set a manual floor, respect it; otherwise step down until 5+ candidates.
    if min_score_floor > 0:
        candidates = [c for c in all_scored if c["score"] >= min_score_floor]
    else:
        candidates = []
        for min_score in (6, 5, 4, 3):
            candidates = [c for c in all_scored if c["score"] >= min_score]
            if len(candidates) >= 5:
                break

    top_picks = candidates[:top_n] if candidates else []

    effective_min = min_score_floor if min_score_floor > 0 else (
        next((s for s in (6, 5, 4, 3) if len([c for c in all_scored if c["score"] >= s]) >= 5), 3)
    )
    summary_msg = (
        f"Screener: scanned {len(universe)}, "
        f"errors {errors}, "
        f"scored {len(all_scored)}, "
        f"qualifying (>={effective_min}) {len(candidates)}, "
        f"selected {len(top_picks)}. "
        f"Top score: {top_score}/8."
    )
    if price_min > 0 or price_max > 0:
        summary_msg += f" Price filter: ${price_min:.0f}–${price_max:.0f}." if price_max > 0 else f" Price min: ${price_min:.0f}."

    # If high error rate, surface a sample error to aid debugging
    if errors > len(universe) * 0.5:
        sample_errors = [
            f"{sym}: {r['error']}"
            for sym, r in list(results_map.items())[:3]
            if r.get("signal") == "ERROR" and r.get("error")
        ]
        if sample_errors:
            summary_msg += " Sample errors: " + " | ".join(sample_errors)

    logger.info(summary_msg)
    _log_alert(db, "INFO", summary_msg)

    week_start   = _next_monday()
    risk_dollars = account * (risk_pct / 100)
    plan_rows    = []

    for rank, c in enumerate(top_picks, 1):
        price   = float(c["price"])
        stop    = round(price * (1 - stop_pct / 100), 4)
        target1 = round(price * (1 + stop_pct * 2 / 100), 4)
        target2 = round(price * (1 + stop_pct * 3 / 100), 4)
        stop_d  = price - stop
        shares  = int(risk_dollars / stop_d) if stop_d > 0 else 0
        risk_amt = round(shares * stop_d, 2)

        plan_rows.append({
            "week_start":    week_start.isoformat(),
            "symbol":        c["symbol"],
            "rank":          rank,
            "score":         c["score"],
            "signal":        c.get("signal", "STAGE2_WATCH"),
            "entry_price":   price,
            "stop_price":    stop,
            "target1":       target1,
            "target2":       target2,
            "position_size": shares,
            "risk_amount":   risk_amt,
            "rationale":     _generate_rationale(c["symbol"], c),
            "status":        "PENDING",
            "mode":          mode,
        })

    # Always save (even empty) so last-run info is queryable
    _save_plan(db, plan_rows, week_start.isoformat(), mode)
    set_setting(db, "screener_last_run", summary_msg)

    if plan_rows:
        top_symbols   = [r["symbol"] for r in plan_rows]
        set_setting(db, "watchlist", ",".join(top_symbols))

        tv_user = get_setting(db, "tv_username", "")
        tv_pass = get_setting(db, "tv_password", "")
        if tv_user and tv_pass:
            from .tradingview_client import update_weekly_picks
            tv_result = update_weekly_picks(tv_user, tv_pass, top_symbols)
            if tv_result["ok"]:
                logger.info("TradingView weekly_picks %s.", tv_result["action"])
            else:
                logger.warning("TradingView sync failed: %s", tv_result["error"])

    logger.info("Screener complete. Week of %s. Plan: %s", week_start, [r["symbol"] for r in plan_rows])
    return plan_rows


def _log_alert(db: Session, level: str, message: str):
    try:
        db.execute(
            text("INSERT INTO alert_log (level, message) VALUES (:l, :m)"),
            {"l": level, "m": message},
        )
        db.commit()
    except Exception:
        pass


def _get_portfolio_value(mode: str) -> float:
    try:
        from . import alpaca_client as alp
        acct = alp.get_account(mode)
        return float(acct.portfolio_value)
    except Exception:
        return 10000.0


def _save_plan(db: Session, rows: list[dict], week_start: str, mode: str):
    db.execute(
        text("DELETE FROM weekly_plan WHERE week_start = :w AND mode = :m"),
        {"w": week_start, "m": mode},
    )
    for r in rows:
        db.execute(
            text("""
                INSERT INTO weekly_plan
                    (week_start, symbol, rank, score, signal, entry_price, stop_price,
                     target1, target2, position_size, risk_amount, rationale, status, mode)
                VALUES (:week_start, :symbol, :rank, :score, :signal, :entry_price, :stop_price,
                        :target1, :target2, :position_size, :risk_amount, :rationale, :status, :mode)
            """),
            r,
        )
    db.commit()
