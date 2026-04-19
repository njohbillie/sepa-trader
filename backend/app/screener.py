"""
Sunday screener: scans a configurable universe of stocks using the Minervini
8-point SEPA criteria, selects top 10 candidates, generates a weekly trading
plan, and saves it to the weekly_plan table.

Runs in parallel (10 workers) using the same analyze() function as the hourly
monitor for consistency.
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from .sepa_analyzer import analyze
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


def _analyze_safe(symbol: str) -> tuple[str, dict]:
    try:
        result = analyze(symbol)
        return symbol, result
    except Exception as exc:
        logger.warning("screener: %s failed: %s", symbol, exc)
        return symbol, {"signal": "ERROR", "score": 0, "price": None}


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
    Scan the stock universe, select top 10 SEPA candidates, save to
    weekly_plan table, and update the watchlist setting.
    Returns a list of plan row dicts.
    """
    if mode is None:
        mode = get_setting(db, "trading_mode", "paper")

    risk_pct = float(get_setting(db, "risk_pct", "2.0"))
    stop_pct = float(get_setting(db, "stop_loss_pct", "8.0"))

    # Try to get portfolio value from Alpaca; fall back to settings
    account = _get_portfolio_value(mode)

    # Get universe
    universe_raw = get_setting(db, "screener_universe", "")
    universe = (
        [s.strip().upper() for s in universe_raw.split(",") if s.strip()]
        if universe_raw
        else DEFAULT_UNIVERSE
    )

    logger.info("Screener: scanning %d symbols (mode=%s)...", len(universe), mode)

    # Parallel analysis
    results_map: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_analyze_safe, sym): sym for sym in universe}
        for future in as_completed(futures):
            sym, result = future.result()
            results_map[sym] = result

    # Filter: need score >= 6 and a valid price
    candidates = []
    for sym, result in results_map.items():
        score = result.get("score", 0)
        price = result.get("price")
        if score >= 6 and price:
            candidates.append({"symbol": sym, **result})

    # Sort: score DESC, then vol_surge as tiebreaker
    candidates.sort(
        key=lambda x: (x["score"], int(bool(x.get("vol_surge"))), int(bool(x.get("above_pivot")))),
        reverse=True,
    )
    top10 = candidates[:10]

    if not top10:
        logger.warning("Screener: no candidates found with score >= 6")
        return []

    week_start   = _next_monday()
    risk_dollars = account * (risk_pct / 100)
    plan_rows    = []

    for rank, c in enumerate(top10, 1):
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

    _save_plan(db, plan_rows, week_start.isoformat(), mode)

    # Update watchlist so the hourly monitor targets these stocks during the week
    watchlist_csv = ",".join(r["symbol"] for r in plan_rows)
    set_setting(db, "watchlist", watchlist_csv)

    logger.info(
        "Screener complete. Week of %s. Plan: %s",
        week_start,
        [r["symbol"] for r in plan_rows],
    )
    return plan_rows


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
