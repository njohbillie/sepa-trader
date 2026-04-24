"""
Relative Strength (RS) Momentum Screener
=========================================
Ranks the US stock market by weighted price momentum (IBD-style RS Rating),
filters to the top percentile, and confirms each pick is in a Stage 2 uptrend.

Single-pass architecture: one TradingView batch call fetches performance data
for up to 400 liquid US stocks. RS scores are computed locally and ranked.
No per-symbol calls needed — TV provides all required performance columns.

RS Score formula (weighted toward recency, same logic as IBD RS Rating):
    rs_score = Perf.3M × 0.40 + Perf.6M × 0.20 + Perf.1Y × 0.20 + Perf.1M × 0.20

All thresholds are configurable via user_settings with the 'rs_' prefix.
"""
import logging
from datetime import date, timedelta

import httpx
from sqlalchemy.orm import Session

from .database import get_user_setting

logger = logging.getLogger(__name__)

SCAN_URL = "https://scanner.tradingview.com/america/scan"

_RS_COLS = [
    "close",
    "EMA50",
    "EMA200",
    "volume",
    "average_volume_30d_calc",
    "market_cap_basic",
    "Perf.1M",
    "Perf.3M",
    "Perf.6M",
    "Perf.1Y",
    "sector",
    "exchange",
]

_TV_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin":  "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
}

# Sectors that don't fit a momentum strategy — excluded by default
_DEFAULT_EXCLUDED_SECTORS = {
    "Consumer Defensive",
    "Energy",
    "Utilities",
    "Real Estate",
    "Basic Materials",
}


def _rs_score(v: dict) -> float:
    """IBD-weighted RS score from TV performance columns."""
    p1m = v.get("Perf.1M") or 0.0
    p3m = v.get("Perf.3M") or 0.0
    p6m = v.get("Perf.6M") or 0.0
    p1y = v.get("Perf.1Y") or 0.0
    return (p3m * 0.40) + (p6m * 0.20) + (p1y * 0.20) + (p1m * 0.20)


def _next_monday() -> date:
    today      = date.today()
    days_ahead = (7 - today.weekday()) % 7 or 7
    return today + timedelta(days=days_ahead)


def get_rs_settings(db: Session, user_id: int) -> dict:
    """Load all RS screener settings with defaults."""
    def _s(key, default):
        return get_user_setting(db, key, str(default), user_id)

    return {
        "price_min":       float(_s("rs_price_min",       10.0)),
        "price_max":       float(_s("rs_price_max",       0.0)),    # 0 = no ceiling
        "avg_vol_min":     float(_s("rs_avg_vol_min",     500_000)),
        "market_cap_min":  float(_s("rs_market_cap_min",  500_000_000)),
        "min_percentile":  float(_s("rs_min_percentile",  70.0)),   # top 30%
        "require_stage2":  _s("rs_require_stage2", "true") == "true",
        "max_extension":   float(_s("rs_max_extension",   15.0)),   # % above EMA50
        "top_n":           int(  _s("rs_top_n",           5)),
        "excluded_sectors": [
            s.strip()
            for s in _s("rs_excluded_sectors", ",".join(_DEFAULT_EXCLUDED_SECTORS)).split(",")
            if s.strip()
        ],
        "exchanges": [
            e.strip().upper()
            for e in _s("rs_exchanges", "NYSE,NASDAQ").split(",")
            if e.strip()
        ],
    }


def _build_tv_filters(cfg: dict) -> list[dict]:
    """Build TradingView server-side filter conditions."""
    filters = []

    # Price
    filters.append({"left": "close", "operation": "greater", "right": cfg["price_min"]})
    if cfg["price_max"] > 0:
        filters.append({"left": "close", "operation": "less", "right": cfg["price_max"]})

    # Volume
    filters.append({
        "left": "average_volume_30d_calc",
        "operation": "greater",
        "right": cfg["avg_vol_min"],
    })

    # Market cap
    filters.append({
        "left": "market_cap_basic",
        "operation": "greater",
        "right": cfg["market_cap_min"],
    })

    # Stage 2: EMA alignment (server-side pre-filter — local check adds extension guard)
    if cfg["require_stage2"]:
        filters.append({"left": "EMA50",  "operation": "greater", "right": "EMA200"})
        filters.append({"left": "close",  "operation": "greater", "right": "EMA50"})

    return filters


def run_rs_screener(
    db: Session,
    mode: str = None,
    user_id: int = None,
    account_value: float = None,
) -> list[dict]:
    """
    Scan the US market for top RS momentum stocks.
    Returns a list of weekly_plan-compatible row dicts (not saved — caller handles).
    Each row has screener_type='rs_momentum'.
    """
    if mode is None:
        mode = get_user_setting(db, "trading_mode", "paper", user_id)

    cfg      = get_rs_settings(db, user_id)
    risk_pct = float(get_user_setting(db, "risk_pct",        "2.0",  user_id) or "2.0")
    stop_pct = float(get_user_setting(db, "stop_loss_pct",   "8.0",  user_id) or "8.0")
    max_ppct = float(get_user_setting(db, "max_position_pct", "20.0", user_id) or "20.0")

    logger.info(
        "RS screener: scanning US market via TradingView (stage2=%s, min_pct=%.0f)…",
        cfg["require_stage2"], cfg["min_percentile"],
    )

    # ── Pass 1: TradingView batch scan ────────────────────────────────────────
    try:
        resp = httpx.post(
            SCAN_URL,
            json={
                "filter":  _build_tv_filters(cfg),
                "columns": _RS_COLS,
                "range":   [0, 400],
                "sort":    {"sortBy": "market_cap_basic", "sortOrder": "desc"},
                "markets": ["america"],
            },
            timeout=30,
            headers=_TV_HEADERS,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.error("RS screener: TradingView batch call failed: %s", exc)
        return []

    raw_rows = resp.json().get("data", [])
    logger.info("RS screener: TV returned %d pre-filtered candidates.", len(raw_rows))

    excluded_sectors = {s.lower() for s in cfg["excluded_sectors"]}
    allowed_exchanges = cfg["exchanges"]

    # ── Pass 2: Local scoring and filtering ───────────────────────────────────
    scored: list[dict] = []

    for row in raw_rows:
        sym = row["s"].split(":")[-1]
        v   = dict(zip(_RS_COLS, row["d"]))

        price  = v.get("close")                or 0.0
        ema50  = v.get("EMA50")                or 0.0
        ema200 = v.get("EMA200")               or 0.0
        avgvol = v.get("average_volume_30d_calc") or 0.0
        mktcap = v.get("market_cap_basic")     or 0.0
        sector = (v.get("sector") or "").strip()
        exch   = (v.get("exchange") or "").strip().upper()

        # Exchange filter
        if allowed_exchanges and exch not in allowed_exchanges:
            continue

        # Sector exclusion
        if sector.lower() in excluded_sectors:
            continue

        # Extension guard: too far above EMA50 = chasing
        if cfg["require_stage2"] and ema50 > 0:
            extension_pct = (price - ema50) / ema50 * 100
            if extension_pct > cfg["max_extension"]:
                continue

        # RS score — skip stocks with no meaningful performance data
        score = _rs_score(v)
        if score <= -50:   # deep negative momentum — not a candidate
            continue

        scored.append({"symbol": sym, "rs_score": score, "price": price, "tv": v})

    if not scored:
        logger.info("RS screener: no candidates after local filtering.")
        return []

    # Rank by RS score descending
    scored.sort(key=lambda x: x["rs_score"], reverse=True)

    # Apply percentile cutoff — keep only the top N% of this batch
    cutoff_idx = max(1, int(len(scored) * (1 - cfg["min_percentile"] / 100)))
    top_bucket = scored[:cutoff_idx]

    logger.info(
        "RS screener: %d/%d passed local filters, top %d by RS (≥%dth percentile)",
        len(scored), len(raw_rows), len(top_bucket), int(cfg["min_percentile"]),
    )

    # ── Position sizing ───────────────────────────────────────────────────────
    if account_value is None:
        try:
            from . import alpaca_client as alp
            acct = alp.get_account(mode)
            account_value = float(acct.portfolio_value)
        except Exception as exc:
            logger.error("RS screener: cannot fetch account value: %s", exc)
            return []

    week_start = _next_monday().isoformat()
    total      = len(scored)
    plan_rows: list[dict] = []

    for i, item in enumerate(top_bucket[:cfg["top_n"]], 1):
        sym   = item["symbol"]
        price = item["price"]
        v     = item["tv"]
        ema50 = v.get("EMA50") or 0.0

        # Stop: just below EMA50 (natural structural support for Stage 2 stocks)
        # Using half the stop_pct below EMA50 — tighter than a raw % from price
        stop = round(ema50 * (1 - (stop_pct / 100) / 2), 2) if ema50 > 0 \
               else round(price * (1 - stop_pct / 100), 2)

        stop_distance = price - stop
        if stop_distance <= 0:
            continue

        target1 = round(price * (1 + 2 * stop_pct / 100), 2)   # 2R
        target2 = round(price * (1 + 3 * stop_pct / 100), 2)   # 3R

        # Risk-based position sizing, capped at max_position_pct
        risk_dollars = account_value * risk_pct / 100
        shares       = int(risk_dollars / stop_distance)
        max_shares   = int(account_value * max_ppct / 100 / price) if price > 0 else 0
        shares       = min(shares, max_shares)

        if shares < 1:
            continue

        risk_amount   = round(shares * stop_distance, 2)
        rs_percentile = int((1 - (i - 1) / total) * 99) if total > 0 else 50

        p1m = v.get("Perf.1M") or 0.0
        p3m = v.get("Perf.3M") or 0.0
        p6m = v.get("Perf.6M") or 0.0
        p1y = v.get("Perf.1Y") or 0.0

        rationale = (
            f"RS Rank #{i} of {len(top_bucket)} | RS Score {item['rs_score']:.1f} | "
            f"1M:{p1m:+.1f}%  3M:{p3m:+.1f}%  6M:{p6m:+.1f}%  1Y:{p1y:+.1f}% | "
            f"Stage 2 uptrend. Stop at EMA50 ${stop:.2f}."
        )

        plan_rows.append({
            "week_start":    week_start,
            "symbol":        sym,
            "rank":          i,
            "score":         rs_percentile,
            "signal":        "RS_MOMENTUM",
            "entry_price":   round(price, 2),
            "stop_price":    stop,
            "target1":       target1,
            "target2":       target2,
            "position_size": shares,
            "risk_amount":   risk_amount,
            "rationale":     rationale,
            "status":        "PENDING",
            "mode":          mode,
            "screener_type": "rs_momentum",
        })

    logger.info(
        "RS screener done: %d picks selected (mode=%s, account=$%.0f)",
        len(plan_rows), mode, account_value,
    )
    return plan_rows
