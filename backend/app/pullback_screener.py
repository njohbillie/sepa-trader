"""
Pullback-to-MA Screener
=======================
Pivot Point Supertrend (PPST) + EMA 20/50/100/200 filter set.

Two-pass architecture for speed:
  Pass 1 — TradingView batch API: hard-filter the universe in one HTTP call
            (~5 seconds for 120 symbols) using price, EMA alignment, RSI 40–60,
            volume, market-cap, relative-volume, 1-week-change, and EMA50 proximity.
  Pass 2 — Per-candidate (yfinance): compute PPST from OHLCV, check earnings date.
            Only runs on the survivors of pass 1 (~10–30 stocks).

All thresholds are configurable via user_settings with the 'pb_' prefix.
"""
import logging
import math
from datetime import date

import httpx
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from .database import get_user_setting
from .tradingview_client import to_tv_symbol

logger = logging.getLogger(__name__)

SCAN_URL = "https://scanner.tradingview.com/america/scan"

# TradingView columns for the pullback filter pass
_PB_COLS = [
    "close",
    "EMA20",
    "EMA50",
    "EMA100",
    "EMA200",
    "RSI",
    "Relative.Volume.10D",
    "average_volume_10d_calc",
    "volume",
    "market_cap_basic",
    "change_1W",
    "beta_1_year",
]

# ── Settings helpers ──────────────────────────────────────────────────────────

def get_pb_settings(db: Session, user_id: int) -> dict:
    """Load all pullback screener settings with defaults."""
    def _s(key, default):
        return get_user_setting(db, key, str(default), user_id)

    return {
        "price_min":         float(_s("pb_price_min",         10.0)),
        "price_max":         float(_s("pb_price_max",         200.0)),
        "ema_alignment":     _s("pb_ema_alignment",     "true") == "true",
        "price_above_ema20": _s("pb_price_above_ema20", "true") == "true",
        "rsi_min":           float(_s("pb_rsi_min",           40.0)),
        "rsi_max":           float(_s("pb_rsi_max",           60.0)),
        "avg_vol_min":       float(_s("pb_avg_vol_min",       1_000_000)),
        "rel_vol_min":       float(_s("pb_rel_vol_min",       0.75)),
        "market_cap_min":    float(_s("pb_market_cap_min",    500_000_000)),
        "week_change_min":   float(_s("pb_week_change_min",   -3.0)),
        "ema50_proximity":   float(_s("pb_ema50_proximity",   8.0)),
        "beta_max":          float(_s("pb_beta_max",          2.5)),
        "earnings_days_min": int(  _s("pb_earnings_days_min", 15)),
        "ppst_required":     _s("pb_ppst_required",     "true") == "true",
        "top_n":             int(  _s("pb_top_n",             5)),
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def run_pullback_screener(
    db: Session,
    mode: str = None,
    user_id: int = None,
) -> list[dict]:
    """
    Run the Pullback-to-MA screener.
    Returns a list of plan row dicts (NOT saved to DB — caller handles combined save).
    Each row includes screener_type='pullback'.
    """
    if mode is None:
        mode = get_user_setting(db, "trading_mode", "paper", user_id)

    cfg = get_pb_settings(db, user_id)

    # Universe: same as Minervini screener by default
    universe_raw = get_user_setting(db, "screener_universe", "", user_id)
    from .screener import DEFAULT_UNIVERSE
    universe = (
        [s.strip().upper() for s in universe_raw.split(",") if s.strip()]
        if universe_raw else DEFAULT_UNIVERSE
    )

    logger.info(
        "Pullback screener: scanning %d symbols (mode=%s, top_n=%d)…",
        len(universe), mode, cfg["top_n"],
    )

    # ── Pass 1: TradingView batch filter ─────────────────────────────────────
    candidates = _tv_filter(universe, cfg)
    logger.info("Pullback screener: %d candidates after TV filter", len(candidates))

    if not candidates:
        logger.info("Pullback screener: no candidates passed TV filter")
        return []

    # ── Pass 2: PPST + earnings (per-candidate) ───────────────────────────────
    scored = _score_candidates(candidates, cfg)
    logger.info("Pullback screener: %d candidates after PPST/earnings check", len(scored))

    if not scored:
        return []

    # Sort: PPST bullish first, then RSI closest to 50 (most reset)
    scored.sort(
        key=lambda x: (x["ppst_bullish"], -abs(x["rsi"] - 50)),
        reverse=True,
    )
    top = scored[:cfg["top_n"]]

    # ── Position sizing ───────────────────────────────────────────────────────
    from .screener import _get_portfolio_value, _next_monday

    account_value = _get_portfolio_value(db, mode, user_id)
    if account_value <= 0:
        logger.warning("Pullback screener: cannot fetch account value — skipping")
        return []

    risk_pct = float(get_user_setting(db, "risk_pct",         "2.0",  user_id))
    stop_pct = float(get_user_setting(db, "stop_loss_pct",    "8.0",  user_id))
    max_pos  = float(get_user_setting(db, "max_position_pct", "20.0", user_id) or "20.0")

    week_start   = _next_monday()
    risk_dollars = account_value * (risk_pct / 100)
    plan_rows    = []

    for rank, c in enumerate(top, 1):
        price    = float(c["price"])
        stop     = round(price * (1 - stop_pct / 100), 4)
        target1  = round(price * (1 + stop_pct * 2 / 100), 4)
        target2  = round(price * (1 + stop_pct * 3 / 100), 4)
        stop_d   = price - stop
        risk_sh  = int(risk_dollars / stop_d) if stop_d > 0 else 0
        max_sh   = int((account_value * max_pos / 100) / price) if price > 0 else 0
        shares   = min(risk_sh, max_sh)
        risk_amt = round(shares * stop_d, 2)

        parts = [
            f"Pullback screener. RSI {c['rsi']:.0f} (reset zone).",
            f"{'PPST bullish. ' if c['ppst_bullish'] else 'PPST not confirmed. '}",
            f"Price {c['ema50_pct']:.1f}% from EMA50.",
            "EMA alignment: 20>50>200.",
        ]
        if c.get("days_to_earnings") is not None:
            parts.append(f"Earnings ≥{c['days_to_earnings']}d away.")
        rationale = " ".join(p.strip() for p in parts)

        signal = "PULLBACK_EMA50" if c["ema50_pct"] <= 5.0 else "PULLBACK_EMA20"

        plan_rows.append({
            "week_start":    week_start.isoformat(),
            "symbol":        c["symbol"],
            "rank":          rank,
            "score":         c.get("score", 3),
            "signal":        signal,
            "entry_price":   price,
            "stop_price":    stop,
            "target1":       target1,
            "target2":       target2,
            "position_size": shares,
            "risk_amount":   risk_amt,
            "rationale":     rationale,
            "status":        "PENDING",
            "mode":          mode,
            "screener_type": "pullback",
        })

    logger.info(
        "Pullback screener done. Week of %s. Selected: %s",
        week_start, [r["symbol"] for r in plan_rows],
    )
    return plan_rows


# ── Pass 1: TradingView batch filter ─────────────────────────────────────────

def _tv_filter(universe: list[str], cfg: dict) -> list[dict]:
    """Single TradingView POST — returns candidates that pass all hard filters."""
    tv_syms = [to_tv_symbol(s) for s in universe]

    try:
        resp = httpx.post(
            SCAN_URL,
            json={
                "symbols": {"tickers": tv_syms, "query": {"types": []}},
                "columns": _PB_COLS,
            },
            timeout=30,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Origin":  "https://www.tradingview.com",
                "Referer": "https://www.tradingview.com/",
            },
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.error("Pullback TV scan failed: %s", exc)
        return []

    candidates = []
    for row in resp.json().get("data", []):
        sym = row["s"].split(":")[-1]
        v   = dict(zip(_PB_COLS, row["d"]))
        c   = _apply_tv_filters(sym, v, cfg)
        if c is not None:
            candidates.append(c)

    return candidates


def _apply_tv_filters(sym: str, v: dict, cfg: dict) -> dict | None:
    """Returns a candidate dict if the stock passes all TV-based filters, else None."""
    close = v.get("close") or 0
    if not close:
        return None

    # ── Price range ───────────────────────────────────────────────────────────
    if cfg["price_min"] > 0 and close < cfg["price_min"]:
        return None
    if cfg["price_max"] > 0 and close > cfg["price_max"]:
        return None

    e20  = v.get("EMA20")  or 0
    e50  = v.get("EMA50")  or 0
    e200 = v.get("EMA200") or 0

    # ── EMA alignment: EMA20 > EMA50 > EMA200 ────────────────────────────────
    if cfg["ema_alignment"] and not (e20 and e50 and e200 and e20 > e50 and e50 > e200):
        return None

    # ── Price above EMA20 ─────────────────────────────────────────────────────
    if cfg["price_above_ema20"] and e20 and close < e20:
        return None

    # ── RSI in reset zone 40–60 ───────────────────────────────────────────────
    rsi = v.get("RSI") or 0
    if rsi and (rsi < cfg["rsi_min"] or rsi > cfg["rsi_max"]):
        return None

    # ── Average volume 10D ────────────────────────────────────────────────────
    avg_vol10 = v.get("average_volume_10d_calc") or 0
    if avg_vol10 < cfg["avg_vol_min"]:
        return None

    # ── Relative volume ───────────────────────────────────────────────────────
    rel_vol = v.get("Relative.Volume.10D") or 0
    if rel_vol < cfg["rel_vol_min"]:
        return None

    # ── Market cap ────────────────────────────────────────────────────────────
    mcap = v.get("market_cap_basic") or 0
    if mcap < cfg["market_cap_min"]:
        return None

    # ── 1-week change ─────────────────────────────────────────────────────────
    w_change = v.get("change_1W")
    if w_change is not None and w_change < cfg["week_change_min"]:
        return None

    # ── Beta ──────────────────────────────────────────────────────────────────
    beta = v.get("beta_1_year")
    if beta is not None and beta > cfg["beta_max"]:
        return None

    # ── EMA50 proximity (the actual pullback condition) ───────────────────────
    ema50_pct = abs(close - e50) / e50 * 100 if e50 else 0
    if e50 and ema50_pct > cfg["ema50_proximity"]:
        return None

    return {
        "symbol":    sym,
        "price":     close,
        "rsi":       rsi or 50.0,
        "ema20":     e20,
        "ema50":     e50,
        "ema200":    e200,
        "ema50_pct": ema50_pct,
        "avg_vol10": avg_vol10,
        "rel_vol":   rel_vol,
        "market_cap": mcap,
        "w_change":  w_change,
    }


# ── Pass 2: PPST + earnings ───────────────────────────────────────────────────

def _score_candidates(candidates: list[dict], cfg: dict) -> list[dict]:
    """
    For each candidate: compute PPST from OHLCV, check earnings date.
    Returns the filtered + scored subset.
    """
    from .strategies.yf_client import fetch_ohlcv, get_next_earnings_date

    today = date.today()
    out   = []

    for c in candidates:
        sym = c["symbol"]
        try:
            # ── PPST via OHLCV ────────────────────────────────────────────────
            df = fetch_ohlcv(sym, period_days=60)
            if df.empty or len(df) < 16:
                logger.debug("Pullback: %s skipped — insufficient OHLCV data", sym)
                continue

            ppst_bullish = _calc_ppst(df)

            if cfg["ppst_required"] and not ppst_bullish:
                logger.debug("Pullback: %s skipped — PPST not bullish", sym)
                continue

            # ── Earnings gate ─────────────────────────────────────────────────
            days_to_earnings = None
            if cfg["earnings_days_min"] > 0:
                next_earn = get_next_earnings_date(sym)
                if next_earn is not None:
                    days_to_earnings = (next_earn - today).days
                    if 0 <= days_to_earnings < cfg["earnings_days_min"]:
                        logger.debug(
                            "Pullback: %s skipped — earnings in %d days",
                            sym, days_to_earnings,
                        )
                        continue

            # ── Score (1–5) ───────────────────────────────────────────────────
            score = 3
            if ppst_bullish:
                score += 1
            if 45 <= c["rsi"] <= 55:
                score += 1

            out.append({
                **c,
                "ppst_bullish":     ppst_bullish,
                "days_to_earnings": days_to_earnings,
                "score":            score,
            })

        except Exception as exc:
            logger.debug("Pullback: %s failed in pass-2: %s", sym, exc)
            continue

    return out


# ── Pivot Point Supertrend ────────────────────────────────────────────────────

def _calc_ppst(df: pd.DataFrame, period: int = 14, multiplier: float = 2.0) -> bool:
    """
    Compute Pivot Point Supertrend (PPST) direction for the most recent bar.

    1. Pivot = (High + Low + Close) / 3
    2. ATR(14) — Wilder's smoothed
    3. Upper band = Pivot + multiplier × ATR  (only moves down)
    4. Lower band = Pivot − multiplier × ATR  (only moves up)
    5. Bullish when Close > lower band; bearish when Close < upper band

    Returns True = bullish (price riding above PPST support).
    """
    try:
        high  = df["High"].to_numpy(dtype=float)
        low   = df["Low"].to_numpy(dtype=float)
        close = df["Close"].to_numpy(dtype=float)
        n     = len(close)

        if n < period + 2:
            return False

        pivot = (high + low + close) / 3.0

        # True Range
        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]
        tr = np.maximum.reduce([
            high - low,
            np.abs(high - prev_close),
            np.abs(low  - prev_close),
        ])

        # Wilder ATR
        atr = np.zeros(n)
        atr[period] = tr[1:period + 1].mean()
        for i in range(period + 1, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

        upper_raw = pivot + multiplier * atr
        lower_raw = pivot - multiplier * atr

        # Adaptive bands + direction
        upper     = upper_raw.copy()
        lower     = lower_raw.copy()
        direction = np.ones(n, dtype=bool)   # True = bullish

        for i in range(period + 1, n):
            # Lower band only moves up
            lower[i] = (
                lower_raw[i]
                if lower_raw[i] > lower[i - 1] or close[i - 1] < lower[i - 1]
                else lower[i - 1]
            )
            # Upper band only moves down
            upper[i] = (
                upper_raw[i]
                if upper_raw[i] < upper[i - 1] or close[i - 1] > upper[i - 1]
                else upper[i - 1]
            )
            # Trend flip
            if direction[i - 1] and close[i] < lower[i]:
                direction[i] = False
            elif not direction[i - 1] and close[i] > upper[i]:
                direction[i] = True
            else:
                direction[i] = direction[i - 1]

        return bool(direction[-1])

    except Exception as exc:
        logger.debug("PPST calc failed: %s", exc)
        return False
