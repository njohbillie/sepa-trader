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
from datetime import date, datetime, timezone

import httpx
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from .database import get_user_setting
from .tradingview_client import to_tv_symbol

logger = logging.getLogger(__name__)

SCAN_URL = "https://scanner.tradingview.com/america/scan"

# TradingView columns fetched for every candidate.
# Only confirmed-valid column names — relative volume is derived locally
# from volume / average_volume_30d_calc.
_PB_COLS = [
    "close",
    "EMA20",
    "EMA50",
    "EMA100",
    "EMA200",
    "RSI",
    "volume",
    "average_volume_30d_calc",
    "market_cap_basic",
    "earnings_release_next_date",   # Unix timestamp — skip Yahoo Finance round-trip when present
    "price_52_week_high",
    "ADX",
    "Perf.1M",
    "Perf.3M",
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

# ── Settings helpers ──────────────────────────────────────────────────────────

def get_pb_settings(db: Session, user_id: int) -> dict:
    """Load all pullback screener settings with defaults."""
    def _s(key, default):
        return get_user_setting(db, key, str(default), user_id)

    return {
        "price_min":           float(_s("pb_price_min",           10.0)),
        "price_max":           float(_s("pb_price_max",           200.0)),
        # EMA ladder — each step is independently togglable
        "price_above_ema20":   _s("pb_price_above_ema20",   "true") == "true",
        "ema20_above_ema50":   _s("pb_ema20_above_ema50",   "true") == "true",
        "ema50_above_ema100":  _s("pb_ema50_above_ema100",  "true") == "true",
        "ema100_above_ema200": _s("pb_ema100_above_ema200", "true") == "true",
        "rsi_min":             float(_s("pb_rsi_min",           40.0)),
        "rsi_max":             float(_s("pb_rsi_max",           60.0)),
        "avg_vol_min":         float(_s("pb_avg_vol_min",       1_000_000)),
        # rel_vol_min: derived from volume / average_volume_30d_calc
        "rel_vol_min":         float(_s("pb_rel_vol_min",       0.75)),
        "market_cap_min":      float(_s("pb_market_cap_min",    500_000_000)),
        "ema50_proximity":     float(_s("pb_ema50_proximity",   8.0)),
        "earnings_days_min":   int(  _s("pb_earnings_days_min", 15)),
        "ppst_required":          _s("pb_ppst_required",     "true") == "true",
        "ppst_pivot_period":      int(  _s("pb_ppst_pivot_period",  2)),    # TV: Pivot Point Period
        "ppst_atr_factor":        float(_s("pb_ppst_multiplier",   3.0)),   # TV: ATR Factor
        "ppst_atr_period":        int(  _s("pb_ppst_period",       10)),    # TV: ATR Period
        # Earnings: block stocks whose next earnings date cannot be determined
        "block_unknown_earnings": _s("pb_block_unknown_earnings", "true") == "true",
        "top_n":                  int(  _s("pb_top_n",             5)),
        "ema_spread_min":     float(_s("pb_ema_spread_min",     1.0)),   # min % EMA20 > EMA50
        "adx_min":            float(_s("pb_adx_min",            20.0)),  # min ADX (trend strength)
        "52w_high_pct_max":   float(_s("pb_52w_high_pct_max",   30.0)),  # max % below 52W high
        "perf_3m_min":        float(_s("pb_3m_perf_min",        -5.0)),  # min 3-month performance %
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

    # ── Pass 1: TradingView filter ────────────────────────────────────────────
    # Option B: user has named a saved TV screener → use it directly
    tv_screener_name = get_user_setting(db, "pb_tv_screener_name", "", user_id).strip()
    tv_user = get_user_setting(db, "tv_username", "", user_id)
    tv_pass = get_user_setting(db, "tv_password", "", user_id)

    if tv_screener_name and tv_user and tv_pass:
        logger.info(
            "Pullback screener [Option B]: using saved TV screener '%s'", tv_screener_name
        )
        candidates = _tv_filter_saved_screener(tv_screener_name, tv_user, tv_pass, cfg)
        if candidates is None:
            # Fallback to Option A when saved screener fetch fails
            logger.warning(
                "Saved TV screener '%s' failed — falling back to server-side filter scan.",
                tv_screener_name,
            )
            candidates = _tv_filter_serverside(cfg)
    else:
        # Option A: send filter conditions to TV's scanner, let TV do the filtering
        logger.info(
            "Pullback screener [Option A]: server-side TV filter scan (universe=%d symbols)",
            len(universe),
        )
        candidates = _tv_filter_serverside(cfg)

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

        ema_ladder = " > ".join(
            label for label, active in [
                ("Price > EMA20",   cfg["price_above_ema20"]),
                ("EMA20 > EMA50",   cfg["ema20_above_ema50"]),
                ("EMA50 > EMA100",  cfg["ema50_above_ema100"]),
                ("EMA100 > EMA200", cfg["ema100_above_ema200"]),
            ] if active
        ) or "No EMA ladder filters active."
        parts = [
            f"Pullback screener. RSI {c['rsi']:.0f} (reset zone).",
            f"{'PPST bullish. ' if c['ppst_bullish'] else 'PPST not confirmed. '}",
            f"Price {c['ema50_pct']:.1f}% from EMA50. EMA spread {c['ema_spread']:.1f}%.",
            f"ADX {c['adx']:.0f}. Perf 1M: {c['perf_1m']:+.1f}%, 3M: {c['perf_3m']:+.1f}%.",
            f"{abs(c['pct_from_52wh']):.1f}% below 52W high.",
            f"EMA: {ema_ladder}.",
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
            "tv_chart_url": f"https://www.tradingview.com/chart/?symbol={c['symbol']}",
        })

    logger.info(
        "Pullback screener done. Week of %s. Selected: %s",
        week_start, [r["symbol"] for r in plan_rows],
    )
    return plan_rows


# ── Pass 1 helpers ────────────────────────────────────────────────────────────

def _build_tv_filters(cfg: dict) -> list[dict]:
    """
    Convert pb_* settings into TradingView scanner filter conditions.
    These are enforced server-side by TV — no local re-check needed.
    """
    f = []

    # Price range
    if cfg["price_min"] > 0:
        f.append({"left": "close", "operation": "greater", "right": cfg["price_min"]})
    if cfg["price_max"] > 0:
        f.append({"left": "close", "operation": "less",    "right": cfg["price_max"]})

    # EMA ladder (each step independent)
    if cfg["price_above_ema20"]:
        f.append({"left": "close", "operation": "greater", "right": "EMA20"})
    if cfg["ema20_above_ema50"]:
        f.append({"left": "EMA20",  "operation": "greater", "right": "EMA50"})
    if cfg["ema50_above_ema100"]:
        f.append({"left": "EMA50",  "operation": "greater", "right": "EMA100"})
    if cfg["ema100_above_ema200"]:
        f.append({"left": "EMA100", "operation": "greater", "right": "EMA200"})

    # RSI reset zone
    f.append({"left": "RSI", "operation": "in_range",
               "right": [cfg["rsi_min"], cfg["rsi_max"]]})

    # Average volume (absolute floor — rel_vol is checked locally after fetch)
    if cfg["avg_vol_min"] > 0:
        f.append({"left": "average_volume_30d_calc",
                  "operation": "greater", "right": cfg["avg_vol_min"]})

    # Market cap
    if cfg["market_cap_min"] > 0:
        f.append({"left": "market_cap_basic",
                  "operation": "greater", "right": cfg["market_cap_min"]})

    # ADX: trend strength (server-side)
    if cfg["adx_min"] > 0:
        f.append({"left": "ADX", "operation": "greater", "right": cfg["adx_min"]})
    # 3-month performance (server-side)
    f.append({"left": "Perf.3M", "operation": "greater", "right": cfg["perf_3m_min"]})

    return f


def _fetch_and_refine(tv_syms: list[str], cfg: dict) -> list[dict]:
    """
    Batch-fetch _PB_COLS for a list of TV-formatted symbols, then apply the
    two filters that can't be expressed server-side:
      • EMA50 proximity  (requires arithmetic on two columns)
      • Relative volume  (volume / average_volume_30d_calc)

    Returns a list of candidate dicts ready for pass 2.
    """
    try:
        resp = httpx.post(
            SCAN_URL,
            json={
                "symbols": {"tickers": tv_syms, "query": {"types": []}},
                "columns": _PB_COLS,
            },
            timeout=30,
            headers=_TV_HEADERS,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.error("Pullback TV data fetch failed: %s", exc)
        return []

    candidates = []
    for row in resp.json().get("data", []):
        sym = row["s"].split(":")[-1]
        v   = dict(zip(_PB_COLS, row["d"]))
        c   = _local_refinement(sym, v, cfg)
        if c is not None:
            candidates.append(c)
    return candidates


def _local_refinement(sym: str, v: dict, cfg: dict) -> dict | None:
    """
    Apply the two filters that TV can't enforce server-side, then build the
    candidate dict.  The EMA ladder and price/volume/cap conditions are already
    guaranteed by the server-side filter — we don't re-check them here.
    """
    close = v.get("close") or 0
    if not close:
        return None

    e20  = v.get("EMA20")  or 0
    e50  = v.get("EMA50")  or 0
    e100 = v.get("EMA100") or 0
    e200 = v.get("EMA200") or 0
    rsi  = v.get("RSI")    or 50.0

    avg_vol = v.get("average_volume_30d_calc") or 0
    vol     = v.get("volume") or 0
    rel_vol = (vol / avg_vol) if avg_vol > 0 else 0

    # Relative volume — can't be done server-side (derived column)
    if rel_vol < cfg["rel_vol_min"]:
        return None

    # EMA50 proximity — requires arithmetic on two TV columns
    ema50_pct = abs(close - e50) / e50 * 100 if e50 else 0
    if e50 and ema50_pct > cfg["ema50_proximity"]:
        return None

    # EMA spread — EMA20 must be meaningfully above EMA50 (requires arithmetic, can't be server-side)
    if e20 and e50:
        ema_spread_pct = (e20 - e50) / e50 * 100
        if ema_spread_pct < cfg["ema_spread_min"]:
            return None
    else:
        ema_spread_pct = 0.0

    # 52-week high proximity (requires arithmetic on two TV columns)
    w52_high = v.get("price_52_week_high") or 0
    pct_from_52wh = ((close - w52_high) / w52_high * 100) if w52_high else -100
    if w52_high and abs(pct_from_52wh) > cfg["52w_high_pct_max"]:
        return None

    # Earnings date from TV (Unix timestamp → date, or None if unavailable)
    tv_earn_ts = v.get("earnings_release_next_date")
    tv_earn_date: date | None = None
    if tv_earn_ts:
        try:
            tv_earn_date = datetime.fromtimestamp(float(tv_earn_ts), tz=timezone.utc).date()
        except Exception:
            pass

    return {
        "symbol":        sym,
        "price":         close,
        "rsi":           rsi,
        "ema20":         e20,
        "ema50":         e50,
        "ema100":        e100,
        "ema200":        e200,
        "ema50_pct":     ema50_pct,
        "avg_vol":       avg_vol,
        "rel_vol":       round(rel_vol, 2),
        "market_cap":    v.get("market_cap_basic") or 0,
        "tv_earn_date":  tv_earn_date,   # None → pass-2 will call Yahoo Finance
        "ema_spread":    round(ema_spread_pct, 2),
        "adx":           v.get("ADX") or 0,
        "perf_1m":       v.get("Perf.1M") or 0,
        "perf_3m":       v.get("Perf.3M") or 0,
        "pct_from_52wh": round(pct_from_52wh, 1),
    }


# ── Pass 1 Option A: server-side TV filter scan ───────────────────────────────

def _tv_filter_serverside(cfg: dict) -> list[dict]:
    """
    Send filter conditions to TV's scanner and let TV do the heavy lifting.
    Returns all of the US market — not limited to a configured universe.
    Eliminates false positives caused by local Python filtering.
    """
    tv_filters = _build_tv_filters(cfg)

    try:
        resp = httpx.post(
            SCAN_URL,
            json={
                "filter":  tv_filters,
                "columns": _PB_COLS,
                "range":   [0, 200],   # up to 200 pre-filtered results
                "sort":    {"sortBy": "market_cap_basic", "sortOrder": "desc"},
                "markets": ["america"],
            },
            timeout=30,
            headers=_TV_HEADERS,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.error("Pullback TV server-side filter scan failed: %s", exc)
        return []

    candidates = []
    for row in resp.json().get("data", []):
        sym = row["s"].split(":")[-1]
        v   = dict(zip(_PB_COLS, row["d"]))
        c   = _local_refinement(sym, v, cfg)
        if c is not None:
            candidates.append(c)

    logger.info(
        "Pullback [Option A]: TV returned %d pre-filtered stocks, %d passed local refinement",
        len(resp.json().get("data", [])), len(candidates),
    )
    return candidates


# ── Pass 1 Option B: saved TV screener ────────────────────────────────────────

def _tv_filter_saved_screener(
    screener_name: str,
    tv_user: str,
    tv_pass: str,
    cfg: dict,
) -> list[dict] | None:
    """
    Fetch results from the user's saved TradingView screener by name,
    then batch-fetch indicator data and apply local refinement (EMA50
    proximity + relative volume).

    Returns None on failure so the caller can fall back to Option A.
    """
    from .tradingview_client import run_saved_screener, to_tv_symbol

    symbols, err = run_saved_screener(tv_user, tv_pass, screener_name)

    if err:
        logger.error("Saved TV screener error: %s", err)
        return None

    if not symbols:
        logger.warning("Saved TV screener '%s' returned 0 symbols.", screener_name)
        return None

    logger.info(
        "Pullback [Option B]: saved screener '%s' returned %d symbols — fetching indicators…",
        screener_name, len(symbols),
    )

    tv_syms = [to_tv_symbol(s) for s in symbols]
    # Batch-fetch indicators then apply local refinement only
    # (EMA ladder is already guaranteed by the TV screener itself)
    candidates = _fetch_and_refine(tv_syms, cfg)

    logger.info(
        "Pullback [Option B]: %d/%d symbols passed local refinement (EMA50 proximity + rel-vol)",
        len(candidates), len(symbols),
    )
    return candidates


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

            ppst_bullish = _calc_ppst(
                df,
                pivot_period=cfg["ppst_pivot_period"],
                atr_factor=cfg["ppst_atr_factor"],
                atr_period=cfg["ppst_atr_period"],
            )

            if cfg["ppst_required"] and not ppst_bullish:
                logger.debug("Pullback: %s skipped — PPST not bullish", sym)
                continue

            # ── Earnings gate ─────────────────────────────────────────────────
            # Source priority: TV (already fetched in pass-1) → Yahoo Finance → block/allow
            days_to_earnings = None
            if cfg["earnings_days_min"] > 0:
                next_earn = c.get("tv_earn_date")   # from TV pass-1 (free)
                if next_earn is None:
                    # TV had no date — try Yahoo Finance as fallback
                    next_earn = get_next_earnings_date(sym)
                    if next_earn is not None:
                        logger.debug("Pullback: %s earnings from Yahoo Finance: %s", sym, next_earn)

                if next_earn is None:
                    # Both sources unknown — block by default unless user opts out
                    if cfg["block_unknown_earnings"]:
                        logger.debug(
                            "Pullback: %s skipped — earnings date unknown (block_unknown_earnings=true)",
                            sym,
                        )
                        continue
                else:
                    days_to_earnings = (next_earn - today).days
                    if 0 <= days_to_earnings < cfg["earnings_days_min"]:
                        logger.debug(
                            "Pullback: %s skipped — earnings in %d days (min=%d)",
                            sym, days_to_earnings, cfg["earnings_days_min"],
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

def _calc_ppst(
    df: pd.DataFrame,
    pivot_period: int   = 2,    # TV "Pivot Point Period"
    atr_factor:   float = 3.0,  # TV "ATR Factor"
    atr_period:   int   = 10,   # TV "ATR Period"
) -> bool:
    """
    Compute Pivot Point Supertrend (PPST) direction for the most recent bar,
    matching TradingView's default inputs (Pivot Point Period=2, ATR Factor=3,
    ATR Period=10).

    Formula (mirrors the most common TV Pine implementation):
      1. Pivot High = highest(High, pivot_period)
         Pivot Low  = lowest(Low,  pivot_period)
         Pivot      = (Pivot High + Pivot Low + Close) / 3
      2. ATR(atr_period) — Wilder's smoothed
      3. Upper band = Pivot + atr_factor × ATR   (only moves down)
         Lower band = Pivot − atr_factor × ATR   (only moves up)
      4. Bullish when Close > lower band (trend = up)
         Bearish when Close < upper band (trend = down)

    Returns True = bullish (price above PPST support band).
    """
    try:
        high  = df["High"].to_numpy(dtype=float)
        low   = df["Low"].to_numpy(dtype=float)
        close = df["Close"].to_numpy(dtype=float)
        n     = len(close)

        min_bars = max(atr_period, pivot_period) + 2
        if n < min_bars:
            return False

        # ── Rolling pivot: highest high / lowest low over pivot_period ────────
        pivot_high = np.array([
            high[max(0, i - pivot_period + 1): i + 1].max() for i in range(n)
        ])
        pivot_low = np.array([
            low[max(0, i - pivot_period + 1): i + 1].min() for i in range(n)
        ])
        pivot = (pivot_high + pivot_low + close) / 3.0

        # ── Wilder ATR ────────────────────────────────────────────────────────
        prev_close    = np.roll(close, 1)
        prev_close[0] = close[0]
        tr = np.maximum.reduce([
            high - low,
            np.abs(high - prev_close),
            np.abs(low  - prev_close),
        ])
        atr = np.zeros(n)
        atr[atr_period] = tr[1: atr_period + 1].mean()
        for i in range(atr_period + 1, n):
            atr[i] = (atr[i - 1] * (atr_period - 1) + tr[i]) / atr_period

        # ── Adaptive bands + trend direction ──────────────────────────────────
        upper_raw = pivot + atr_factor * atr
        lower_raw = pivot - atr_factor * atr
        upper     = upper_raw.copy()
        lower     = lower_raw.copy()
        direction = np.ones(n, dtype=bool)   # True = bullish

        start = max(atr_period, pivot_period) + 1
        for i in range(start, n):
            # Lower band only moves up (ratchet)
            lower[i] = (
                lower_raw[i]
                if lower_raw[i] > lower[i - 1] or close[i - 1] < lower[i - 1]
                else lower[i - 1]
            )
            # Upper band only moves down (ratchet)
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
