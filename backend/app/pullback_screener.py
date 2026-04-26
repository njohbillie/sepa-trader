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


def _validate_tv_payload(payload: dict, expected_cols: list[str], context: str) -> list[dict]:
    """Sanity-check a TradingView scan response before zipping into dicts.

    TV occasionally renames or drops columns silently — when that happens the
    response either returns no `data` key, returns rows with the wrong number
    of cells, or produces all-None rows. We raise RuntimeError so the caller
    can log + alert instead of returning an empty plan with no signal.
    """
    if not isinstance(payload, dict) or "data" not in payload:
        raise RuntimeError(f"TV {context}: response missing 'data' key (schema drift?)")

    rows = payload.get("data") or []
    if not rows:
        return []

    expected_n = len(expected_cols)
    sample     = rows[0]
    if not isinstance(sample, dict) or "d" not in sample or "s" not in sample:
        raise RuntimeError(f"TV {context}: row shape changed (no 's'/'d' keys)")
    if len(sample.get("d") or []) != expected_n:
        raise RuntimeError(
            f"TV {context}: column count mismatch — got {len(sample['d'])}, expected {expected_n}"
        )

    # All-None sentinel: if every cell of the critical columns is None, schema
    # drift renamed them. Check the first 5 rows on the close column (always [0]).
    sample_size = min(5, len(rows))
    if all((rows[i].get("d") or [None])[0] is None for i in range(sample_size)):
        raise RuntimeError(f"TV {context}: critical column 'close' is all-None across sample rows")

    return rows

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
    "sector",                       # Used for sector exclusion filter
]

# Sectors excluded by default — commodity/defensive/low-growth sectors that
# don't fit a momentum-pullback growth strategy.
DEFAULT_EXCLUDED_SECTORS = (
    "Consumer Defensive,"
    "Energy,"
    "Utilities,"
    "Real Estate,"
    "Basic Materials,"
    "Financial Services"
)

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
        # AI chart review — runs per candidate after all quantitative filters
        "ai_chart_review":       _s("pb_ai_chart_review",      "false") == "true",
        "ai_chart_min_grade":    _s("pb_ai_chart_min_grade",   "B").strip().upper(),
        "ema_spread_min":     float(_s("pb_ema_spread_min",     1.0)),   # min % EMA20 > EMA50
        "adx_min":            float(_s("pb_adx_min",            20.0)),  # min ADX (trend strength)
        "52w_high_pct_max":   float(_s("pb_52w_high_pct_max",   30.0)),  # max % below 52W high
        "perf_3m_min":        float(_s("pb_3m_perf_min",        -5.0)),  # min 3-month performance %
        # Exchange filter — comma-separated list, e.g. "NYSE,NASDAQ"
        "exchanges": [
            e.strip().upper()
            for e in _s("pb_exchanges", "NYSE,NASDAQ").split(",")
            if e.strip()
        ],
        # Sector exclusion — per-strategy, comma-separated TV sector names.
        "excluded_sectors": [
            s.strip()
            for s in _s("pb_excluded_sectors", DEFAULT_EXCLUDED_SECTORS).split(",")
            if s.strip()
        ],
        # Minimum YoY revenue growth % (0 = disabled)
        "min_revenue_growth": float(_s("pb_min_revenue_growth", "0")),
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def run_pullback_screener(
    db: Session,
    mode: str = None,
    user_id: int = None,
    account_value: float = None,
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
            candidates = _adaptive_ema_serverside(cfg)
    else:
        # Option A: send filter conditions to TV's scanner, let TV do the filtering
        logger.info(
            "Pullback screener [Option A]: server-side TV filter scan (universe=%d symbols)",
            len(universe),
        )
        candidates = _adaptive_ema_serverside(cfg)

    logger.info("Pullback screener: %d candidates after TV filter", len(candidates))

    if not candidates:
        logger.info("Pullback screener: no candidates passed TV filter")
        return []

    # ── Pass 2: PPST + earnings + optional AI chart review ───────────────────
    scored = _score_candidates(candidates, cfg, db=db, user_id=user_id)
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

    if account_value is None:
        try:
            account_value = _get_portfolio_value(db, mode, user_id)
        except RuntimeError as exc:
            logger.error("Pullback screener: %s", exc)
            raise

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
        if c.get("sector"):
            parts.append(f"Sector: {c['sector']}.")
        if c.get("rev_growth") is not None:
            parts.append(f"Revenue growth: {c['rev_growth']:+.1f}% YoY.")
        if c.get("days_to_earnings") is not None:
            parts.append(f"Earnings ≥{c['days_to_earnings']}d away.")
        if c.get("ai_chart_grade"):
            parts.append(
                f"AI chart grade: {c['ai_chart_grade']}."
                + (f" {c['ai_chart_reason']}" if c.get("ai_chart_reason") else "")
            )
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

    # Exchange filter (server-side — only scan selected exchanges)
    if cfg.get("exchanges"):
        f.append({"left": "exchange", "operation": "in_range", "right": cfg["exchanges"]})

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

    try:
        rows = _validate_tv_payload(resp.json(), _PB_COLS, "fetch_and_refine")
    except RuntimeError as exc:
        logger.error("Pullback TV schema invalid: %s", exc)
        try:
            from . import telegram_alerts as tg
            tg.alert_system_error_sync("Pullback screener: TV schema drift", exc)
        except Exception:
            pass
        return []

    candidates = []
    for row in rows:
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

    # Sector exclusion — block commodity/defensive/low-growth sectors
    sector = (v.get("sector") or "").strip()
    if cfg.get("excluded_sectors") and sector and sector in cfg["excluded_sectors"]:
        logger.debug("Pullback: %s skipped — excluded sector (%s)", sym, sector)
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
        "sector":        sector,
        "ema_spread":    round(ema_spread_pct, 2),
        "adx":           v.get("ADX") or 0,
        "perf_1m":       v.get("Perf.1M") or 0,
        "perf_3m":       v.get("Perf.3M") or 0,
        "pct_from_52wh": round(pct_from_52wh, 1),
    }


# ── Pass 1 Option A: server-side TV filter scan ───────────────────────────────

# Minimum candidates pass-1 should produce before we consider the EMA ladder
# "too tight." If TV returns fewer than this with all 4 EMA toggles on, the
# higher EMAs (100/200) are progressively dropped. The two short-MA conditions
# (price > EMA20, EMA20 > EMA50) are core to the pullback thesis and never relaxed.
_PB_ADAPTIVE_MIN_CANDIDATES = 5


def _adaptive_ema_serverside(cfg: dict) -> list[dict]:
    """
    Run server-side TV filter, progressively relaxing the higher EMA-ladder
    rungs (EMA100>EMA200, then EMA50>EMA100) when the result set is too small.
    The short-MA conditions (price>EMA20, EMA20>EMA50) are never relaxed —
    relaxing them would defeat the "pullback to short MA" thesis.
    """
    candidates = _tv_filter_serverside(cfg)

    # Relaxation only kicks in when (a) result is sparse, and (b) at least one
    # of the higher rungs is currently on. Otherwise nothing to relax.
    relax_steps = [
        ("ema100_above_ema200", "EMA100>EMA200"),
        ("ema50_above_ema100",  "EMA50>EMA100"),
    ]
    relaxed_cfg = dict(cfg)
    for key, label in relax_steps:
        if len(candidates) >= _PB_ADAPTIVE_MIN_CANDIDATES:
            break
        if not relaxed_cfg.get(key):
            continue
        relaxed_cfg = dict(relaxed_cfg)
        relaxed_cfg[key] = False
        logger.warning(
            "Pullback screener: only %d candidates — relaxing %s and retrying.",
            len(candidates), label,
        )
        candidates = _tv_filter_serverside(relaxed_cfg)

    return candidates


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

    try:
        rows = _validate_tv_payload(resp.json(), _PB_COLS, "serverside_filter")
    except RuntimeError as exc:
        logger.error("Pullback TV schema invalid: %s", exc)
        try:
            from . import telegram_alerts as tg
            tg.alert_system_error_sync("Pullback screener: TV schema drift", exc)
        except Exception:
            pass
        return []

    candidates = []
    for row in rows:
        sym = row["s"].split(":")[-1]
        v   = dict(zip(_PB_COLS, row["d"]))
        c   = _local_refinement(sym, v, cfg)
        if c is not None:
            candidates.append(c)

    logger.info(
        "Pullback [Option A]: TV returned %d pre-filtered stocks, %d passed local refinement",
        len(rows), len(candidates),
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


# ── AI chart review (pass 2 gate) ────────────────────────────────────────────

def _ai_chart_review(
    db,
    symbol: str,
    df: pd.DataFrame,
    c: dict,
    user_id: int,
) -> dict:
    """
    Analyse the chart setup using the already-fetched OHLCV data.
    Derives price-action metrics a trader reads visually (volume contraction,
    pullback depth, EMA positioning, candle quality) and asks the AI to grade
    the setup: A (excellent) / B (acceptable) / C (marginal) / SKIP (reject).

    Returns {"grade": str, "pass": bool, "reasoning": str}.
    Falls back to {"grade": "B", "pass": True, "reasoning": "AI unavailable"}
    so the screener does not stall when no AI key is configured.
    """
    try:
        from .claude_analyst import _call_ai
        import json as _json

        close  = df["Close"].to_numpy(dtype=float)
        high   = df["High"].to_numpy(dtype=float)
        low    = df["Low"].to_numpy(dtype=float)
        vol    = df["Volume"].to_numpy(dtype=float)
        n      = len(close)

        # ── Price action metrics ──────────────────────────────────────────────
        recent = min(10, n)
        bars   = df.tail(recent)

        # Volume: is it contracting during the pullback?
        vol_recent = vol[-5:].mean()   if n >= 5  else vol.mean()
        vol_prior  = vol[-10:-5].mean() if n >= 10 else vol.mean()
        vol_contraction = vol_recent < vol_prior * 0.9   # >10% contraction = healthy

        # Pullback depth from recent 20-bar swing high
        swing_high = high[-20:].max() if n >= 20 else high.max()
        pullback_pct = (swing_high - close[-1]) / swing_high * 100

        # How many of the last 10 bars closed above EMA50?
        e50 = c.get("ema50") or 0
        bars_above_ema50 = sum(1 for x in close[-10:] if e50 and x > e50) if n >= 10 else "n/a"

        # Average candle body/range ratio (last 10 bars) — small bodies = indecision/pullback
        opens  = df["Open"].to_numpy(dtype=float)
        bodies = np.abs(close - opens)
        ranges = high - low
        avg_body_ratio = (bodies[-recent:] / np.where(ranges[-recent:] > 0, ranges[-recent:], 1)).mean()

        # Recent bar table (last 7 bars, compact)
        bar_rows = []
        for i in range(max(0, n - 7), n):
            direction = "▲" if close[i] >= opens[i] else "▼"
            bar_rows.append(
                f"  {direction} O:{opens[i]:.2f} H:{high[i]:.2f} L:{low[i]:.2f} "
                f"C:{close[i]:.2f} Vol:{vol[i]/1e6:.2f}M"
            )
        bar_table = "\n".join(bar_rows)

        # ── Prompt ────────────────────────────────────────────────────────────
        prompt = f"""You are an expert technical analyst specialising in Minervini-style Stage 2 pullback setups.

Review the following chart data for {symbol} and grade the setup:

QUANTITATIVE METRICS (already passed all quantitative filters):
• Price:        ${c['price']:.2f}
• EMA20:        ${c.get('ema20', 0):.2f}   EMA50: ${c.get('ema50', 0):.2f}   EMA100: ${c.get('ema100', 0):.2f}   EMA200: ${c.get('ema200', 0):.2f}
• EMA20/50 spread: {c.get('ema_spread', 0):.2f}%   (higher = better-defined uptrend)
• RSI:          {c.get('rsi', 0):.1f}   (target: 40–60 reset zone)
• ADX:          {c.get('adx', 0):.1f}   (>25 = strong trend)
• PPST:         {"BULLISH ✅" if c.get("ppst_bullish") else "bearish ❌"}
• Dist. from EMA50: {c.get('ema50_pct', 0):.1f}%
• % below 52W high: {abs(c.get('pct_from_52wh', 0)):.1f}%
• 1M performance:   {c.get('perf_1m', 0):+.1f}%
• 3M performance:   {c.get('perf_3m', 0):+.1f}%
• Earnings:     {f"{c.get('days_to_earnings', '?')} days away" if c.get('days_to_earnings') is not None else "unknown"}

PRICE-ACTION ANALYSIS (derived from 60-day OHLCV):
• Pullback depth from 20-bar swing high: {pullback_pct:.1f}%
• Volume contraction on pullback:        {"YES ✅ (healthy)" if vol_contraction else "NO ⚠️ (elevated selling)"}
• Recent avg vol:  {vol_recent/1e6:.2f}M   Prior avg vol: {vol_prior/1e6:.2f}M
• Bars above EMA50 (last 10):            {bars_above_ema50}/10
• Avg candle body/range:                 {avg_body_ratio:.2f}   (0.3–0.6 = orderly, <0.3 = indecision)

LAST 7 DAILY BARS:
{bar_table}

GRADING CRITERIA:
A — Pristine Stage 2 pullback: EMAs well-fanned, volume contracts on pullback, price near EMA support, PPST bullish, RSI reset 45–55, recent momentum positive.
B — Solid setup with one minor blemish (e.g. slightly elevated pullback volume, RSI at edge of range, modest 3M perf).
C — Marginal: passes filters but chart is messy, volume not contracting, or trend quality is weak.
SKIP — Reject: downtrend bounce not a real pullback, excessive earnings risk, volume expanding on decline, or broken trend structure.

Respond ONLY with a single compact JSON object on one line — no markdown fences, no newlines inside strings, no trailing text:
{{"grade":"A|B|C|SKIP","reasoning":"Max 30-word plain-English summary"}}"""

        import time as _time
        raw = None
        for attempt in range(3):
            try:
                raw = _call_ai(db, prompt, max_tokens=250, user_id=user_id)
                break
            except Exception as _exc:
                msg = str(_exc).lower()
                if ("529" in msg or "overloaded" in msg) and attempt < 2:
                    wait = 10 * (2 ** attempt)   # 10s, 20s
                    logger.warning(
                        "AI chart review %s: API overloaded (attempt %d/3) — retrying in %ds",
                        symbol, attempt + 1, wait,
                    )
                    _time.sleep(wait)
                else:
                    raise
        if not raw:
            return {"grade": "B", "pass": True, "reasoning": "AI key not configured — skipping chart review"}

        # Robust JSON extraction — find first {{ ... }} block, handle truncation
        import re as _re
        text = raw.strip()
        # Strip markdown fences if present
        text = _re.sub(r"^```[a-z]*\n?", "", text)
        text = _re.sub(r"\n?```$", "", text)
        text = text.strip()
        # Extract the first JSON object
        m = _re.search(r'\{[^{}]*\}', text, _re.DOTALL)
        if not m:
            logger.warning("AI chart review %s: no JSON found in response: %s", symbol, text[:200])
            return {"grade": "B", "pass": True, "reasoning": "AI response unparseable — allowing through"}
        raw_json = m.group(0)
        # Sanitise: replace real newlines inside strings with spaces
        raw_json = _re.sub(r'\n', ' ', raw_json)
        result = _json.loads(raw_json)

        grade     = result.get("grade", "B").strip().upper()
        reasoning = result.get("reasoning", "")

        logger.info("AI chart review %s: grade=%s — %s", symbol, grade, reasoning[:120])
        return {"grade": grade, "pass": grade not in ("C", "SKIP"), "reasoning": reasoning}

    except Exception as exc:
        logger.warning("AI chart review %s failed: %s — allowing through", symbol, exc)
        return {"grade": "?", "pass": True, "reasoning": f"AI review error: {exc}"}


# ── Earnings cache ────────────────────────────────────────────────────────────

def _get_cached_earnings(db, symbol: str) -> "date | None":
    """Earnings date with persistent cache.

    Yahoo Finance frequently returns null for valid symbols, which previously
    caused `block_unknown_earnings=true` to silently zero out the universe on
    bad-data days. Cache positive hits for 7 days, null hits for 1 day so a
    single-day yfinance hiccup doesn't immediately invalidate good data.

    Returns the cached date (which may be None) when fresh; on cache miss or
    TTL expiry, fetches from yfinance and stores the result (including None).
    Falls through to a live fetch if `db` is None or the cache lookup errors.
    """
    from .strategies.yf_client import get_next_earnings_date
    from sqlalchemy import text as _text

    if db is None:
        return get_next_earnings_date(symbol)

    try:
        row = db.execute(
            _text("""
                SELECT next_earnings,
                       EXTRACT(EPOCH FROM (NOW() - fetched_at)) AS age_sec
                FROM earnings_cache
                WHERE symbol = :s
            """),
            {"s": symbol},
        ).fetchone()
    except Exception as exc:
        logger.debug("earnings_cache lookup failed for %s: %s", symbol, exc)
        return get_next_earnings_date(symbol)

    if row is not None:
        cached_date, age_sec = row[0], float(row[1] or 0)
        ttl_sec = 86400 if cached_date is None else 7 * 86400   # 1d for null, 7d for known
        if age_sec < ttl_sec:
            return cached_date

    fresh = get_next_earnings_date(symbol)
    try:
        db.execute(
            _text("""
                INSERT INTO earnings_cache (symbol, next_earnings, fetched_at)
                VALUES (:s, :d, NOW())
                ON CONFLICT (symbol) DO UPDATE
                  SET next_earnings = EXCLUDED.next_earnings,
                      fetched_at    = EXCLUDED.fetched_at
            """),
            {"s": symbol, "d": fresh},
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.debug("earnings_cache write failed for %s: %s", symbol, exc)
    return fresh


# ── Pass 2: PPST + earnings ───────────────────────────────────────────────────

def _score_candidates(
    candidates: list[dict],
    cfg: dict,
    db=None,
    user_id: int = None,
) -> list[dict]:
    """
    For each candidate: compute PPST from OHLCV, check earnings date,
    optionally run AI chart review (pb_ai_chart_review=true).
    Returns the filtered + scored subset.
    """
    from .strategies.yf_client import fetch_ohlcv

    today        = date.today()
    out          = []
    fail_count   = 0
    last_fail_exc: Exception | None = None

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
                    # TV had no date — try cached Yahoo Finance fallback
                    next_earn = _get_cached_earnings(db, sym)
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
                    # Block when next earnings is within the buffer window.
                    # Negative values (stale/past dates from upstream) are also
                    # blocked — a "next earnings" date that's already in the
                    # past means the data is unreliable.
                    if days_to_earnings < cfg["earnings_days_min"]:
                        logger.debug(
                            "Pullback: %s skipped — earnings in %d days (min=%d)",
                            sym, days_to_earnings, cfg["earnings_days_min"],
                        )
                        continue

            # ── Revenue growth gate (optional pass-2) ─────────────────────────
            rev_growth = None
            if cfg["min_revenue_growth"] > 0:
                from .strategies.yf_client import get_revenue_growth
                rev_growth = get_revenue_growth(sym)
                if rev_growth is None:
                    logger.debug("Pullback: %s — revenue growth unavailable, allowing through", sym)
                elif rev_growth < cfg["min_revenue_growth"]:
                    logger.debug(
                        "Pullback: %s skipped — revenue growth %.1f%% < min %.1f%%",
                        sym, rev_growth, cfg["min_revenue_growth"],
                    )
                    continue

            # ── AI chart review (optional pass-2 gate) ────────────────────────
            ai_grade    = None
            ai_reasoning = None
            if cfg.get("ai_chart_review") and db is not None:
                review = _ai_chart_review(db, sym, df, {**c, "ppst_bullish": ppst_bullish}, user_id)
                ai_grade     = review["grade"]
                ai_reasoning = review["reasoning"]
                min_grade    = cfg.get("ai_chart_min_grade", "B")
                grade_order  = {"A": 0, "B": 1, "C": 2, "SKIP": 3}
                threshold    = grade_order.get(min_grade, 1)
                if grade_order.get(ai_grade, 99) > threshold:
                    logger.info(
                        "Pullback: %s rejected by AI chart review (grade=%s, min=%s): %s",
                        sym, ai_grade, min_grade, (ai_reasoning or "")[:120],
                    )
                    continue

            # ── Score (1–6) ───────────────────────────────────────────────────
            score = 3
            if ppst_bullish:
                score += 1
            if 45 <= c["rsi"] <= 55:
                score += 1
            if ai_grade == "A":
                score += 1

            out.append({
                **c,
                "ppst_bullish":     ppst_bullish,
                "days_to_earnings": days_to_earnings,
                "rev_growth":       rev_growth,
                "ai_chart_grade":   ai_grade,
                "ai_chart_reason":  ai_reasoning,
                "score":            score,
            })

        except Exception as exc:
            fail_count   += 1
            last_fail_exc = exc
            logger.warning("Pullback: %s failed in pass-2: %s", sym, exc)
            continue

    # If yfinance is having a bad day, the entire pass-2 silently degrades.
    # Surface it: warn at >=30% failure, alert + raise at >=70% so the
    # scheduler error path catches it and notifies via Telegram.
    total = len(candidates)
    if total >= 5 and fail_count >= 1:
        rate = fail_count / total
        if rate >= 0.7:
            logger.error(
                "Pullback pass-2: %d/%d candidates failed (%.0f%%) — likely yfinance outage",
                fail_count, total, rate * 100,
            )
            try:
                from . import telegram_alerts as tg
                tg.alert_system_error_sync(
                    f"Pullback pass-2 degraded: {fail_count}/{total} symbols failed",
                    last_fail_exc or RuntimeError("unknown"),
                )
            except Exception:
                pass
        elif rate >= 0.3:
            logger.warning(
                "Pullback pass-2: %d/%d candidates failed (%.0f%%)",
                fail_count, total, rate * 100,
            )

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
