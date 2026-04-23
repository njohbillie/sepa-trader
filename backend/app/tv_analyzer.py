"""
TradingView Scanner API-based SEPA analyzer.

Replaces yfinance: fetches all universe symbols in ONE HTTP request
to TradingView's public screener endpoint — no per-symbol rate limits,
no retries, ~5 seconds for 100 symbols vs 3–5 minutes with yfinance.
"""
import logging
import httpx

from .tradingview_client import to_tv_symbol

logger = logging.getLogger(__name__)

SCAN_URL = "https://scanner.tradingview.com/america/scan"

_TV_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin":  "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
}

# Cached session cookie from TV login (populated by _get_tv_cookie)
_tv_cookie: str = ""


def _get_tv_cookie(db=None) -> str:
    """
    Return a cached TV session cookie if TV credentials are configured.
    Falls back to empty string (unauthenticated) if not set.
    """
    global _tv_cookie
    if _tv_cookie:
        return _tv_cookie
    if db is None:
        return ""
    try:
        from .database import get_setting as _gs
        tv_user = _gs(db, "tv_username", "")
        tv_pass = _gs(db, "tv_password", "")
        if not tv_user or not tv_pass:
            return ""
        from .tradingview_client import get_session_cookie
        _tv_cookie = get_session_cookie(tv_user, tv_pass) or ""
        if _tv_cookie:
            logger.info("tv_analyzer: TV session cookie obtained")
    except Exception as exc:
        logger.debug("tv_analyzer: could not get TV cookie: %s", exc)
    return _tv_cookie

# Confirmed-valid TradingView scanner columns only.
# EMA150 is not a standard TV field — interpolated from EMA100+EMA200.
# 52W High/Low not available via scanner API — those 2 criteria score 0; max is 6/8.
_COLS = [
    "close",
    "EMA20",
    "EMA50",
    "EMA100",                   # proxy for EMA150
    "EMA200",
    "SMA200",                   # EMA200 > SMA200 → EMA200 is rising
    "volume",
    "average_volume_30d_calc",
]


def batch_analyze(
    symbols: list[str],
    vol_surge_pct: float = 40.0,
    ema20_pct: float = 2.0,
    ema50_pct: float = 3.0,
    db=None,
) -> dict[str, dict]:
    """
    Fetch SEPA indicators for every symbol in one TradingView API call.
    Returns {symbol: result_dict} in the same format as sepa_analyzer.analyze().

    Handles TV 401 gracefully — returns INSUFFICIENT_DATA so the monitor
    can still manage stops/exits even when TV blocks unauthenticated requests.
    """
    global _tv_cookie
    tv_syms = [to_tv_symbol(s) for s in symbols]

    def _do_request(cookie: str = ""):
        headers = dict(_TV_HEADERS)
        if cookie:
            headers["Cookie"] = cookie
        return httpx.post(
            SCAN_URL,
            json={
                "symbols": {"tickers": tv_syms, "query": {"types": []}},
                "columns": _COLS,
            },
            timeout=30,
            headers=headers,
        )

    try:
        resp = _do_request(_tv_cookie)

        # TV returns 401 when unauthenticated from cloud IPs — retry with session cookie
        if resp.status_code == 401:
            logger.warning(
                "tv_analyzer: TV scanner returned 401 — attempting authenticated request"
            )
            _tv_cookie = ""          # clear stale cookie
            fresh_cookie = _get_tv_cookie(db)
            if fresh_cookie:
                resp = _do_request(fresh_cookie)
            else:
                logger.warning(
                    "tv_analyzer: no TV credentials configured — "
                    "scanner unavailable. Add TV username/password in Settings → Integrations. "
                    "Monitor will continue managing stops and exits."
                )
                return {s: {"signal": "INSUFFICIENT_DATA", "score": 0, "price": None} for s in symbols}

        if resp.status_code == 401:
            logger.error("tv_analyzer: TV scanner still 401 after auth — credentials may be wrong")
            return {s: {"signal": "INSUFFICIENT_DATA", "score": 0, "price": None} for s in symbols}

        resp.raise_for_status()

    except Exception as exc:
        logger.error("TradingView scan request failed: %s", exc)
        err = str(exc)
        return {s: {"signal": "ERROR", "score": 0, "price": None, "error": err} for s in symbols}

    rows = resp.json().get("data", [])
    results: dict[str, dict] = {}

    for row in rows:
        sym = row["s"].split(":")[-1]
        vals = dict(zip(_COLS, row["d"]))
        results[sym] = _score_sepa(sym, vals, vol_surge_pct, ema20_pct, ema50_pct)

    # Symbols TradingView didn't return (unknown/delisted)
    for s in symbols:
        if s not in results:
            results[s] = {"signal": "INSUFFICIENT_DATA", "score": 0, "price": None}

    logger.info(
        "TradingView scan: %d requested, %d returned, %d errors",
        len(symbols),
        len(rows),
        sum(1 for r in results.values() if r.get("signal") in ("ERROR", "INSUFFICIENT_DATA")),
    )
    return results


def analyze(symbol: str, db=None) -> dict:
    """Single-symbol wrapper — used by the hourly monitor."""
    return batch_analyze([symbol], db=db).get(symbol, {"signal": "ERROR", "score": 0, "price": None})


def _score_sepa(
    symbol: str,
    v: dict,
    vol_surge_pct: float = 40.0,
    ema20_pct: float = 2.0,
    ema50_pct: float = 3.0,
) -> dict:
    close = v.get("close")
    if not close:
        return {"signal": "INSUFFICIENT_DATA", "score": 0, "price": None}

    e20      = v.get("EMA20") or 0
    e50      = v.get("EMA50") or 0
    e100     = v.get("EMA100") or 0
    e200     = v.get("EMA200") or 0
    sma200   = v.get("SMA200") or 0
    w52h     = 0  # not available via TV scanner — criterion 7 always 0
    w52l     = 0  # not available via TV scanner — criterion 8 always 0
    vol      = v.get("volume") or 0
    vol_avg  = v.get("average_volume_30d_calc") or 1

    # EMA150 not a standard TV field — interpolate between EMA100 and EMA200
    e150 = (e100 * 0.5 + e200 * 0.5) if e100 and e200 else 0

    # Criterion 6: EMA200 rising — EMA200 > SMA200 means recent closes are
    # pulling the exponential average above the simple one (upward momentum)
    e200_rising = bool(e200 and sma200 and e200 > sma200)

    score = sum([
        bool(e50  and close > e50),
        bool(e150 and close > e150),
        bool(e200 and close > e200),
        bool(e50  and e150 and e50  > e150),
        bool(e150 and e200 and e150 > e200),
        e200_rising,
        bool(w52h and close >= w52h * 0.75),
        bool(w52l and close >= w52l * 1.30),
    ])

    vol_surge   = bool(vol and vol_avg and vol > vol_avg * (1 + vol_surge_pct / 100))
    e20_near    = bool(e20  and abs(close - e20)  / e20  * 100 <= ema20_pct)
    e50_near    = bool(e50  and abs(close - e50)  / e50  * 100 <= ema50_pct)
    above_pivot = score >= 7

    if score >= 7:
        if vol_surge:
            signal = "BREAKOUT"
        elif e20_near:
            signal = "PULLBACK_EMA20"
        elif e50_near:
            signal = "PULLBACK_EMA50"
        else:
            signal = "STAGE2_WATCH"
    elif score >= 4:
        signal = "PULLBACK_EMA20" if e20_near else ("PULLBACK_EMA50" if e50_near else "STAGE2_WATCH")
    else:
        signal = "NO_SETUP"

    return {
        "signal":      signal,
        "score":       score,
        "price":       round(close, 4),
        "ema20":       round(e20, 4)  if e20  else None,
        "ema50":       round(e50, 4)  if e50  else None,
        "ema150":      round(e150, 4) if e150 else None,
        "ema200":      round(e200, 4) if e200 else None,
        "week52_high": round(w52h, 4) if w52h else None,
        "week52_low":  round(w52l, 4) if w52l else None,
        "vol_today":   int(vol),
        "vol_avg30":   int(vol_avg),
        "vol_surge":   vol_surge,
        "near20":      e20_near,
        "near50":      e50_near,
        "above_pivot": above_pivot,
    }
