"""
Yahoo Finance data client — bypasses yfinance's broken session handling.

Yahoo Finance blocks plain Python requests in cloud/container environments.
This module calls the v8/finance/chart JSON API directly, pre-warms the
session against the main site to obtain cookies, then falls back to the
query2 mirror if query1 fails.

Public API
----------
fetch_history(symbol, period_days)  → pd.DataFrame with "Close" column
get_current_price(symbol)           → float  (latest close, 0.0 on error)
"""
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://finance.yahoo.com/",
    "Origin":          "https://finance.yahoo.com",
}

_session: requests.Session | None = None
_session_warmed_at: float = 0.0
_WARM_TTL = 3600  # re-warm cookies every hour


def _get_session() -> requests.Session:
    """Return (and lazily warm) a shared requests session."""
    global _session, _session_warmed_at
    now = time.monotonic()

    if _session is None:
        _session = requests.Session()
        _session.headers.update(_HEADERS)

    if now - _session_warmed_at > _WARM_TTL:
        # Pre-warm: load yahoo.com to pick up consent cookie + crumb
        try:
            _session.get("https://finance.yahoo.com", timeout=10)
            logger.debug("yf_client: session warmed (cookies: %s)", list(_session.cookies.keys()))
        except Exception as exc:
            logger.warning("yf_client: session warm-up failed (%s) — continuing anyway", exc)
        _session_warmed_at = now

    return _session


def _parse_chart_response(body: dict, ohlcv: bool = False) -> pd.DataFrame:
    result     = body["chart"]["result"][0]
    timestamps = result["timestamp"]
    indicators = result["indicators"]
    quote      = indicators["quote"][0]

    if "adjclose" in indicators and indicators["adjclose"]:
        closes = indicators["adjclose"][0]["adjclose"]
    else:
        closes = quote["close"]

    if ohlcv:
        df = pd.DataFrame(
            {
                "Open":   quote.get("open",   [None] * len(timestamps)),
                "High":   quote.get("high",   [None] * len(timestamps)),
                "Low":    quote.get("low",    [None] * len(timestamps)),
                "Close":  closes,
                "Volume": quote.get("volume", [None] * len(timestamps)),
            },
            index=pd.to_datetime(timestamps, unit="s"),
        )
    else:
        df = pd.DataFrame(
            {"Close": closes},
            index=pd.to_datetime(timestamps, unit="s"),
        )
    return df.dropna(subset=["Close"])


def fetch_history(symbol: str, period_days: int = 365) -> pd.DataFrame:
    """
    Return a DataFrame with a 'Close' column for the requested symbol.
    Returns an empty DataFrame on failure (callers must handle this).
    """
    session  = _get_session()
    end_ts   = int(datetime.now().timestamp())
    start_ts = int((datetime.now() - timedelta(days=period_days + 10)).timestamp())

    params = {
        "period1":        start_ts,
        "period2":        end_ts,
        "interval":       "1d",
        "events":         "adjsplits,dividends",
        "includePrePost": "false",
    }

    for host in ("query1", "query2"):
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}"
        try:
            resp = session.get(url, params=params, timeout=20)
            resp.raise_for_status()
            body = resp.json()
            result = body.get("chart", {}).get("result")
            if not result:
                err = body.get("chart", {}).get("error", {})
                logger.warning("yf_client: %s %s — %s", host, symbol, err)
                continue
            df = _parse_chart_response(body)
            if not df.empty:
                return df
        except Exception as exc:
            logger.warning("yf_client: %s %s failed: %s", host, symbol, exc)

    logger.error("yf_client: all hosts failed for %s — returning empty", symbol)
    return pd.DataFrame()


def fetch_ohlcv(symbol: str, period_days: int = 60) -> pd.DataFrame:
    """
    Return a DataFrame with Open, High, Low, Close, Volume columns.
    Returns an empty DataFrame on failure.
    """
    session  = _get_session()
    end_ts   = int(datetime.now().timestamp())
    start_ts = int((datetime.now() - timedelta(days=period_days + 10)).timestamp())

    params = {
        "period1":        start_ts,
        "period2":        end_ts,
        "interval":       "1d",
        "events":         "adjsplits,dividends",
        "includePrePost": "false",
    }

    for host in ("query1", "query2"):
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}"
        try:
            resp = session.get(url, params=params, timeout=20)
            resp.raise_for_status()
            body = resp.json()
            result = body.get("chart", {}).get("result")
            if not result:
                continue
            df = _parse_chart_response(body, ohlcv=True)
            if not df.empty:
                return df
        except Exception as exc:
            logger.warning("yf_client fetch_ohlcv: %s %s failed: %s", host, symbol, exc)

    return pd.DataFrame()


def get_next_earnings_date(symbol: str) -> "date | None":
    """
    Return the next upcoming earnings date, or None if unknown.
    Uses Yahoo Finance quoteSummary calendarEvents module.
    """
    from datetime import date as _date
    session = _get_session()
    try:
        url  = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
        resp = session.get(url, params={"modules": "calendarEvents"}, timeout=10)
        resp.raise_for_status()
        data     = resp.json()
        result   = data.get("quoteSummary", {}).get("result") or []
        if not result:
            return None
        earnings = result[0].get("calendarEvents", {}).get("earnings", {})
        dates    = earnings.get("earningsDate", [])
        today    = _date.today()
        for d in dates:
            raw = d.get("raw") if isinstance(d, dict) else d
            if raw:
                try:
                    ed = _date.fromtimestamp(int(raw))
                    if ed >= today:
                        return ed
                except Exception:
                    pass
    except Exception as exc:
        logger.debug("get_next_earnings_date %s: %s", symbol, exc)
    return None


def get_current_price(symbol: str) -> float:
    """Latest adjusted close price, 0.0 on any failure."""
    df = fetch_history(symbol, period_days=5)
    if df.empty:
        return 0.0
    return float(df["Close"].iloc[-1])
