"""
DD fetcher via Yahoo Finance quoteSummary JSON API.
Authenticates with Yahoo's cookie+crumb session flow (no yfinance dependency).
Results must be cached by the caller (7-day TTL in dd_cache table).
"""
import time
import logging
import httpx

logger = logging.getLogger(__name__)

_MODULES = "assetProfile,summaryDetail,financialData,defaultKeyStatistics"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_COMMON_HEADERS = {
    "User-Agent": _UA,
    "Accept-Language": "en-US,en;q=0.9",
}

# Process-lifetime session cache; refreshed on 401
_crumb: str | None = None
_cookies: dict = {}


def _refresh_session() -> tuple[str, dict]:
    """Establish a Yahoo Finance session and return (crumb, cookies)."""
    with httpx.Client(headers=_COMMON_HEADERS, follow_redirects=True, timeout=15) as c:
        c.get("https://finance.yahoo.com/")
        r = c.get(
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            headers={**_COMMON_HEADERS, "Accept": "*/*"},
        )
        crumb = r.text.strip()
        if not crumb or len(crumb) > 40 or "<" in crumb:
            raise RuntimeError(f"Bad crumb response: {r.text[:100]}")
        return crumb, dict(c.cookies)


def _ensure_session():
    global _crumb, _cookies
    if not _crumb:
        _crumb, _cookies = _refresh_session()
        logger.info("Yahoo Finance session OK (crumb: %s…)", _crumb[:6])


def _fetch_summary(symbol: str) -> dict:
    """Return the raw quoteSummary result dict for one symbol."""
    global _crumb, _cookies
    _ensure_session()

    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
    params = {"modules": _MODULES, "crumb": _crumb, "lang": "en-US", "region": "US"}
    headers = {**_COMMON_HEADERS, "Accept": "application/json"}

    resp = httpx.get(url, params=params, headers=headers, cookies=_cookies,
                     timeout=15, follow_redirects=True)

    if resp.status_code == 401:
        logger.info("Yahoo Finance session expired — refreshing.")
        _crumb, _cookies = _refresh_session()
        params["crumb"] = _crumb
        resp = httpx.get(url, params=params, headers=headers, cookies=_cookies,
                         timeout=15, follow_redirects=True)

    resp.raise_for_status()
    qs = resp.json().get("quoteSummary", {})
    err = qs.get("error")
    if err:
        raise ValueError(f"YF error: {err}")
    results = qs.get("result") or []
    if not results:
        raise ValueError("Empty quoteSummary result")
    return results[0]


def _raw(result: dict, module: str, key: str, default=None):
    """Extract a scalar from a module field (unwraps {raw: …} dicts)."""
    val = (result.get(module) or {}).get(key)
    if isinstance(val, dict):
        return val.get("raw", default)
    return val if val is not None else default


_RATING = {
    (1.0, 1.5): ("Strong Buy",   "text-emerald-400"),
    (1.5, 2.5): ("Buy",          "text-emerald-300"),
    (2.5, 3.5): ("Hold",         "text-yellow-400"),
    (3.5, 4.5): ("Underperform", "text-orange-400"),
    (4.5, 5.1): ("Sell",         "text-red-400"),
}


def _rating_meta(mean):
    if mean is None:
        return "N/A", "text-slate-500"
    for (lo, hi), (lbl, css) in _RATING.items():
        if lo <= float(mean) < hi:
            return lbl, css
    return "N/A", "text-slate-500"


def fetch_dd(symbol: str) -> dict:
    """Fetch DD for one symbol. Never raises — returns {error} on failure."""
    try:
        result = _fetch_summary(symbol)

        def v(mod, key):
            return _raw(result, mod, key)

        profile = result.get("assetProfile") or {}
        rating_mean = v("financialData", "recommendationMean")
        label, css  = _rating_meta(rating_mean)

        return {
            "symbol":          symbol,
            "name":            profile.get("longName") or profile.get("shortName") or symbol,
            "sector":          profile.get("sector") or "",
            "industry":        profile.get("industry") or "",
            "market_cap":      v("summaryDetail",        "marketCap"),
            "pe_ttm":          v("summaryDetail",        "trailingPE"),
            "forward_pe":      v("summaryDetail",        "forwardPE"),
            "eps_ttm":         v("defaultKeyStatistics", "trailingEps"),
            "revenue_growth":  v("financialData",        "revenueGrowth"),
            "earnings_growth": v("financialData",        "earningsGrowth"),
            "gross_margin":    v("financialData",        "grossMargins"),
            "net_margin":      v("financialData",        "profitMargins"),
            "roe":             v("financialData",        "returnOnEquity"),
            "debt_to_equity":  v("financialData",        "debtToEquity"),
            "analyst_rating":  float(rating_mean) if rating_mean is not None else None,
            "analyst_label":   label,
            "analyst_css":     css,
            "analyst_count":   v("financialData", "numberOfAnalystOpinions"),
            "description":     (profile.get("longBusinessSummary") or "")[:500],
            "error":           None,
        }
    except Exception as exc:
        logger.warning("DD fetch failed for %s: %s", symbol, exc)
        return {"symbol": symbol, "error": str(exc)[:200]}


def fetch_dd_batch(symbols: list[str]) -> list[dict]:
    """Fetch DD sequentially with 1 s throttle."""
    results = []
    for i, sym in enumerate(symbols):
        if i > 0:
            time.sleep(1)
        results.append(fetch_dd(sym))
    return results
