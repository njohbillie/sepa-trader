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
    client = httpx.Client(
        headers=_COMMON_HEADERS,
        follow_redirects=True,
        timeout=15,
    )
    try:
        # Step 1: fc.yahoo.com sets initial tracking cookies Yahoo requires
        try:
            client.get("https://fc.yahoo.com")
            time.sleep(0.3)
        except Exception:
            pass  # non-fatal — best effort

        # Step 2: Finance home with full browser-like headers to get A1/A3 cookies
        client.get(
            "https://finance.yahoo.com/",
            headers={
                **_COMMON_HEADERS,
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,image/webp,*/*;q=0.8"
                ),
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        time.sleep(1.5)  # give Yahoo time to fully set session cookies

        # Step 3: Crumb — try query2 first, fall back to query1
        r = None
        for host in ("query2", "query1"):
            r = client.get(
                f"https://{host}.finance.yahoo.com/v1/test/getcrumb",
                headers={
                    **_COMMON_HEADERS,
                    "Accept": "text/plain, */*; q=0.01",
                    "Referer": "https://finance.yahoo.com/",
                },
            )
            crumb = r.text.strip()
            if crumb and len(crumb) <= 40 and "<" not in crumb and "{" not in crumb:
                logger.info("Crumb acquired via %s: %s…", host, crumb[:6])
                return crumb, dict(client.cookies)
            logger.warning(
                "Bad crumb from %s (status=%s): %s", host, r.status_code, r.text[:120]
            )

        raise RuntimeError(
            f"Could not obtain valid crumb after trying both hosts. "
            f"Last response: {r.text[:120] if r else 'no response'}"
        )
    finally:
        client.close()


def _force_refresh():
    """Clear stale session state and re-authenticate."""
    global _crumb, _cookies
    _crumb = None
    _cookies = {}
    _crumb, _cookies = _refresh_session()
    logger.info("Yahoo Finance session refreshed (crumb: %s…)", _crumb[:6])


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
    headers = {
        **_COMMON_HEADERS,
        "Accept": "application/json",
        "Referer": "https://finance.yahoo.com/",
    }

    resp = httpx.get(
        url,
        params=params,
        headers=headers,
        cookies=_cookies,
        timeout=15,
        follow_redirects=True,
    )

    if resp.status_code == 401:
        logger.info("Yahoo Finance session expired — refreshing.")
        _force_refresh()
        params["crumb"] = _crumb
        resp = httpx.get(
            url,
            params=params,
            headers=headers,
            cookies=_cookies,
            timeout=15,
            follow_redirects=True,
        )

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

        target_mean = v("financialData", "targetMeanPrice")
        target_high = v("financialData", "targetHighPrice")
        target_low  = v("financialData", "targetLowPrice")

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
            "target_mean":     float(target_mean) if target_mean is not None else None,
            "target_high":     float(target_high) if target_high is not None else None,
            "target_low":      float(target_low)  if target_low  is not None else None,
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
