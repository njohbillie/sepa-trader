"""
Unofficial TradingView watchlist API client.

Authenticates with username/password, then creates or replaces
the 'weekly_picks' watchlist with the screener's top 10 symbols.

Note: uses TradingView's internal REST API which may change without notice.
2FA is not supported — use an account without two-factor authentication.
"""
import logging
import httpx

logger = logging.getLogger(__name__)

TV_BASE        = "https://www.tradingview.com"
SIGNIN_URL     = f"{TV_BASE}/accounts/signin/"
WATCHLIST_API  = f"{TV_BASE}/api/v1/symbols_list/watchlists/"
SCREENER_API   = f"{TV_BASE}/api/v1/symbols_list/screeners/"
SCAN_URL       = "https://scanner.tradingview.com/america/scan"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Stocks in our universe that trade on NYSE (everything else defaults to NASDAQ)
_NYSE = {
    "JPM", "BAC", "GS", "MS", "V", "MA", "WFC", "BX", "AXP", "SPGI",
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "TMO", "ISRG", "REGN", "VRTX",
    "ORCL", "NOW", "WMT", "HD", "NKE", "MCD", "TJX", "DECK", "ONON",
    "XOM", "CVX", "COP", "SLB", "CAT", "DE", "HON", "LMT", "RTX", "GE",
    "UNP", "CSX", "DIS", "ENPH", "FSLR", "BRK.B",
}


def to_tv_symbol(symbol: str) -> str:
    """Return exchange-prefixed symbol for TradingView (e.g. NASDAQ:AAPL)."""
    exchange = "NYSE" if symbol.upper() in _NYSE else "NASDAQ"
    return f"{exchange}:{symbol.upper()}"


def _headers(csrf: str = "") -> dict:
    h = {
        "User-Agent":  _UA,
        "Referer":     TV_BASE + "/",
        "Accept":      "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }
    if csrf:
        h["X-CSRFToken"] = csrf
    return h


def _signin(username: str, password: str) -> tuple[dict, str]:
    """
    Authenticate and return (cookies_dict, csrf_token).
    Raises RuntimeError on failure.
    """
    with httpx.Client(follow_redirects=True, timeout=30) as client:
        # Visit homepage first to get initial CSRF cookie
        client.get(TV_BASE + "/", headers={"User-Agent": _UA})
        csrf = client.cookies.get("csrftoken", "")

        resp = client.post(
            SIGNIN_URL,
            data={"username": username, "password": password, "remember_me": "on"},
            headers=_headers(csrf),
        )

        if resp.status_code not in (200, 204):
            raise RuntimeError(f"Signin HTTP {resp.status_code}: {resp.text[:200]}")

        try:
            body = resp.json()
            if isinstance(body, dict) and body.get("error"):
                raise RuntimeError(f"Signin rejected: {body['error']}")
        except ValueError:
            pass  # non-JSON response is OK if status was 200

        csrf = client.cookies.get("csrftoken", csrf)
        return dict(client.cookies), csrf


def _list_watchlists(cookies: dict, csrf: str) -> list:
    with httpx.Client(cookies=cookies, timeout=30) as client:
        resp = client.get(WATCHLIST_API, headers=_headers(csrf))
        resp.raise_for_status()
        data = resp.json()
        # API may return {"payload": [...]} or a bare list
        return data.get("payload", data) if isinstance(data, dict) else data


def _create_watchlist(cookies: dict, csrf: str, name: str, tv_symbols: list[str]) -> dict:
    payload = {
        "name": name,
        "symbols": {"content": [{"symbol": s} for s in tv_symbols]},
    }
    with httpx.Client(cookies=cookies, timeout=30) as client:
        resp = client.post(WATCHLIST_API, json=payload, headers=_headers(csrf))
        resp.raise_for_status()
        return resp.json()


def _update_watchlist(cookies: dict, csrf: str, wl_id: str, name: str, tv_symbols: list[str]) -> dict:
    payload = {
        "name": name,
        "symbols": {"content": [{"symbol": s} for s in tv_symbols]},
    }
    with httpx.Client(cookies=cookies, timeout=30) as client:
        resp = client.put(
            f"{WATCHLIST_API}{wl_id}/",
            json=payload,
            headers=_headers(csrf),
        )
        resp.raise_for_status()
        return resp.json()


def list_saved_screeners(username: str, password: str) -> list[dict]:
    """
    Return the user's saved TradingView screeners as
    [{"id": "...", "name": "...", "symbol_count": N}, ...]

    Tries the watchlist-style screener API first, then falls back to the
    scanner saved-screener endpoint.  Returns an empty list on any error.
    """
    try:
        cookies, csrf = _signin(username, password)
        with httpx.Client(cookies=cookies, timeout=30) as client:
            # Primary attempt — same API family as watchlists
            resp = client.get(SCREENER_API, headers=_headers(csrf))

            # Fallback: scanner-domain saved list
            if resp.status_code in (404, 403, 401):
                resp = client.get(
                    "https://scanner.tradingview.com/screener/saved/list",
                    headers=_headers(csrf),
                )

            resp.raise_for_status()
            data = resp.json()
            items = data.get("payload", data) if isinstance(data, dict) else data
            if not isinstance(items, list):
                return []

            return [
                {
                    "id":           str(item.get("id", "")),
                    "name":         item.get("name", ""),
                    "symbol_count": item.get("symbol_count", item.get("count", None)),
                }
                for item in items
                if item.get("name")
            ]
    except Exception as exc:
        logger.error("list_saved_screeners failed: %s", exc)
        return []


def run_saved_screener(
    username: str,
    password: str,
    screener_name: str,
) -> tuple[list[str], str]:
    """
    Authenticate with TradingView, find the saved screener by name, and
    execute its filter set against the US stock universe.

    Returns (symbols, error_message).
    symbols is empty and error_message is non-empty on failure.

    Two execution strategies are attempted:
      1. Fetch the screener's saved filter config and re-run it via the scanner API.
      2. If the screener stores a symbol list (like a watchlist), return that directly.
    """
    try:
        cookies, csrf = _signin(username, password)
    except Exception as exc:
        return [], f"TradingView sign-in failed: {exc}"

    # ── Step 1: Find screener by name ─────────────────────────────────────────
    screener_id = None
    screener_filters = None

    with httpx.Client(cookies=cookies, timeout=30) as client:
        for url in (SCREENER_API, "https://scanner.tradingview.com/screener/saved/list"):
            try:
                resp = client.get(url, headers=_headers(csrf))
                if resp.status_code not in (200, 201):
                    continue
                data = resp.json()
                items = data.get("payload", data) if isinstance(data, dict) else data
                if not isinstance(items, list):
                    continue
                match = next(
                    (i for i in items if i.get("name", "").lower() == screener_name.lower()),
                    None,
                )
                if match:
                    screener_id = str(match.get("id", ""))
                    # Some APIs embed the filter directly in the list response
                    screener_filters = match.get("filters") or match.get("filter")
                    break
            except Exception:
                continue

    if not screener_id:
        return [], f"Screener '{screener_name}' not found in your saved TradingView screeners."

    # ── Step 2: Fetch filter config if not already embedded ───────────────────
    if not screener_filters and screener_id:
        with httpx.Client(cookies=cookies, timeout=30) as client:
            for url in (
                f"{SCREENER_API}{screener_id}/",
                f"https://scanner.tradingview.com/screener/saved/{screener_id}",
            ):
                try:
                    resp = client.get(url, headers=_headers(csrf))
                    if resp.status_code not in (200, 201):
                        continue
                    data = resp.json()
                    payload = data.get("payload", data) if isinstance(data, dict) else data
                    screener_filters = payload.get("filters") or payload.get("filter")
                    if screener_filters:
                        break
                except Exception:
                    continue

    # ── Step 3a: Re-run the saved filter set via the scanner API ─────────────
    if screener_filters:
        try:
            resp = httpx.post(
                SCAN_URL,
                json={
                    "filter":  screener_filters,
                    "columns": ["close"],
                    "range":   [0, 500],
                    "markets": ["america"],
                },
                timeout=30,
                headers={
                    "User-Agent": _UA,
                    "Origin":     TV_BASE + "/",
                    "Referer":    TV_BASE + "/",
                    **{k: v for k, v in (httpx.Cookies(cookies) if hasattr(cookies, "items") else {}).items()},
                },
                cookies=cookies,
            )
            resp.raise_for_status()
            symbols = [row["s"].split(":")[-1] for row in resp.json().get("data", [])]
            logger.info("run_saved_screener '%s': %d symbols via filter re-run", screener_name, len(symbols))
            return symbols, ""
        except Exception as exc:
            logger.warning("run_saved_screener filter re-run failed: %s — trying symbol list", exc)

    # ── Step 3b: Fallback — screener saved as a symbol list (like a watchlist) ─
    with httpx.Client(cookies=cookies, timeout=30) as client:
        for url in (
            f"{SCREENER_API}{screener_id}/",
            f"https://scanner.tradingview.com/screener/saved/{screener_id}",
        ):
            try:
                resp = client.get(url, headers=_headers(csrf))
                if resp.status_code not in (200, 201):
                    continue
                data = resp.json()
                payload = data.get("payload", data) if isinstance(data, dict) else data
                # Symbol list formats used by different TV endpoints
                symbols = (
                    [s["symbol"] for s in payload.get("symbols", {}).get("content", [])]
                    or [s.get("symbol") or s.get("s") for s in payload.get("symbols", [])]
                    or []
                )
                symbols = [s.split(":")[-1] for s in symbols if s]
                if symbols:
                    logger.info(
                        "run_saved_screener '%s': %d symbols via symbol list",
                        screener_name, len(symbols),
                    )
                    return symbols, ""
            except Exception:
                continue

    return [], (
        f"Screener '{screener_name}' was found (id={screener_id}) but its results "
        "could not be fetched — TradingView's API may not support reading filter "
        "configurations for this screener type."
    )


def get_session_cookie(username: str, password: str) -> str:
    """
    Sign in to TradingView and return the session cookie string
    suitable for use in an HTTP Cookie header.
    Returns empty string on failure.
    """
    try:
        cookies, _ = _signin(username, password)
        # Build a Cookie header string from the dict
        return "; ".join(f"{k}={v}" for k, v in cookies.items())
    except Exception as exc:
        import logging as _logging
        _logging.getLogger(__name__).warning("get_session_cookie failed: %s", exc)
        return ""


def update_weekly_picks(
    username: str,
    password: str,
    symbols: list[str],
    watchlist_name: str = "weekly_picks",
) -> dict:
    """
    Create or replace the named TradingView watchlist with the given symbols.

    Returns {"ok": True, "action": "created"|"updated", "count": N}
    on success, or {"ok": False, "error": "..."} on failure.
    """
    try:
        cookies, csrf = _signin(username, password)
        tv_syms = [to_tv_symbol(s) for s in symbols]

        watchlists = _list_watchlists(cookies, csrf)
        existing = next((w for w in watchlists if w.get("name") == watchlist_name), None)

        if existing:
            _update_watchlist(cookies, csrf, str(existing["id"]), watchlist_name, tv_syms)
            action = "updated"
        else:
            _create_watchlist(cookies, csrf, watchlist_name, tv_syms)
            action = "created"

        logger.info(
            "TradingView watchlist '%s' %s with %d symbols: %s",
            watchlist_name, action, len(tv_syms), tv_syms,
        )
        return {"ok": True, "action": action, "count": len(tv_syms)}

    except Exception as exc:
        logger.error("TradingView watchlist sync failed: %s", exc, exc_info=True)
        return {"ok": False, "error": str(exc)}
