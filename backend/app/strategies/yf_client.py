"""
Shared yfinance session helper.

Yahoo Finance blocks requests from cloud/container environments that don't send
a browser-like User-Agent.  All strategy modules should import `get_ticker` and
`yf_download` from here rather than calling yfinance directly.
"""
import requests
import yfinance as yf

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def get_ticker(symbol: str) -> yf.Ticker:
    """Return a yfinance Ticker backed by a session with proper browser headers."""
    return yf.Ticker(symbol, session=_make_session())
