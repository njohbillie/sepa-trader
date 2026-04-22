"""
Dual Momentum — Gary Antonacci's Global Equity Momentum (GEM)

Algorithm:
  Step 1 — Absolute momentum:
    If SPY 12-month return > BIL (T-bill proxy) → equities have positive
    absolute momentum; proceed to Step 2.
    Otherwise → hold AGG (aggregate bonds, defensive).

  Step 2 — Relative momentum:
    Compare SPY vs EFA (international equities).
    Hold whichever has the higher 12-month return.

Rebalance: monthly (or on demand).
Universe: SPY, EFA, AGG, BIL
"""
import logging
from datetime import datetime, timedelta

from .yf_client import get_ticker

logger = logging.getLogger(__name__)

ASSETS = {
    "SPY": "US Equities (S&P 500)",
    "EFA": "International Equities (MSCI EAFE)",
    "AGG": "US Aggregate Bonds",
    "BIL": "Short-term T-Bills (cash proxy)",
}

DEFAULT_LOOKBACK_MONTHS = 12


def _fetch_momentum(symbol: str, months: int) -> float:
    """
    Total price return over *months* calendar months.
    Uses adjusted close to account for dividends/splits.
    Returns 0.0 on any data error.
    """
    try:
        end   = datetime.today()
        start = end - timedelta(days=int(months * 31.5))
        hist  = get_ticker(symbol).history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            auto_adjust=True,
        )
        if len(hist) < 20:
            logger.warning("dual_momentum: insufficient history for %s", symbol)
            return 0.0
        first = float(hist["Close"].iloc[0])
        last  = float(hist["Close"].iloc[-1])
        return (last / first) - 1.0
    except Exception as exc:
        logger.error("dual_momentum: failed to fetch %s: %s", symbol, exc)
        return 0.0


def _fetch_current_price(symbol: str) -> float:
    try:
        hist = get_ticker(symbol).history(period="2d", auto_adjust=True)
        return float(hist["Close"].iloc[-1]) if len(hist) > 0 else 0.0
    except Exception:
        return 0.0


def evaluate(lookback_months: int = DEFAULT_LOOKBACK_MONTHS) -> dict:
    """
    Run the GEM algorithm.

    Returns a dict with:
      recommended_symbol  — SPY | EFA | AGG
      asset_class         — human-readable name
      action              — BUY | HOLD | SWITCH
      momentum            — {symbol: float} for all four assets
      prices              — {symbol: float} current prices
      reasoning           — plain-English explanation
      lookback_months     — months used
      evaluated_at        — ISO timestamp
    """
    logger.info("dual_momentum: fetching %d-month momentum for all assets…", lookback_months)

    momentum = {sym: _fetch_momentum(sym, lookback_months) for sym in ASSETS}
    prices   = {sym: _fetch_current_price(sym) for sym in ASSETS}

    spy_mom = momentum["SPY"]
    efa_mom = momentum["EFA"]
    bil_mom = momentum["BIL"]

    # ── GEM decision tree ─────────────────────────────────────────────────────
    if spy_mom > bil_mom:
        # Absolute momentum positive — go equities
        if spy_mom >= efa_mom:
            recommended = "SPY"
            reasoning = (
                f"Absolute momentum positive: US equities ({spy_mom:+.1%}) beat T-bills "
                f"({bil_mom:+.1%}). Relative momentum: US ({spy_mom:+.1%}) leads "
                f"international ({efa_mom:+.1%}). → Hold SPY."
            )
        else:
            recommended = "EFA"
            reasoning = (
                f"Absolute momentum positive: equities beat T-bills ({bil_mom:+.1%}). "
                f"Relative momentum: international ({efa_mom:+.1%}) leads US "
                f"({spy_mom:+.1%}). → Hold EFA."
            )
    else:
        recommended = "AGG"
        reasoning = (
            f"Absolute momentum negative: US equities ({spy_mom:+.1%}) below T-bills "
            f"({bil_mom:+.1%}). Defensive posture. → Hold AGG (bonds)."
        )

    return {
        "recommended_symbol": recommended,
        "asset_class":        ASSETS[recommended],
        "momentum":           {k: round(v, 4) for k, v in momentum.items()},
        "prices":             {k: round(v, 2) for k, v in prices.items()},
        "reasoning":          reasoning,
        "lookback_months":    lookback_months,
        "evaluated_at":       datetime.utcnow().isoformat(),
    }
