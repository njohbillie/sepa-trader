"""
Market environment assessment.

Classifies the current market regime using:
  • SPY price vs 200-day SMA (trend direction)
  • VIX level (volatility regime)
  • SPY 20-day return (short-term momentum)

Used by the AI strategist to select the most appropriate strategy
and to weight its recommendations accordingly.
"""
import logging
from datetime import datetime

from .yf_client import get_ticker

logger = logging.getLogger(__name__)


def assess() -> dict:
    """
    Returns a market environment dict:
      environment      — BULL | BULL_VOLATILE | CORRECTION | BEAR | TRANSITIONAL
      description      — one-line summary for AI context
      spy_price        — current SPY price
      spy_200sma       — 200-day simple moving average
      spy_above_200    — bool
      spy_20d_return   — SPY return over last 20 trading days (%)
      vix              — current VIX reading
      assessed_at      — ISO timestamp
    """
    try:
        spy_hist = get_ticker("SPY").history(period="1y", auto_adjust=True)
        if len(spy_hist) < 200:
            raise ValueError(f"Insufficient SPY history (got {len(spy_hist)} rows)")

        spy_price  = float(spy_hist["Close"].iloc[-1])
        spy_200sma = float(spy_hist["Close"].rolling(200).mean().iloc[-1])
        spy_20d    = float((spy_hist["Close"].iloc[-1] / spy_hist["Close"].iloc[-20] - 1) * 100)
        spy_above  = spy_price > spy_200sma

    except Exception as exc:
        logger.warning("market_env: SPY fetch failed (%s) — returning UNKNOWN", exc)
        return {
            "environment":    "UNKNOWN",
            "description":    "Market data unavailable",
            "spy_price":      None,
            "spy_200sma":     None,
            "spy_above_200":  None,
            "spy_20d_return": None,
            "vix":            None,
            "assessed_at":    datetime.utcnow().isoformat(),
        }

    try:
        vix_hist = get_ticker("^VIX").history(period="5d", auto_adjust=True)
        vix = float(vix_hist["Close"].iloc[-1]) if len(vix_hist) > 0 else 20.0
    except Exception as exc:
        logger.warning("market_env: VIX fetch failed (%s) — defaulting to 20", exc)
        vix = 20.0

    # ── Regime classification ─────────────────────────────────────────────────
    if spy_above and vix < 20:
        env  = "BULL"
        desc = "Strong uptrend, low volatility — trend-following strategies favoured"
    elif spy_above and vix < 30:
        env  = "BULL_VOLATILE"
        desc = "Uptrend intact but elevated volatility — momentum with tighter risk"
    elif not spy_above and vix > 30:
        env  = "BEAR"
        desc = "Downtrend with high volatility — defensive positioning (bonds/cash)"
    elif not spy_above:
        env  = "CORRECTION"
        desc = "SPY below 200SMA — rotation to bonds or mean-reversion setups"
    else:
        env  = "TRANSITIONAL"
        desc = "Mixed signals — await confirmation before deploying new capital"

    return {
        "environment":    env,
        "description":    desc,
        "spy_price":      round(spy_price, 2),
        "spy_200sma":     round(spy_200sma, 2),
        "spy_above_200":  spy_above,
        "spy_20d_return": round(spy_20d, 2),
        "vix":            round(vix, 2),
        "assessed_at":    datetime.utcnow().isoformat(),
    }


# ── Strategy fit scores per regime ───────────────────────────────────────────
# Used by the AI strategist when no API key is configured.

STRATEGY_FIT: dict[str, dict[str, int]] = {
    # environment          sepa  dual_momentum  sector_rotation  mean_reversion
    "BULL":           {"sepa": 9, "dual_momentum": 8, "sector_rotation": 8, "mean_reversion": 4},
    "BULL_VOLATILE":  {"sepa": 6, "dual_momentum": 7, "sector_rotation": 6, "mean_reversion": 7},
    "CORRECTION":     {"sepa": 3, "dual_momentum": 6, "sector_rotation": 5, "mean_reversion": 8},
    "BEAR":           {"sepa": 1, "dual_momentum": 7, "sector_rotation": 4, "mean_reversion": 6},
    "TRANSITIONAL":   {"sepa": 5, "dual_momentum": 6, "sector_rotation": 5, "mean_reversion": 5},
    "UNKNOWN":        {"sepa": 5, "dual_momentum": 5, "sector_rotation": 5, "mean_reversion": 5},
}
