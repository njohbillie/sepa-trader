"""
Shared utilities.
"""
import math


def sf(v, default=None):
    """
    Safe float — converts None / nan / inf to `default` (None by default).

    Use this everywhere Alpaca API or yfinance data is converted to float
    before being placed in a JSON response, to prevent:
        ValueError: Out of range float values are not JSON compliant
    """
    if v is None:
        return default
    try:
        f = float(v)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return default
