"""
Backtest harness for the Dual Momentum (GEM) strategy.

Pulls daily adjusted-close history for SPY / EFA / AGG / BIL from Yahoo Finance,
walks the time series at the configured rebalance frequency, and replays the
GEM decision rule using ONLY data available at each rebalance date.
No look-ahead bias.

Output:
    summary   — CAGR, Sharpe, max drawdown, rotations, total return
    benchmark — buy-and-hold SPY for the same window
    signals   — every rotation with date, from→to, equity at that point
    equity_curve — sampled monthly so the JSON response stays small
"""
import logging
from datetime import datetime
from typing import Literal

import pandas as pd

from .yf_client import fetch_history

logger = logging.getLogger(__name__)

UNIVERSE: tuple[str, ...] = ("SPY", "EFA", "AGG", "BIL")


def _gem_signal(closes: pd.DataFrame, asof: pd.Timestamp, lookback_months: int) -> str:
    """Replay GEM at `asof` using data in [asof - lookback_months, asof]."""
    cutoff_lo = asof - pd.DateOffset(months=lookback_months)
    window = closes.loc[(closes.index >= cutoff_lo) & (closes.index <= asof)]
    if len(window) < 20:
        return "BIL"  # insufficient lookback — park in cash equivalent

    rets: dict[str, float] = {}
    for sym in UNIVERSE:
        first = window[sym].iloc[0]
        last  = window[sym].iloc[-1]
        if pd.isna(first) or pd.isna(last) or first == 0:
            return "BIL"
        rets[sym] = (last / first) - 1.0

    if rets["SPY"] > rets["BIL"]:
        return "SPY" if rets["SPY"] >= rets["EFA"] else "EFA"
    return "AGG"


def _rebalance_dates(
    index: pd.DatetimeIndex,
    frequency: str,
    start: pd.Timestamp,
) -> list[pd.Timestamp]:
    """Generate rebalance dates restricted to actual trading days.

    monthly  — last trading day of each month
    biweekly — every 10th trading day
    weekly   — every 5th trading day
    """
    in_range = index[index >= start]
    if len(in_range) == 0:
        return []

    if frequency == "monthly":
        result: list[pd.Timestamp] = []
        months = pd.period_range(
            in_range.min().to_period("M"),
            in_range.max().to_period("M"),
            freq="M",
        )
        for m in months:
            mask = (in_range >= m.start_time) & (in_range <= m.end_time)
            members = in_range[mask]
            if len(members) > 0:
                result.append(members[-1])
        return result
    if frequency == "biweekly":
        return [in_range[i] for i in range(0, len(in_range), 10)]
    if frequency == "weekly":
        return [in_range[i] for i in range(0, len(in_range), 5)]
    raise ValueError(f"unknown frequency: {frequency}")


def run_backtest(
    start_year: int = 2010,
    end_year: int | None = None,
    lookback_months: int = 12,
    frequency: Literal["monthly", "biweekly", "weekly"] = "monthly",
    initial_capital: float = 10_000.0,
) -> dict:
    end_year = end_year or datetime.utcnow().year

    # Pull enough history to cover the lookback at start_year too
    period_days = (end_year - start_year + 2) * 372

    series: dict[str, pd.Series] = {}
    for sym in UNIVERSE:
        df = fetch_history(sym, period_days=period_days)
        if df.empty:
            return {"status": "error", "error": f"no data for {sym}"}
        # Force tz-naive so all four series align cleanly
        idx = df.index
        if getattr(idx, "tz", None) is not None:
            df = df.copy()
            df.index = idx.tz_localize(None)
        series[sym] = df["Close"]

    closes = pd.DataFrame(series).sort_index()
    closes = closes.dropna()
    if closes.empty:
        return {"status": "error", "error": "no overlapping data across universe"}

    start_ts = pd.Timestamp(start_year, 1, 1)
    if closes.index[-1] < start_ts:
        return {"status": "error", "error": f"no data on or after {start_ts.date()}"}

    rebalance_set = set(_rebalance_dates(closes.index, frequency, start_ts))
    if not rebalance_set:
        return {"status": "error", "error": "no rebalance dates in range"}

    # ── Walk forward ────────────────────────────────────────────────────────
    cash         = initial_capital
    holding_sym  = None
    holding_qty  = 0.0
    equity_curve = []
    signal_log   = []

    in_range = closes.loc[closes.index >= start_ts]
    for ts, row in in_range.iterrows():
        position_value = holding_qty * row[holding_sym] if holding_sym else 0.0
        equity = cash + position_value
        equity_curve.append({
            "date":    ts.strftime("%Y-%m-%d"),
            "equity":  round(equity, 2),
            "holding": holding_sym or "CASH",
        })

        if ts in rebalance_set:
            target = _gem_signal(closes, ts, lookback_months)
            if target != holding_sym:
                if holding_sym:
                    cash += holding_qty * row[holding_sym]
                price = float(row[target])
                if price <= 0:
                    continue
                holding_qty = (cash * 0.98) / price   # 98% leaves slippage buffer
                cash -= holding_qty * price
                signal_log.append({
                    "date":          ts.strftime("%Y-%m-%d"),
                    "from":          holding_sym or "CASH",
                    "to":            target,
                    "price":         round(price, 2),
                    "qty":           round(holding_qty, 2),
                    "equity_before": round(equity, 2),
                })
                holding_sym = target

    # ── Metrics ─────────────────────────────────────────────────────────────
    final_equity = equity_curve[-1]["equity"]
    eq_series    = pd.Series([e["equity"] for e in equity_curve])
    years        = (in_range.index[-1] - in_range.index[0]).days / 365.25 or 1.0

    total_ret = (final_equity / initial_capital) - 1.0
    cagr      = ((final_equity / initial_capital) ** (1 / years)) - 1.0

    daily_rets = eq_series.pct_change().dropna()
    sharpe     = (daily_rets.mean() / daily_rets.std()) * (252 ** 0.5) \
                 if daily_rets.std() and daily_rets.std() > 0 else 0.0

    rolling_max = eq_series.cummax()
    drawdown    = (eq_series - rolling_max) / rolling_max
    max_dd      = float(drawdown.min())

    # Buy-and-hold SPY benchmark over same window
    spy_first    = float(in_range["SPY"].iloc[0])
    spy_last     = float(in_range["SPY"].iloc[-1])
    bh_total_ret = (spy_last / spy_first) - 1.0
    bh_cagr      = ((spy_last / spy_first) ** (1 / years)) - 1.0

    bh_series  = in_range["SPY"]
    bh_dd      = (bh_series - bh_series.cummax()) / bh_series.cummax()
    bh_max_dd  = float(bh_dd.min())

    # Sample equity curve to ~monthly frequency to keep response payload small
    sample_step = max(1, len(equity_curve) // 240)
    sampled = equity_curve[::sample_step]
    if sampled[-1]["date"] != equity_curve[-1]["date"]:
        sampled.append(equity_curve[-1])

    return {
        "status": "ok",
        "params": {
            "start_year":      start_year,
            "end_year":        end_year,
            "lookback_months": lookback_months,
            "frequency":       frequency,
            "initial_capital": initial_capital,
        },
        "summary": {
            "final_equity":  round(final_equity, 2),
            "total_return":  round(total_ret, 4),
            "cagr":          round(cagr, 4),
            "sharpe":        round(sharpe, 2),
            "max_drawdown":  round(max_dd, 4),
            "rotations":     len(signal_log),
            "years":         round(years, 1),
        },
        "benchmark_spy": {
            "total_return": round(bh_total_ret, 4),
            "cagr":         round(bh_cagr, 4),
            "max_drawdown": round(bh_max_dd, 4),
        },
        "signals":      signal_log,
        "equity_curve": sampled,
    }
