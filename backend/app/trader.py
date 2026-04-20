"""
Auto-execution engine: fires trades based on SEPA signals.
Every 30-min cycle:
  1. Trailing stop adjustment (green positions only)
  2. Exit guard (ensure every position has an OCO)
  3. Signal evaluation
  4. Watchlist breakout entries
Pre-trade AI gate + live account size limits applied throughout.
"""
import asyncio
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from . import alpaca_client as alp
from .sepa_analyzer import analyze
from .database import get_setting
from . import telegram_alerts as tg

logger = logging.getLogger(__name__)


def _size_position(portfolio_value: float, price: float, risk_pct: float, stop_pct: float) -> float:
    risk_dollars = portfolio_value * (risk_pct / 100)
    stop_dollars = price * (stop_pct / 100)
    if stop_dollars <= 0:
        return 0
    return risk_dollars / stop_dollars


def _effective_max_positions(db: Session, mode: str) -> int:
    """
    For live accounts under $10K, cap max_positions at 3 regardless of
    what the settings say. Paper accounts always use the settings value.
    """
    configured = int(get_setting(db, "max_positions", "10"))
    if mode != "live":
        return configured
    try:
        from .database import get_live_account_limits
        acct   = alp.get_account(mode)
        limits = get_live_account_limits(float(acct.portfolio_value))
        cap    = limits.get("max_positions")
        if cap is not None:
            effective = min(configured, cap)
            if effective != configured:
                logger.info(
                    "Live account <$10K: max_positions capped at %d (settings=%d)",
                    effective, configured,
                )
            return effective
    except Exception as exc:
        logger.warning("_effective_max_positions: could not fetch account — using settings: %s", exc)
    return configured


def _get_weekly_plan_exits(db: Session, symbol: str, mode: str) -> tuple[float, float]:
    """Return (stop_price, target1) — most recent plan row for this symbol+mode."""
    row = db.execute(
        text("""
            SELECT stop_price, target1
            FROM weekly_plan
            WHERE symbol = :sym
              AND mode = :mode
            ORDER BY week_start DESC
            LIMIT 1
        """),
        {"sym": symbol, "mode": mode},
    ).fetchone()
    if not row:
        return 0.0, 0.0
    return float(row[0] or 0), float(row[1] or 0)


def _get_current_stop_price(orders: list) -> float | None:
    """Extract the active stop price from open OCO/bracket order legs."""
    for o in orders:
        order_class = str(getattr(o, 'order_class', '') or '').lower()
        side        = str(getattr(o, 'side',        '') or '').lower()

        if 'sell' not in side:
            continue

        if any(kw in order_class for kw in ('oco', 'bracket', 'oto')):
            legs = getattr(o, 'legs', None) or []
            for leg in legs:
                order_type = str(getattr(leg, 'type', '') or '').lower()
                if 'stop' in order_type:
                    sp = getattr(leg, 'stop_price', None)
                    if sp is not None:
                        return float(sp)
            sp = getattr(o, 'stop_price', None)
            if sp is not None:
                return float(sp)

    return None


# ── Trailing stop tiers ───────────────────────────────────────────────────────
# Only applied to green positions. Red positions are never touched.
#
# Tier 3 (≥3R): trail 1R below current price (dynamic)
# Tier 2 (≥2R): fix stop at entry + 1R (lock in 1R profit)
# Tier 1 (≥1R): fix stop at entry (breakeven)
# <1R:           no adjustment
_TRAIL_TIERS = [
    (3.0, lambda entry, R, price: price - R),
    (2.0, lambda entry, R, price: entry + R),
    (1.0, lambda entry, R, price: entry),
]

# Minimum improvement before replacing — avoids API churn for tiny moves
_MIN_STOP_IMPROVEMENT_PCT = 0.005


def _compute_new_stop(entry: float, original_stop: float, current_price: float) -> float | None:
    """Return the ratcheted stop level or None if no adjustment warranted."""
    R = entry - original_stop
    if R <= 0:
        return None

    gain_r = (current_price - entry) / R

    for threshold, fn in _TRAIL_TIERS:
        if gain_r >= threshold:
            new_stop = round(fn(entry, R, current_price), 2)
            max_stop = round(current_price * 0.995, 2)  # never within 0.5% of price
            return min(new_stop, max_stop)

    return None


def _adjust_trailing_stops(
    db: Session,
    positions: list,
    open_orders_by_symbol: dict,
    mode: str,
):
    """
    Ratchet stops upward for green positions each 30-minute cycle.
    Red positions (current_price <= entry) are completely skipped.
    Updates weekly_plan.stop_price so the exit guard stays in sync.
    """
    for pos in positions:
        sym           = pos.symbol
        current_price = float(pos.current_price)
        entry         = float(pos.avg_entry_price)
        qty           = float(pos.qty)

        # Never touch red positions
        if current_price <= entry:
            continue

        stop_orig, target = _get_weekly_plan_exits(db, sym, mode)
        if stop_orig <= 0 or target <= 0:
            logger.debug("Trailing stop: %s has no plan exits — skipping.", sym)
            continue

        new_stop = _compute_new_stop(entry, stop_orig, current_price)
        if new_stop is None:
            continue

        current_stop = _get_current_stop_price(open_orders_by_symbol.get(sym, []))
        effective_current = current_stop if current_stop else stop_orig

        if new_stop <= effective_current * (1 + _MIN_STOP_IMPROVEMENT_PCT):
            logger.debug(
                "Trailing stop: %s new=$%.2f vs current=$%.2f — no update needed.",
                sym, new_stop, effective_current,
            )
            continue

        R      = entry - stop_orig
        gain_r = (current_price - entry) / R

        logger.info(
            "Trailing stop: %s  gain=%.1fR  price=$%.2f  stop $%.2f → $%.2f  target=$%.2f [%s]",
            sym, gain_r, current_price, effective_current, new_stop, target, mode,
        )

        try:
            alp.replace_oca_exit(sym, qty, new_stop, target, mode)

            db.execute(
                text("""
                    UPDATE weekly_plan
                    SET stop_price = :stop
                    WHERE symbol = :sym AND mode = :mode
                      AND week_start = (
                          SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
                      )
                """),
                {"stop": new_stop, "sym": sym, "mode": mode},
            )
            db.commit()

            logger.info(
                "Trailing stop updated: %s $%.2f → $%.2f (%.1fR gain) [%s]",
                sym, effective_current, new_stop, gain_r, mode,
            )

        except Exception as exc:
            logger.error("Trailing stop update failed for %s: %s", sym, exc)


# ── Exit guard ────────────────────────────────────────────────────────────────

def _classify_exit_orders(open_orders_by_symbol: dict) -> tuple[set[str], dict]:
    oco_covered      = set()
    orphan_order_ids = {}

    for sym, orders in open_orders_by_symbol.items():
        for o in orders:
            order_class = str(getattr(o, 'order_class', '') or '').lower()
            side        = str(getattr(o, 'side',        '') or '').lower()
            is_sell     = 'sell' in side
            is_oco      = any(kw in order_class for kw in ('oco', 'bracket', 'oto'))

            if is_sell and is_oco:
                oco_covered.add(sym)
                break
            if is_sell and not is_oco:
                orphan_order_ids.setdefault(sym, []).append(str(o.id))

    return oco_covered, orphan_order_ids


def _ensure_exit_orders(
    db: Session,
    positions: list,
    open_orders_by_symbol: dict,
    mode: str,
):
    """
    Ensure every live position has an active OCO exit order.
    Runs AFTER trailing stop adjustment so it sees the updated orders map.
    """
    oco_covered, orphan_order_ids = _classify_exit_orders(open_orders_by_symbol)
    client = alp.get_client(mode)

    for pos in positions:
        sym = pos.symbol
        if sym in oco_covered:
            continue

        for oid in orphan_order_ids.get(sym, []):
            try:
                client.cancel_order_by_id(oid)
                logger.info("Exit guard: cancelled orphaned sell %s for %s [%s]", oid, sym, mode)
            except Exception as exc:
                logger.warning("Exit guard: could not cancel %s for %s: %s", oid, sym, exc)

        qty          = float(pos.qty)
        stop, target = _get_weekly_plan_exits(db, sym, mode)

        if stop <= 0 or target <= 0:
            logger.warning(
                "Exit guard: %s has no OCO but no stop/target in plan [%s] "
                "— use 'Set Stop / Target' on the position card.", sym, mode,
            )
            continue

        try:
            alp.place_oca_exit(sym, qty, stop, target, mode)
            logger.info(
                "Exit guard: placed OCO for %s qty=%.0f stop=$%.2f target=$%.2f [%s]",
                sym, qty, stop, target, mode,
            )
        except Exception as exc:
            logger.error("Exit guard: failed to place OCO for %s: %s", sym, exc)


# ── Pre-trade gate ────────────────────────────────────────────────────────────

def _gate(
    db: Session,
    symbol: str,
    qty: float,
    entry: float,
    stop: float,
    target: float,
    trigger: str,
    mode: str,
) -> bool:
    """Pre-trade AI gate. Returns True if order should proceed. Fails open."""
    try:
        from .claude_analyst import pre_trade_analysis, log_pre_trade
        acct         = alp.get_account(mode)
        portfolio    = float(acct.portfolio_value)
        cash         = float(acct.cash)
        buying_power = float(acct.buying_power)

        result = pre_trade_analysis(
            db=db, symbol=symbol, side="BUY", qty=qty,
            entry_price=entry, stop_price=stop, target_price=target,
            trigger=trigger, portfolio_value=portfolio,
            cash=cash, buying_power=buying_power, mode=mode,
        )
        log_pre_trade(
            db, symbol, trigger,
            result["verdict"], result["reason"], result["analysis"], mode,
        )

        if not result["proceed"]:
            logger.warning("Pre-trade gate BLOCKED %s [%s]: %s", symbol, trigger, result["reason"])
            return False
        if result["warnings"]:
            logger.warning("Pre-trade gate WARNED %s [%s]: %s", symbol, trigger, ", ".join(result["warnings"]))
        logger.info("Pre-trade gate PASSED %s [%s]: %s", symbol, trigger, result["reason"])
        return True

    except Exception as exc:
        logger.error("Pre-trade gate error for %s: %s — proceeding.", symbol, exc)
        return True


# ── Main monitor ──────────────────────────────────────────────────────────────

async def run_monitor(db: Session):
    mode         = get_setting(db, "trading_mode", "paper")
    auto_execute = get_setting(db, "auto_execute", "true").lower() == "true"
    risk_pct     = float(get_setting(db, "risk_pct", "2.0"))
    stop_pct     = float(get_setting(db, "stop_loss_pct", "8.0"))

    try:
        clock       = alp.get_clock(mode)
        market_open = clock.is_open

        acct      = alp.get_account(mode)
        positions = alp.get_positions(mode)
        portfolio = float(acct.portfolio_value)
        day_pnl   = float(acct.equity) - float(acct.last_equity)

        if market_open and positions:
            try:
                open_orders_by_symbol = alp.get_open_orders_by_symbol(mode)

                # Step 1 — trailing stop adjustment (green positions only)
                _adjust_trailing_stops(db, positions, open_orders_by_symbol, mode)

                # Re-fetch after potential cancel+replace
                open_orders_by_symbol = alp.get_open_orders_by_symbol(mode)

                # Step 2 — exit guard (ensure OCO on every position)
                _ensure_exit_orders(db, positions, open_orders_by_symbol, mode)

            except Exception as exc:
                logger.error("Stop management cycle failed: %s", exc)

        # Step 3 — signal evaluation
        stage2_lost   = []
        new_breakouts = []
        results       = []

        for pos in positions:
            sym    = pos.symbol
            qty    = float(pos.qty)
            result = analyze(sym)
            signal = result.get("signal", "ERROR")

            _log_signal(db, sym, signal, result.get("score", 0), result.get("price"), mode)

            if signal == "NO_SETUP":
                stage2_lost.append(sym)
                if auto_execute and market_open:
                    try:
                        alp.close_position(sym, mode)
                        _log_trade(db, sym, "SELL", qty, result.get("price") or 0, "STAGE2_LOST", mode)
                    except Exception as e:
                        results.append({"sym": sym, "action": "SELL_FAILED", "error": str(e)})
            elif signal == "BREAKOUT":
                new_breakouts.append(sym)

            results.append({"sym": sym, "signal": signal})

        # Step 4 — watchlist breakout entries
        held_symbols = {p.symbol for p in positions}
        watchlist    = _get_watchlist(db)
        max_pos      = _effective_max_positions(db, mode)

        if auto_execute and market_open and len(positions) < max_pos:
            for sym in watchlist:
                if sym in held_symbols:
                    continue
                result = analyze(sym)
                signal = result.get("signal")
                _log_signal(db, sym, signal, result.get("score", 0), result.get("price"), mode)

                if signal == "BREAKOUT" and result.get("price"):
                    price        = result["price"]
                    qty          = _size_position(portfolio, price, risk_pct, stop_pct)
                    stop, target = _get_weekly_plan_exits(db, sym, mode)

                    if qty >= 1:
                        if not _gate(db, sym, qty, price, stop, target, "BREAKOUT", mode):
                            results.append({"sym": sym, "action": "BLOCKED_BY_AI"})
                            continue
                        try:
                            if stop > 0 and target > 0:
                                alp.place_bracket_buy(sym, qty, stop, target, mode)
                            else:
                                alp.place_market_buy(sym, qty, mode)
                                logger.warning(
                                    "Watchlist buy %s: no stop/target — plain market buy [%s]",
                                    sym, mode,
                                )
                            _log_trade(db, sym, "BUY", qty, price, "BREAKOUT", mode)
                            new_breakouts.append(sym)
                            held_symbols.add(sym)
                        except Exception as e:
                            results.append({"sym": sym, "action": "BUY_FAILED", "error": str(e)})

        if stage2_lost:
            asyncio.create_task(tg.alert_stage2_lost(stage2_lost, mode))
        if new_breakouts:
            asyncio.create_task(tg.alert_breakout(new_breakouts, mode))

        asyncio.create_task(tg.alert_monitor_summary(portfolio, day_pnl, len(positions), mode))

        return {
            "status":        "ok",
            "mode":          mode,
            "market_open":   market_open,
            "portfolio":     portfolio,
            "day_pnl":       day_pnl,
            "stage2_lost":   stage2_lost,
            "new_breakouts": new_breakouts,
            "results":       results,
        }

    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _get_watchlist(db: Session) -> list[str]:
    row = db.execute(text("SELECT value FROM settings WHERE key = 'watchlist'")).fetchone()
    if not row or not row[0]:
        return []
    return [s.strip().upper() for s in row[0].split(",") if s.strip()]


def _log_signal(db: Session, symbol: str, signal: str, score: int, price, mode: str):
    db.execute(
        text("INSERT INTO signal_log (symbol, signal, score, price, mode) VALUES (:s,:sig,:sc,:p,:m)"),
        {"s": symbol, "sig": signal, "sc": score, "p": price, "m": mode},
    )
    db.commit()


def _log_trade(db: Session, symbol: str, action: str, qty: float, price: float, trigger: str, mode: str):
    db.execute(
        text("INSERT INTO trade_log (symbol, action, qty, price, trigger, mode) VALUES (:s,:a,:q,:p,:t,:m)"),
        {"s": symbol, "a": action, "q": qty, "p": price, "t": trigger, "m": mode},
    )
    db.commit()