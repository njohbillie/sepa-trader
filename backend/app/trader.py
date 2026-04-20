"""
Auto-execution engine: fires trades based on SEPA signals.
Position sizing: (portfolio * risk_pct/100) / (entry_price * stop_loss_pct/100)
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


def _get_weekly_plan_exits(db: Session, symbol: str) -> tuple[float, float]:
    """Return (stop_price, target1) from the current week's plan for a symbol. (0, 0) if missing."""
    row = db.execute(
        text("""
            SELECT stop_price, target1
            FROM weekly_plan
            WHERE symbol = :sym
              AND week_start = (SELECT MAX(week_start) FROM weekly_plan)
            LIMIT 1
        """),
        {"sym": symbol},
    ).fetchone()
    if not row:
        return 0.0, 0.0
    return float(row[0] or 0), float(row[1] or 0)


def _classify_exit_orders(open_orders_by_symbol: dict) -> tuple[set[str], set[str]]:
    """
    Inspect open orders per symbol and return two sets:
      - oco_covered:      symbols that have a proper OCO or bracket exit in place
      - orphan_order_ids: order IDs of standalone sell orders that need to be
                          cancelled before placing an OCO (they block the stop leg)

    An OCO/bracket order has order_class of 'oco', 'bracket', or 'oto'.
    A standalone sell (limit or stop without a paired leg) is an orphan.
    """
    oco_covered      = set()
    orphan_order_ids = {}   # symbol -> [order_id, ...]

    for sym, orders in open_orders_by_symbol.items():
        for o in orders:
            order_class = str(getattr(o, 'order_class', '') or '').lower()
            side        = str(getattr(o, 'side', '') or '').lower()

            is_sell = 'sell' in side
            is_oco  = any(kw in order_class for kw in ('oco', 'bracket', 'oto'))

            if is_sell and is_oco:
                oco_covered.add(sym)
                break   # fully covered — no need to inspect further

            if is_sell and not is_oco:
                # Orphaned standalone sell — needs to be cancelled
                orphan_order_ids.setdefault(sym, []).append(str(o.id))

    return oco_covered, orphan_order_ids


def _ensure_exit_orders(
    db: Session,
    positions: list,
    open_orders_by_symbol: dict,
    mode: str,
):
    """
    For every live position that does NOT have a proper OCO exit order:
      1. Cancel any orphaned standalone sell orders (they prevent the stop leg)
      2. Look up stop/target from the weekly plan
      3. Place a fresh OCO exit order

    This repairs positions broken by the old DAY TIF bug or the previous
    two-independent-orders approach.
    """
    oco_covered, orphan_order_ids = _classify_exit_orders(open_orders_by_symbol)
    client = alp.get_client(mode)

    for pos in positions:
        sym = pos.symbol

        if sym in oco_covered:
            continue  # OCO already in place — nothing to do

        # Cancel any orphaned standalone sell orders first
        orphans = orphan_order_ids.get(sym, [])
        for oid in orphans:
            try:
                client.cancel_order_by_id(oid)
                logger.info("Exit guard: cancelled orphaned sell order %s for %s", oid, sym)
            except Exception as exc:
                logger.warning("Exit guard: could not cancel order %s for %s: %s", oid, sym, exc)

        qty  = float(pos.qty)
        stop, target = _get_weekly_plan_exits(db, sym)

        if stop <= 0 or target <= 0:
            logger.warning(
                "Exit guard: %s has no OCO exit but weekly plan has no stop/target "
                "— skipping. Use 'Set Stop / Target' on the position card.", sym
            )
            continue

        try:
            alp.place_oca_exit(sym, qty, stop, target, mode)
            logger.info(
                "Exit guard: placed OCO exit for %s qty=%.0f stop=$%.2f target=$%.2f",
                sym, qty, stop, target,
            )
        except Exception as exc:
            logger.error("Exit guard: failed to place OCO exit for %s: %s", sym, exc)


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

        # ── Exit guard ────────────────────────────────────────────────────────
        # Runs every cycle while market is open. Cancels orphaned standalone
        # sell orders and replaces them with proper OCO exits. Also catches
        # positions with no exit orders at all.
        if market_open:
            try:
                open_orders_by_symbol = alp.get_open_orders_by_symbol(mode)
                _ensure_exit_orders(db, positions, open_orders_by_symbol, mode)
            except Exception as exc:
                logger.error("Exit guard failed: %s", exc)
        # ─────────────────────────────────────────────────────────────────────

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

        # Check watchlist for new breakout entries
        held_symbols = {p.symbol for p in positions}
        watchlist    = _get_watchlist(db)
        max_pos      = int(get_setting(db, "max_positions", "10"))

        if auto_execute and market_open and len(positions) < max_pos:
            for sym in watchlist:
                if sym in held_symbols:
                    continue
                result = analyze(sym)
                signal = result.get("signal")
                _log_signal(db, sym, signal, result.get("score", 0), result.get("price"), mode)

                if signal == "BREAKOUT" and result.get("price"):
                    price = result["price"]
                    qty   = _size_position(portfolio, price, risk_pct, stop_pct)
                    if qty >= 1:
                        stop, target = _get_weekly_plan_exits(db, sym)
                        try:
                            if stop > 0 and target > 0:
                                alp.place_bracket_buy(sym, qty, stop, target, mode)
                            else:
                                alp.place_market_buy(sym, qty, mode)
                                logger.warning(
                                    "Watchlist buy %s: no stop/target in weekly plan — "
                                    "plain market buy, no exit orders attached.", sym
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