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


def _symbols_with_open_sell_orders(open_orders_by_symbol: dict) -> set[str]:
    """
    Return set of symbols that already have at least one open SELL order
    (stop or limit) — meaning exit strategy is already in place.
    """
    covered = set()
    for sym, orders in open_orders_by_symbol.items():
        for o in orders:
            side = str(o.side).lower()
            if "sell" in side:
                covered.add(sym)
                break
    return covered


def _ensure_exit_orders(
    db: Session,
    positions: list,
    open_orders_by_symbol: dict,
    mode: str,
):
    """
    For every live position that has NO open sell orders, look up the weekly
    plan and immediately place stop + target exit orders. This repairs positions
    whose bracket legs expired due to the DAY TIF bug.
    """
    covered = _symbols_with_open_sell_orders(open_orders_by_symbol)

    for pos in positions:
        sym = pos.symbol
        if sym in covered:
            continue  # exit orders already active

        qty = float(pos.qty)
        stop, target = _get_weekly_plan_exits(db, sym)

        if stop <= 0 or target <= 0:
            logger.warning(
                "Exit guard: %s has no open sell orders but weekly plan "
                "has no stop/target — skipping. Manual review needed.", sym
            )
            continue

        try:
            alp.place_oca_exit(sym, qty, stop, target, mode)
            logger.info(
                "Exit guard: placed stop=$%.2f + target=$%.2f for %s (qty=%.0f) "
                "— exit orders were missing.",
                stop, target, sym, qty,
            )
        except Exception as exc:
            logger.error("Exit guard: failed to place exit orders for %s: %s", sym, exc)


async def run_monitor(db: Session):
    mode         = get_setting(db, "trading_mode", "paper")
    auto_execute = get_setting(db, "auto_execute", "true").lower() == "true"
    risk_pct     = float(get_setting(db, "risk_pct", "2.0"))
    stop_pct     = float(get_setting(db, "stop_loss_pct", "8.0"))

    try:
        clock = alp.get_clock(mode)
        market_open = clock.is_open

        acct      = alp.get_account(mode)
        positions = alp.get_positions(mode)
        portfolio = float(acct.portfolio_value)
        day_pnl   = float(acct.equity) - float(acct.last_equity)

        # ── Exit guard ────────────────────────────────────────────────────────
        # Before evaluating signals, ensure every live position has active
        # stop + target orders. Repairs positions broken by the DAY TIF bug.
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

        # Check watchlist for new breakout entries (symbols not yet held)
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
                        # Pull stop/target from weekly plan for bracket order
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

        # Telegram alerts
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