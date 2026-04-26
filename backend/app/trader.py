"""
Auto-execution engine: fires trades based on SEPA signals.
- Every 30-minute cycle: trailing stop adjustment + signal evaluation
- Pre-trade AI gate runs before every buy order
- Exit guard ensures every position has an OCO at all times,
  and replaces existing OCOs when the plan's stop/target has changed
- Live accounts under $10K use small-account position limits automatically
"""
import asyncio
import logging
from datetime import datetime
import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session
from . import alpaca_client as alp
from .sepa_analyzer import analyze
from .database import get_setting, set_setting, get_all_user_settings
from . import telegram_alerts as tg

_ET = pytz.timezone("America/New_York")

logger = logging.getLogger(__name__)


def _size_position(
    portfolio_value: float,
    price: float,
    risk_pct: float,
    stop_pct: float,
    stop_price: float = 0.0,
) -> float:
    risk_dollars  = portfolio_value * (risk_pct / 100)
    stop_distance = (price - stop_price) if stop_price > 0 and price > stop_price else price * (stop_pct / 100)
    if stop_distance <= 0:
        return 0
    return risk_dollars / stop_distance


def _effective_max_positions(db: Session, mode: str) -> int:
    """
    For live accounts under $10K, cap max_positions at 3 regardless of settings.
    Paper accounts always use the settings value.
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
                    "Live account %s: max_positions capped at %d (settings=%d)",
                    limits.get("tier", ""), effective, configured,
                )
            return effective
    except Exception as exc:
        logger.warning("_effective_max_positions: could not fetch account — using settings: %s", exc)
    return configured


def _get_weekly_plan_exits(db: Session, symbol: str, mode: str) -> tuple[float, float, float]:
    """Return (stop_price, target1, target2) — most recent plan row for this symbol+mode."""
    row = db.execute(
        text("""
            SELECT stop_price, target1, target2
            FROM weekly_plan
            WHERE symbol = :sym
              AND mode = :mode
            ORDER BY week_start DESC
            LIMIT 1
        """),
        {"sym": symbol, "mode": mode},
    ).fetchone()
    if not row:
        return 0.0, 0.0, 0.0
    return float(row[0] or 0), float(row[1] or 0), float(row[2] or 0)


def _get_current_stop_price(orders: list) -> float | None:
    """Extract the active stop price from an open OCO/bracket order's stop leg."""
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


def _get_current_target_price(orders: list) -> float | None:
    """Extract the active target (limit) price from an open OCO/bracket order."""
    for o in orders:
        order_class = str(getattr(o, 'order_class', '') or '').lower()
        side        = str(getattr(o, 'side',        '') or '').lower()
        if 'sell' not in side:
            continue
        if any(kw in order_class for kw in ('oco', 'bracket', 'oto')):
            # Parent limit_price is the take-profit leg
            lp = getattr(o, 'limit_price', None)
            if lp is not None:
                return float(lp)
            # Check child legs
            legs = getattr(o, 'legs', None) or []
            for leg in legs:
                order_type = str(getattr(leg, 'type', '') or '').lower()
                if 'limit' in order_type:
                    lp = getattr(leg, 'limit_price', None)
                    if lp is not None:
                        return float(lp)
    return None


# ── Trailing stop tiers ───────────────────────────────────────────────────────
# Only applied when position is green — never touches red positions.
# Tier 1 — gain >= 1R: move stop to breakeven (entry)
# Tier 2 — gain >= 2R: move stop to entry + 1R (lock in 1R profit)
# Tier 3 — gain >= 3R: trail stop at current_price - 1R (dynamic trailing)
# Stop only ever moves UP — never down.

_TRAIL_TIERS = [
    (3.0, lambda entry, R, price: price - R),
    (2.0, lambda entry, R, price: entry + R),
    (1.0, lambda entry, R, price: entry),
]

_MIN_STOP_IMPROVEMENT_PCT = 0.005


def _compute_new_stop(entry: float, original_stop: float, current_price: float) -> float | None:
    R = entry - original_stop
    if R <= 0:
        return None
    gain_r = (current_price - entry) / R
    for threshold, fn in _TRAIL_TIERS:
        if gain_r >= threshold:
            new_stop = round(fn(entry, R, current_price), 2)
            max_stop = round(current_price * 0.995, 2)
            return min(new_stop, max_stop)
    return None


def _adjust_trailing_stops(
    db: Session,
    positions: list,
    open_orders_by_symbol: dict,
    mode: str,
):
    """
    Ratchet stops upward for green positions. Red positions untouched.
    Updates weekly_plan.stop_price so exit guard stays in sync.
    """
    for pos in positions:
        sym           = pos.symbol
        current_price = float(pos.current_price)
        entry         = float(pos.avg_entry_price)
        qty           = float(pos.qty)

        if current_price <= entry:
            continue  # red or flat — never touch

        stop_orig, target, target2 = _get_weekly_plan_exits(db, sym, mode)
        if stop_orig <= 0 or target <= 0:
            logger.debug("Trailing stop: %s has no plan exits — skipping.", sym)
            continue

        new_stop = _compute_new_stop(entry, stop_orig, current_price)
        if new_stop is None:
            continue  # gain < 1R

        current_stop      = _get_current_stop_price(open_orders_by_symbol.get(sym, []))
        effective_current = current_stop if current_stop else stop_orig

        if new_stop <= effective_current * (1 + _MIN_STOP_IMPROVEMENT_PCT):
            logger.debug(
                "Trailing stop: %s new=$%.2f vs current=$%.2f — no update needed.",
                sym, new_stop, effective_current,
            )
            continue

        R      = entry - stop_orig
        gain_r = (current_price - entry) / R

        # Count open OCO/bracket sell orders to detect split-lot state
        sym_orders = open_orders_by_symbol.get(sym, [])
        oco_count  = sum(
            1 for o in sym_orders
            if 'sell' in str(getattr(o, 'side', '') or '').lower()
            and any(kw in str(getattr(o, 'order_class', '') or '').lower()
                    for kw in ('oco', 'bracket', 'oto'))
        )

        logger.info(
            "Trailing stop: %s  gain=%.1fR  price=$%.2f  old=$%.2f → new=$%.2f  "
            "target1=$%.2f target2=$%.2f oco_count=%d [%s]",
            sym, gain_r, current_price, effective_current, new_stop,
            target, target2, oco_count, mode,
        )

        try:
            if target2 > 0 and oco_count >= 2:
                # Both T1 and T2 lots still open — update stop on both
                qty1 = int(qty) // 2
                qty2 = int(qty) - qty1
                alp.replace_split_oca_exits(sym, qty1, qty2, new_stop, target, target2, mode)
            elif target2 > 0 and oco_count == 1:
                # T1 was already hit — only T2 lot remains; pos.qty is the current qty
                alp.replace_oca_exit(sym, qty, new_stop, target2, mode)
            else:
                # Single-lot position
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
                "Trailing stop updated: %s $%.2f → $%.2f (gain=%.1fR) [%s]",
                sym, effective_current, new_stop, gain_r, mode,
            )
        except Exception as exc:
            logger.error("Trailing stop update failed for %s: %s", sym, exc)
            # The replace cancelled the old OCO before placing the new one,
            # so a placement failure leaves the position naked until the next
            # monitor cycle. Alert immediately.
            try:
                from . import telegram_alerts as tg
                tg.alert_system_error_sync(
                    f"NAKED POSITION [{mode}] {sym} — trailing-stop replace failed",
                    exc,
                )
            except Exception:
                pass


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


# Tolerance for considering stop/target "changed" — avoids churn on tiny rounding diffs
_PRICE_CHANGE_THRESHOLD = 0.02   # $0.02


def _ensure_exit_orders(
    db: Session,
    positions: list,
    open_orders_by_symbol: dict,
    mode: str,
):
    """
    For every live position:
      - If no OCO exists: cancel orphaned orders and place a fresh OCO
      - If an OCO exists but stop/target differ from the plan: replace it
      - If an OCO exists and prices match the plan: leave it alone

    This ensures that edits made via 'Set Stop / Target' (Auto mode) take
    effect on the next monitor cycle without requiring manual intervention.
    """
    oco_covered, orphan_order_ids = _classify_exit_orders(open_orders_by_symbol)
    client = alp.get_client(mode)

    # Cancel exit orders for symbols that have no live position (e.g., user
    # closed manually, or a previous close left dangling legs). Otherwise the
    # orphan OCO can fire against a fresh re-entry, prematurely closing the
    # new position.
    held_symbols = {p.symbol for p in positions}
    for sym in list(open_orders_by_symbol.keys()):
        if sym in held_symbols:
            continue
        for o in open_orders_by_symbol.get(sym, []):
            side = str(getattr(o, "side", "") or "").lower()
            if "sell" not in side:
                continue
            try:
                client.cancel_order_by_id(str(o.id))
                logger.info(
                    "Exit guard: cancelled orphan exit %s for %s (no live position) [%s]",
                    o.id, sym, mode,
                )
            except Exception as exc:
                logger.warning(
                    "Exit guard: could not cancel orphan %s for %s: %s",
                    o.id, sym, exc,
                )

    for pos in positions:
        sym   = pos.symbol
        qty   = float(pos.qty)
        stop, target, target2 = _get_weekly_plan_exits(db, sym, mode)

        # Count open OCO/bracket sell orders — needed to distinguish split-lot state
        sym_orders = open_orders_by_symbol.get(sym, [])
        oco_count  = sum(
            1 for o in sym_orders
            if 'sell' in str(getattr(o, 'side', '') or '').lower()
            and any(kw in str(getattr(o, 'order_class', '') or '').lower()
                    for kw in ('oco', 'bracket', 'oto'))
        )

        if sym in oco_covered:
            # At least one OCO exists
            if stop <= 0 or target <= 0:
                continue

            current_stop   = _get_current_stop_price(sym_orders)
            current_target = _get_current_target_price(sym_orders)
            stop_changed   = current_stop is not None and abs(current_stop - stop) > _PRICE_CHANGE_THRESHOLD

            if target2 > 0:
                if oco_count >= 2:
                    # Both lots open — only validate the stop (each lot has a different target)
                    if stop_changed:
                        qty1 = int(qty) // 2
                        qty2 = int(qty) - qty1
                        try:
                            alp.replace_split_oca_exits(sym, qty1, qty2, stop, target, target2, mode)
                            logger.info(
                                "Exit guard: replaced split OCOs for %s stop=$%.2f T1=$%.2f T2=$%.2f [%s]",
                                sym, stop, target, target2, mode,
                            )
                        except Exception as exc:
                            logger.error("Exit guard: split OCO replacement failed for %s: %s", sym, exc)
                    else:
                        logger.debug("Exit guard: %s split OCOs in place — no action.", sym)
                else:
                    # T1 was hit — only T2 lot remains; validate against target2
                    target_changed = (
                        current_target is not None
                        and abs(current_target - target2) > _PRICE_CHANGE_THRESHOLD
                    )
                    if stop_changed or target_changed:
                        try:
                            alp.replace_oca_exit(sym, qty, stop, target2, mode)
                            logger.info(
                                "Exit guard: replaced T2 OCO for %s stop=$%.2f T2=$%.2f [%s]",
                                sym, stop, target2, mode,
                            )
                        except Exception as exc:
                            logger.error("Exit guard: T2 OCO replacement failed for %s: %s", sym, exc)
                            try:
                                from . import telegram_alerts as tg
                                tg.alert_system_error_sync(
                                    f"NAKED POSITION [{mode}] {sym} — T2 OCO replace failed",
                                    exc,
                                )
                            except Exception:
                                pass
                    else:
                        logger.debug("Exit guard: %s T2 OCO in place — no action.", sym)
            else:
                # Single-lot — original logic
                target_changed = (
                    current_target is not None
                    and abs(current_target - target) > _PRICE_CHANGE_THRESHOLD
                )
                if stop_changed or target_changed:
                    logger.info(
                        "Exit guard: %s plan changed (stop $%.2f→$%.2f, target $%.2f→$%.2f) — replacing OCO [%s]",
                        sym, current_stop or 0, stop, current_target or 0, target, mode,
                    )
                    try:
                        alp.replace_oca_exit(sym, qty, stop, target, mode)
                        logger.info("Exit guard: replaced OCO for %s stop=$%.2f target=$%.2f [%s]", sym, stop, target, mode)
                    except Exception as exc:
                        logger.error("Exit guard: OCO replacement failed for %s: %s", sym, exc)
                        try:
                            from . import telegram_alerts as tg
                            tg.alert_system_error_sync(
                                f"NAKED POSITION [{mode}] {sym} — OCO replace failed",
                                exc,
                            )
                        except Exception:
                            pass
                else:
                    logger.debug("Exit guard: %s OCO in place and matches plan — no action.", sym)
            continue

        # No OCO at all — cancel any orphaned standalone sells, then place fresh exit(s)
        for oid in orphan_order_ids.get(sym, []):
            try:
                client.cancel_order_by_id(oid)
                logger.info("Exit guard: cancelled orphaned sell %s for %s [%s]", oid, sym, mode)
            except Exception as exc:
                logger.warning("Exit guard: could not cancel %s for %s: %s", oid, sym, exc)

        if stop <= 0 or target <= 0:
            logger.warning(
                "Exit guard: %s has no OCO but no stop/target in plan [%s] "
                "— use 'Set Stop / Target' on the position card.", sym, mode,
            )
            continue

        if target2 > 0 and int(qty) >= 2:
            # Split-lot: place two OCOs (T1 for half, T2 for the other half)
            qty1 = int(qty) // 2
            qty2 = int(qty) - qty1
            try:
                alp.place_oca_exit(sym, qty1, stop, target,  mode)
                alp.place_oca_exit(sym, qty2, stop, target2, mode)
                logger.info(
                    "Exit guard: placed split OCOs for %s qty=%d+%d stop=$%.2f T1=$%.2f T2=$%.2f [%s]",
                    sym, qty1, qty2, stop, target, target2, mode,
                )
            except Exception as exc:
                logger.error("Exit guard: split OCO placement failed for %s: %s", sym, exc)
                try:
                    from . import telegram_alerts as tg
                    tg.alert_system_error_sync(
                        f"NAKED POSITION [{mode}] {sym} — split OCO placement failed",
                        exc,
                    )
                except Exception:
                    pass
        else:
            try:
                alp.place_oca_exit(sym, qty, stop, target, mode)
                logger.info(
                    "Exit guard: placed OCO for %s qty=%.0f stop=$%.2f target=$%.2f [%s]",
                    sym, qty, stop, target, mode,
                )
            except Exception as exc:
                logger.error("Exit guard: failed to place OCO for %s: %s", sym, exc)
                try:
                    from . import telegram_alerts as tg
                    tg.alert_system_error_sync(
                        f"NAKED POSITION [{mode}] {sym} — OCO placement failed",
                        exc,
                    )
                except Exception:
                    pass


# ── Tape context helper ───────────────────────────────────────────────────────

def _get_tape_context(db: Session, user_id: int | None) -> dict | None:
    """
    Return today's cached tape verdict (no LLM call).
    Returns None if no cache row exists yet for today.
    """
    import json as _json
    from datetime import date
    from sqlalchemy import text as _text

    if user_id is None:
        return None
    try:
        today = date.today().isoformat()
        row = db.execute(
            _text("""
                SELECT signals, verdict, summary, key_risk
                FROM market_tape_cache
                WHERE user_id = :uid AND cache_date = :d
            """),
            {"uid": user_id, "d": today},
        ).fetchone()
        if not row:
            return None
        signals = _json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
        return {
            "condition": row[1],
            "summary":   row[2],
            "key_risk":  row[3],
            "signals":   signals,
        }
    except Exception as exc:
        logger.debug("_get_tape_context failed (non-fatal): %s", exc)
        return None


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
    user_id: int = None,
) -> bool:
    """Pre-trade AI gate. Returns True if order should proceed. Fails closed."""
    try:
        from .claude_analyst import pre_trade_analysis, log_pre_trade, get_stored_weekly_plan_analysis
        stored       = get_stored_weekly_plan_analysis(db, symbol, mode)
        acct         = alp.get_account(mode)
        portfolio    = float(acct.portfolio_value)
        cash         = float(acct.cash)
        buying_power = float(acct.buying_power)

        # Fetch today's tape from cache (no extra LLM call; cache may be empty)
        tape_context = _get_tape_context(db, user_id)

        result = pre_trade_analysis(
            db=db, symbol=symbol, side="BUY", qty=qty,
            entry_price=entry, stop_price=stop, target_price=target,
            trigger=trigger, portfolio_value=portfolio,
            cash=cash, buying_power=buying_power, mode=mode,
            user_id=user_id, tape_context=tape_context,
            stored_analysis=stored,
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
        logger.error("Pre-trade gate error for %s: %s — BLOCKING (fail-closed).", symbol, exc)
        try:
            from . import telegram_alerts as tg
            tg.alert_system_error_sync(f"Pre-trade gate {symbol} [{mode}]", exc, level="URGENT")
        except Exception:
            pass
        return False


# ── Partial-fill reconciliation ───────────────────────────────────────────────

def _reconcile_partial_fills(db: Session, positions, mode: str) -> None:
    """Sync stale planned qty against actual filled qty.

    `weekly_plan.position_size` is set at screener time and `trade_log.qty`
    captures the requested size. Neither is updated when a bracket entry
    partial-fills, so reporting drifts from reality. Bracket exit legs
    auto-resize to filled qty in Alpaca, so this is a reporting-fix, not
    a risk-fix — but stale numbers also leak into the AI gate's portfolio
    context and the cash-buffer calculation on the next monitor cycle.
    """
    from sqlalchemy import text as _text
    from . import telegram_alerts as tg

    for pos in positions:
        sym       = pos.symbol
        actual_qty = float(getattr(pos, "qty", 0) or 0)
        if actual_qty <= 0:
            continue

        try:
            # Latest weekly_plan row for this symbol+mode
            wp = db.execute(
                _text("""
                    SELECT id, position_size, entry_price
                    FROM weekly_plan
                    WHERE symbol = :sym AND mode = :mode
                      AND week_start = (
                          SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
                      )
                """),
                {"sym": sym, "mode": mode},
            ).fetchone()
            if not wp or wp[1] is None:
                continue

            planned = int(wp[1])
            if planned <= 0 or planned == int(actual_qty):
                continue

            # Decide whether this is a "fresh" partial fill worth alerting on.
            # If the most-recent BUY trade_log entry is older than 2 hours, the
            # drift is historical state we're cleaning up — sync silently.
            tl = db.execute(
                _text("""
                    SELECT id, EXTRACT(EPOCH FROM (NOW() - created_at)) AS age_sec
                    FROM trade_log
                    WHERE symbol = :sym AND mode = :mode AND action = 'BUY'
                    ORDER BY created_at DESC
                    LIMIT 1
                """),
                {"sym": sym, "mode": mode},
            ).fetchone()
            is_fresh = bool(tl and tl[1] is not None and float(tl[1]) < 7200)

            db.execute(
                _text("UPDATE weekly_plan SET position_size = :q WHERE id = :id"),
                {"q": int(actual_qty), "id": wp[0]},
            )
            if tl:
                db.execute(
                    _text("UPDATE trade_log SET qty = :q WHERE id = :id"),
                    {"q": actual_qty, "id": tl[0]},
                )
            db.commit()

            diff_pct = abs(actual_qty - planned) / planned * 100
            logger.info(
                "Reconciled %s [%s]: planned=%d actual=%.0f (%.1f%% diff)",
                sym, mode, planned, actual_qty, diff_pct,
            )
            # Alert on material partial fill — but only when fresh.
            # Older drift is historical state being cleaned up (e.g. first
            # monitor cycle after this reconciler shipped) — sync silently.
            if diff_pct >= 10 and is_fresh:
                try:
                    tg.alert_system_error_sync(
                        f"Partial fill reconciled [{mode}] {sym}",
                        f"planned={planned}, filled={int(actual_qty)} ({diff_pct:.0f}% short)",
                        level="INFO",
                    )
                except Exception:
                    pass

        except Exception as exc:
            db.rollback()
            logger.warning("Partial-fill reconcile failed for %s: %s", sym, exc)


# ── Main monitor ──────────────────────────────────────────────────────────────

async def run_monitor(db: Session, user_id: int | None = None, mode: str | None = None):
    """
    mode — if passed explicitly (by scheduler for parallel runs) the monitor
    operates in that exact mode and does not read trading_mode from settings.
    If None, falls back to reading trading_mode from user/global settings.
    """
    if user_id:
        from .database import get_all_user_settings as _gaus
        _s = _gaus(db, user_id)
        if mode is None:
            mode = _s.get("trading_mode", "paper")
        # Per-mode auto_execute flags — live mode is FAIL-SAFE (defaults off):
        #   paper_auto_execute: default "true"
        #   live_auto_execute:  default "false" — must be EXPLICITLY set to "true"
        # Live mode never inherits a "true" default even if the key is absent.
        if mode == "live":
            auto_execute = _s.get("live_auto_execute", "false").lower() == "true"
        else:
            auto_execute = _s.get("paper_auto_execute", _s.get("auto_execute", "true")).lower() == "true"
        risk_pct         = float(_s.get("risk_pct", "2.0") or "2.0")
        stop_pct         = float(_s.get("stop_loss_pct", "8.0") or "8.0")
        interval_minutes = int(_s.get("monitor_interval_minutes", "30") or "30")
        try:
            alp.configure_from_db_settings(_s, mode, is_admin=True)
        except ValueError as _creds_err:
            logger.warning("run_monitor [%s]: credential error — %s", mode, _creds_err)
            return {"status": "error", "error": str(_creds_err)}
    else:
        if mode is None:
            mode = get_setting(db, "trading_mode", "paper")
        if mode == "live":
            auto_execute = get_setting(db, "live_auto_execute",  "false").lower() == "true"
        else:
            auto_execute = get_setting(db, "paper_auto_execute", get_setting(db, "auto_execute", "true")).lower() == "true"
        risk_pct         = float(get_setting(db, "risk_pct", "2.0"))
        stop_pct         = float(get_setting(db, "stop_loss_pct", "8.0"))
        interval_minutes = int(get_setting(db, "monitor_interval_minutes", "30") or "30")

    if mode == "live" and auto_execute:
        logger.warning(
            "LIVE AUTO-EXECUTE IS ENABLED — monitor will place real-money orders [user=%s]",
            user_id,
        )

    try:
        clock       = alp.get_clock(mode)
        market_open = clock.is_open

        acct         = alp.get_account(mode)
        positions    = alp.get_positions(mode)
        portfolio    = float(acct.portfolio_value)
        cash         = float(acct.cash)
        buying_power = float(acct.buying_power)
        day_pnl      = float(acct.equity) - float(acct.last_equity)

        if market_open and positions:
            # Step 0: Reconcile partial fills before any stop/target sizing.
            # Exit orders are sized off pos.qty (already correct), but
            # weekly_plan.position_size and the BUY trade_log entry are
            # frozen at submit-time, so partial fills leave them stale.
            _reconcile_partial_fills(db, positions, mode)

            try:
                open_orders_by_symbol = alp.get_open_orders_by_symbol(mode)

                # Step 1: Trailing stop adjustment
                # Green positions get stops ratcheted up.
                # Red positions are untouched.
                _adjust_trailing_stops(db, positions, open_orders_by_symbol, mode)

                # Re-fetch after potential cancel+replace from trailing stops
                open_orders_by_symbol = alp.get_open_orders_by_symbol(mode)

                # Step 2: Exit guard
                # Ensures every position has an active OCO.
                # Replaces existing OCOs when plan stop/target has changed.
                _ensure_exit_orders(db, positions, open_orders_by_symbol, mode)

            except Exception as exc:
                logger.error("Stop management cycle failed: %s", exc)
                try:
                    from . import telegram_alerts as tg
                    tg.alert_system_error_sync(f"Monitor stop-mgmt cycle [{mode}]", exc)
                except Exception:
                    pass

        # Step 3: Signal evaluation
        stage2_lost   = []
        new_breakouts = []
        results       = []

        for pos in positions:
            sym    = pos.symbol
            qty    = float(pos.qty)
            result = analyze(sym, db=db)
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

        # Daily-drawdown kill-switch — block all NEW entries (slot fill +
        # watchlist) when day_pnl breaches the configured threshold. Existing
        # exits/trailing stops still run; we only stop adding risk.
        # Setting `daily_drawdown_halt_pct` (default 5.0); 0 disables the check.
        # Halt latches for the ET calendar day in `drawdown_halt_<mode>_date`
        # so a mid-day restart doesn't forget that we already tripped.
        entries_halted = False
        try:
            halt_pct = float(get_setting(db, "daily_drawdown_halt_pct", "5.0") or "0")
        except (TypeError, ValueError):
            halt_pct = 5.0

        today_et   = datetime.now(_ET).strftime("%Y-%m-%d")
        halt_key   = f"drawdown_halt_{mode}_date"
        prior_halt = get_setting(db, halt_key, "")

        if halt_pct > 0 and prior_halt == today_et:
            entries_halted = True
            logger.warning(
                "DAILY DRAWDOWN HALT [%s]: previously tripped today (%s) — entries still blocked.",
                mode, today_et,
            )

        last_eq = float(getattr(acct, "last_equity", 0) or 0)
        if not entries_halted and halt_pct > 0 and last_eq > 0:
            day_pnl_pct = day_pnl / last_eq * 100
            if day_pnl_pct <= -halt_pct:
                entries_halted = True
                try:
                    set_setting(db, halt_key, today_et)
                    db.commit()
                except Exception:
                    db.rollback()
                logger.warning(
                    "DAILY DRAWDOWN HALT [%s]: day_pnl=%.2f%% breached -%.2f%% threshold — "
                    "blocking new entries (exits still run).",
                    mode, day_pnl_pct, halt_pct,
                )
                try:
                    tg.send_sync(
                        f"*DAILY DRAWDOWN HALT* [{mode.upper()}]\n\n"
                        f"Day P&L: `{day_pnl_pct:.2f}%` (threshold `-{halt_pct:.2f}%`)\n"
                        f"New entries blocked for the rest of the session. Exits still run.",
                        level="URGENT",
                    )
                except Exception:
                    pass

        # Step 4: Weekly-plan slot fill — buys PENDING picks when capacity exists.
        # This is the primary entry mechanism (screener picks → weekly_plan → orders).
        if auto_execute and market_open and not entries_halted:
            try:
                from .position_manager import fill_open_slots
                fill_open_slots(
                    db=db, mode=mode, portfolio=portfolio,
                    cash=cash, buying_power=buying_power,
                    risk_pct=risk_pct, stop_pct=stop_pct,
                    positions=positions, user_id=user_id,
                )
                # Re-fetch positions so watchlist step has fresh state
                positions = alp.get_positions(mode)
            except Exception as exc:
                logger.error("fill_open_slots failed: %s", exc)

        # Step 5: Watchlist breakout entries (manual watchlist, not screener picks)
        held_symbols = {p.symbol for p in positions}
        watchlist    = _get_watchlist(db, user_id)
        max_pos      = _effective_max_positions(db, mode)

        if auto_execute and market_open and not entries_halted and len(positions) < max_pos:
            for sym in watchlist:
                if sym in held_symbols:
                    continue
                result = analyze(sym, db=db)
                signal = result.get("signal")
                _log_signal(db, sym, signal, result.get("score", 0), result.get("price"), mode)

                if signal == "BREAKOUT" and result.get("price"):
                    price               = result["price"]
                    stop, target, target2 = _get_weekly_plan_exits(db, sym, mode)
                    qty                 = _size_position(portfolio, price, risk_pct, stop_pct, stop_price=stop)

                    if qty >= 1:
                        if not _gate(db, sym, qty, price, stop, target, "BREAKOUT", mode, user_id=user_id):
                            results.append({"sym": sym, "action": "BLOCKED_BY_AI"})
                            continue
                        order_placed = False
                        try:
                            from .position_manager import _place_entry as _pm_place_entry
                            order_desc = _pm_place_entry(db, sym, qty, price, stop, target, "BREAKOUT", mode, "minervini", target2=target2)
                            order_placed = True
                            logger.info("Watchlist buy %s qty=%.0f — %s [%s]", sym, qty, order_desc, mode)
                            _log_trade(db, sym, "BUY", qty, price, "BREAKOUT", mode)
                            new_breakouts.append(sym)
                            held_symbols.add(sym)
                        except Exception as e:
                            if order_placed:
                                logger.error(
                                    "Watchlist buy %s [%s]: ORDER PLACED but trade_log failed: %s",
                                    sym, mode, e,
                                )
                                try:
                                    tg.alert_system_error_sync(
                                        f"UNTRACKED POSITION [{mode}] {sym} qty={qty} — watchlist order placed, log failed",
                                        e, level="URGENT",
                                    )
                                except Exception:
                                    pass
                            results.append({"sym": sym, "action": "BUY_FAILED", "error": str(e)})

        if stage2_lost:
            asyncio.create_task(tg.alert_stage2_lost(stage2_lost, mode))
        if new_breakouts:
            asyncio.create_task(tg.alert_breakout(new_breakouts, mode))

        asyncio.create_task(tg.alert_monitor_summary(portfolio, day_pnl, len(positions), mode, interval_minutes))

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
        logger.exception("Monitor [%s] top-level failure", mode)
        try:
            from . import telegram_alerts as tg
            tg.alert_system_error_sync(f"Monitor crashed [{mode}]", exc)
        except Exception:
            pass
        return {"status": "error", "error": str(exc)}


def _get_watchlist(db: Session, user_id: int | None = None) -> list[str]:
    """Return the manual watchlist, preferring user_settings over global settings."""
    raw = None
    if user_id:
        from .database import get_all_user_settings as _gaus
        raw = _gaus(db, user_id).get("watchlist", "")
    if not raw:
        row = db.execute(text("SELECT value FROM settings WHERE key = 'watchlist'")).fetchone()
        raw = row[0] if row else ""
    if not raw:
        return []
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


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