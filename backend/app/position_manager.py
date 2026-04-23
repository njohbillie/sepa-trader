"""
Position manager: Monday open fills + midweek slot refill with AI analysis.
Pre-trade gate runs before every buy. Live <$10K accounts use conservative limits.
"""
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from .database import get_setting, set_setting
from . import alpaca_client as alp

logger = logging.getLogger(__name__)


def _size_qty(portfolio: float, entry: float, stop: float, risk_pct: float, stop_pct: float) -> float:
    stop_dollar = (entry - stop) if stop > 0 else entry * (stop_pct / 100)
    if stop_dollar <= 0:
        return 0
    return (portfolio * risk_pct / 100) / stop_dollar


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


def _infer_close_reason(db: Session, symbol: str, mode: str) -> tuple[str, float | None, float | None]:
    """
    Infer why a position closed by comparing last known close price
    against the weekly plan's stop and target.
    Returns (reason, entry_price, close_price).
    """
    try:
        plan_row = db.execute(
            text("""
                SELECT entry_price, stop_price, target1
                FROM weekly_plan
                WHERE symbol = :sym AND mode = :mode
                ORDER BY week_start DESC LIMIT 1
            """),
            {"sym": symbol, "mode": mode},
        ).fetchone()

        trade_row = db.execute(
            text("""
                SELECT price FROM trade_log
                WHERE symbol = :s AND action = 'BUY' AND mode = :mode
                ORDER BY created_at DESC LIMIT 1
            """),
            {"s": symbol, "mode": mode},
        ).fetchone()

        sell_row = db.execute(
            text("""
                SELECT price FROM trade_log
                WHERE symbol = :s AND action = 'SELL' AND mode = :mode
                ORDER BY created_at DESC LIMIT 1
            """),
            {"s": symbol, "mode": mode},
        ).fetchone()

        entry_price = float(trade_row[0]) if trade_row else None
        close_price = float(sell_row[0])  if sell_row  else None

        if plan_row and close_price:
            stop   = float(plan_row[1] or 0)
            target = float(plan_row[2] or 0)
            if stop   > 0 and close_price <= stop   * 1.01:
                return "stop_hit",   entry_price, close_price
            if target > 0 and close_price >= target * 0.99:
                return "target_hit", entry_price, close_price

        return "manual", entry_price, close_price

    except Exception:
        return "manual", None, None


def _place_entry(
    db: Session,
    sym: str,
    qty: float,
    entry: float,
    stop: float,
    target: float,
    trigger: str,
    mode: str,
    screener_type: str = "minervini",
) -> str:
    """
    Submit the entry buy using the user-configured order type.
    Returns a short description string for logging.

    Settings are split by screener source:
      mv_entry_order_type / mv_entry_slippage_pct  — Minervini/breakout picks
      pb_entry_order_type / pb_entry_slippage_pct  — Pullback-to-MA picks

    order_type values:
      market     — market order + bracket exits (original behaviour)
      limit      — DAY limit at entry×(1+slippage%), bracket exits attached
      stop_limit — DAY stop-limit at entry price, limit at entry×(1+slippage%);
                   Alpaca does not support brackets here so exits are added by
                   the monitor on the next cycle after fill
    """
    if screener_type == "pullback":
        order_type   = get_setting(db, "pb_entry_order_type",   "limit")
        slippage_pct = float(get_setting(db, "pb_entry_slippage_pct", "0.5"))
    else:  # minervini (default)
        order_type   = get_setting(db, "mv_entry_order_type",   "stop_limit")
        slippage_pct = float(get_setting(db, "mv_entry_slippage_pct", "1.0"))
    has_exits    = stop > 0 and target > 0

    if order_type == "limit":
        limit_px = round(entry * (1 + slippage_pct / 100), 2)
        if has_exits:
            try:
                alp.place_limit_bracket_buy(sym, qty, entry, stop, target, slippage_pct, mode)
                return f"limit bracket (lim=${limit_px} stop=${stop:.2f} tgt=${target:.2f})"
            except Exception as exc:
                logger.error("_place_entry: limit bracket FAILED for %s: %s — falling back to market bracket", sym, exc)
                alp.place_bracket_buy(sym, qty, stop, target, mode)
                return f"market bracket [limit fallback] (stop=${stop:.2f} tgt=${target:.2f})"
        else:
            alp.place_limit_buy(sym, qty, limit_px, mode)
            return f"limit buy (lim=${limit_px})"

    elif order_type == "stop_limit":
        limit_px = round(entry * (1 + slippage_pct / 100), 2)
        alp.place_stop_limit_buy(sym, qty, entry, slippage_pct, mode)
        return f"stop-limit (stop=${entry:.2f} lim=${limit_px}) — exits pending fill"

    else:  # "market" or unrecognised — original behaviour
        if has_exits:
            try:
                alp.place_bracket_buy(sym, qty, stop, target, mode)
                return f"market bracket (stop=${stop:.2f} tgt=${target:.2f})"
            except Exception as exc:
                logger.error("_place_entry: market bracket FAILED for %s: %s — plain market buy", sym, exc)
                alp.place_market_buy(sym, qty, mode)
                return f"market buy [bracket failed]"
        else:
            alp.place_market_buy(sym, qty, mode)
            return f"market buy"


def run_monday_open(db: Session):
    """
    Called every Monday at 9:35 ET. Fills available position slots from the
    current week's PENDING picks for the active mode. Respects max_positions.
    Pre-trade AI gate runs before every buy.
    """
    mode      = get_setting(db, "trading_mode", "paper")
    auto_exec = get_setting(db, "auto_execute", "true").lower() == "true"
    if not auto_exec:
        logger.info("Monday open: auto_execute off — skipping.")
        return

    max_pos = _effective_max_positions(db, mode)

    try:
        positions = alp.get_positions(mode)
    except Exception as exc:
        logger.error("Monday open: could not fetch positions: %s", exc)
        return

    slots = max_pos - len(positions)
    if slots <= 0:
        logger.info("Monday open: portfolio full (%d/%d). No buys.", len(positions), max_pos)
        return

    rows = db.execute(
        text("""
            SELECT symbol, entry_price, stop_price, target1,
                   COALESCE(screener_type, 'minervini') AS screener_type
            FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
            )
              AND mode = :mode
              AND status = 'PENDING'
            ORDER BY rank ASC
            LIMIT :slots
        """),
        {"slots": slots, "mode": mode},
    ).fetchall()

    if not rows:
        logger.info("Monday open: no PENDING picks for mode=%s.", mode)
        return

    try:
        acct      = alp.get_account(mode)
        portfolio = float(acct.portfolio_value)
    except Exception as exc:
        logger.error("Monday open: could not fetch account: %s", exc)
        return

    risk_pct = float(get_setting(db, "risk_pct", "2.0"))
    stop_pct = float(get_setting(db, "stop_loss_pct", "8.0"))
    held     = {p.symbol for p in positions}

    for row in rows:
        sym    = row[0]
        entry  = float(row[1] or 0)
        stop   = float(row[2] or 0)
        target = float(row[3] or 0)
        stype  = row[4]

        if sym in held or entry <= 0:
            continue

        qty = _size_qty(portfolio, entry, stop, risk_pct, stop_pct)
        if qty < 1:
            logger.info("Monday open: skipping %s — position size < 1 share.", sym)
            continue

        if not _gate(db, sym, qty, entry, stop, target, "MONDAY_OPEN", mode):
            continue

        try:
            order_desc   = _place_entry(db, sym, qty, entry, stop, target, "MONDAY_OPEN", mode, stype)
            order_placed = True
            logger.info("Monday open: %s qty=%.0f — %s", sym, qty, order_desc)

            if not order_placed:
                continue

            db.execute(
                text("""
                    UPDATE weekly_plan SET status = 'EXECUTED'
                    WHERE symbol = :sym AND mode = :mode
                      AND week_start = (
                          SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
                      )
                """),
                {"sym": sym, "mode": mode},
            )
            db.execute(
                text("""
                    INSERT INTO trade_log (symbol, action, qty, price, trigger, mode)
                    VALUES (:s, 'BUY', :q, :p, 'MONDAY_OPEN', :m)
                """),
                {"s": sym, "q": qty, "p": entry, "m": mode},
            )
            db.commit()
            held.add(sym)

        except Exception as exc:
            logger.error("Monday open: buy failed for %s: %s", sym, exc)


def check_post_close(db: Session):
    """
    Called in each monitor cycle. Detects newly closed positions then:
      1. Infers why the position closed
      2. Runs post-close Claude analysis
      3. Runs slot-refill analysis to decide whether to open a replacement
      4. If approved, runs pre-trade gate before executing
    """
    mode = get_setting(db, "trading_mode", "paper")

    try:
        current = {p.symbol for p in alp.get_positions(mode)}
    except Exception as exc:
        logger.error("check_post_close: cannot fetch positions: %s", exc)
        return

    snapshot_key = f"positions_snapshot_{mode}"
    snap_row = db.execute(
        text("SELECT value FROM settings WHERE key = :k"),
        {"k": snapshot_key},
    ).fetchone()
    prev = set(snap_row[0].split(",")) if snap_row and snap_row[0] else set()

    set_setting(db, snapshot_key, ",".join(sorted(current)))
    db.commit()

    closed = prev - current
    if not closed:
        return

    logger.info("[%s] Detected closed positions: %s", mode, closed)

    api_key   = get_setting(db, "claude_api_key", "")
    auto_exec = get_setting(db, "auto_execute", "true").lower() == "true"
    max_pos   = _effective_max_positions(db, mode)

    for sym in closed:
        close_reason, entry_price, close_price = _infer_close_reason(db, sym, mode)
        logger.info(
            "Post-close [%s]: %s closed via %s (entry=$%s close=$%s)",
            mode, sym, close_reason,
            f"{entry_price:.2f}" if entry_price else "?",
            f"{close_price:.2f}" if close_price else "?",
        )

        if api_key:
            _run_claude_analysis(db, sym, mode)

        if auto_exec and len(current) < max_pos:
            _refill_slot(
                db=db,
                mode=mode,
                closed_symbol=sym,
                close_reason=close_reason,
                entry_price=entry_price,
                close_price=close_price,
                current_positions=current,
                max_pos=max_pos,
            )
            try:
                current = {p.symbol for p in alp.get_positions(mode)}
            except Exception:
                pass


def _refill_slot(
    db: Session,
    mode: str,
    closed_symbol: str,
    close_reason: str,
    entry_price: float | None,
    close_price: float | None,
    current_positions: set,
    max_pos: int,
):
    """Run slot-refill analysis and execute the recommended pick if approved."""
    from .claude_analyst import analyze_slot_refill, log_analysis

    try:
        acct         = alp.get_account(mode)
        portfolio    = float(acct.portfolio_value)
        cash         = float(acct.cash)
        buying_power = float(acct.buying_power)
    except Exception as exc:
        logger.error("Slot refill: account fetch failed: %s", exc)
        return

    held_tuple = tuple(current_positions) if current_positions else ("__none__",)
    pending_rows = db.execute(
        text("""
            SELECT symbol, score, signal, entry_price, stop_price, target1, rationale, rank,
                   COALESCE(screener_type, 'minervini') AS screener_type
            FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
            )
              AND mode = :mode
              AND status = 'PENDING'
              AND symbol NOT IN :held
            ORDER BY rank ASC
        """),
        {"mode": mode, "held": held_tuple},
    ).fetchall()

    pending_picks = [dict(r._mapping) for r in pending_rows]

    if not pending_picks:
        logger.info("Slot refill [%s]: no PENDING picks remaining.", mode)
        return

    try:
        analysis = analyze_slot_refill(
            db=db,
            closed_symbol=closed_symbol,
            close_reason=close_reason,
            entry_price=entry_price,
            close_price=close_price,
            portfolio_value=portfolio,
            cash=cash,
            buying_power=buying_power,
            open_positions=list(current_positions),
            pending_picks=pending_picks,
            mode=mode,
        )

        log_analysis(
            db,
            trigger="slot_refill",
            symbol=closed_symbol,
            analysis_text=(
                f"Closed: {closed_symbol} ({close_reason})\n"
                f"VERDICT: {analysis['verdict']}\n"
                f"SYMBOL: {analysis['symbol'] or 'NONE'}\n"
                f"REASON: {analysis['reason']}\n\n"
                f"{analysis['analysis']}"
            ),
            mode=mode,
        )

        logger.info(
            "Slot refill [%s]: verdict=%s symbol=%s reason=%s",
            mode, analysis["verdict"], analysis["symbol"], analysis["reason"],
        )

        if not analysis["should_open"]:
            logger.info("Slot refill [%s]: %s — no new position opened.", mode, analysis["verdict"])
            return

        _execute_specific_pick(db=db, mode=mode, symbol=analysis["symbol"], pending_picks=pending_picks)

    except Exception as exc:
        logger.error(
            "Slot refill analysis failed: %s — NOT auto-executing fallback. Manual review required.", exc
        )
        try:
            from .claude_analyst import log_analysis
            log_analysis(
                db, "slot_refill", closed_symbol,
                f"Slot-refill analysis failed: {exc}. "
                "No position opened automatically — manual review required.",
                mode,
            )
        except Exception:
            pass


def _execute_specific_pick(db: Session, mode: str, symbol: str, pending_picks: list[dict]):
    """Execute a specific symbol from the pending picks list."""
    pick = next((p for p in pending_picks if p["symbol"] == symbol), None)
    if not pick:
        logger.warning("Slot refill: recommended symbol %s not found in pending picks.", symbol)
        return

    entry  = float(pick.get("entry_price")   or 0)
    stop   = float(pick.get("stop_price")   or 0)
    target = float(pick.get("target1")      or 0)
    stype  = pick.get("screener_type", "minervini")

    if entry <= 0:
        logger.warning("Slot refill: %s has no entry price — skipping.", symbol)
        return

    try:
        acct      = alp.get_account(mode)
        portfolio = float(acct.portfolio_value)
    except Exception as exc:
        logger.error("Slot refill: account fetch failed: %s", exc)
        return

    risk_pct = float(get_setting(db, "risk_pct", "2.0"))
    stop_pct = float(get_setting(db, "stop_loss_pct", "8.0"))
    qty      = _size_qty(portfolio, entry, stop, risk_pct, stop_pct)

    if qty < 1:
        logger.info("Slot refill: %s position size < 1 share — skipping.", symbol)
        return

    if not _gate(db, symbol, qty, entry, stop, target, "SLOT_REFILL", mode):
        return

    try:
        order_desc = _place_entry(db, symbol, qty, entry, stop, target, "SLOT_REFILL", mode, stype)
        logger.info("Slot refill: %s qty=%.0f — %s [%s]", symbol, qty, order_desc, mode)

        db.execute(
            text("""
                UPDATE weekly_plan SET status = 'EXECUTED'
                WHERE symbol = :sym AND mode = :mode
                  AND week_start = (
                      SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
                  )
            """),
            {"sym": symbol, "mode": mode},
        )
        db.execute(
            text("""
                INSERT INTO trade_log (symbol, action, qty, price, trigger, mode)
                VALUES (:s, 'BUY', :q, :p, 'SLOT_REFILL', :m)
            """),
            {"s": symbol, "q": qty, "p": entry, "m": mode},
        )
        db.commit()
        logger.info("Slot refill complete: opened %s [%s]", symbol, mode)

    except Exception as exc:
        logger.error("Slot refill buy failed for %s: %s", symbol, exc)


def _execute_next_pick(db: Session, mode: str, held: set):
    """Fallback — execute the next PENDING pick without slot-refill analysis."""
    row = db.execute(
        text("""
            SELECT symbol, entry_price, stop_price, target1,
                   COALESCE(screener_type, 'minervini') AS screener_type
            FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
            )
              AND mode = :mode
              AND status = 'PENDING'
            ORDER BY rank ASC
            LIMIT 1
        """),
        {"mode": mode},
    ).fetchone()

    if not row:
        logger.info("Post-close fallback: no PENDING picks left for mode=%s.", mode)
        return

    sym    = row[0]
    entry  = float(row[1] or 0)
    stop   = float(row[2] or 0)
    target = float(row[3] or 0)
    stype  = row[4]

    if sym in held or entry <= 0:
        return

    try:
        acct      = alp.get_account(mode)
        portfolio = float(acct.portfolio_value)
    except Exception as exc:
        logger.error("Post-close fallback: account fetch failed: %s", exc)
        return

    risk_pct = float(get_setting(db, "risk_pct", "2.0"))
    stop_pct = float(get_setting(db, "stop_loss_pct", "8.0"))
    qty      = _size_qty(portfolio, entry, stop, risk_pct, stop_pct)
    if qty < 1:
        return

    if not _gate(db, sym, qty, entry, stop, target, "POST_CLOSE", mode):
        return

    try:
        order_desc = _place_entry(db, sym, qty, entry, stop, target, "POST_CLOSE", mode, stype)
        logger.info("Post-close fallback: %s qty=%.0f — %s", sym, qty, order_desc)

        db.execute(
            text("""
                UPDATE weekly_plan SET status = 'EXECUTED'
                WHERE symbol = :sym AND mode = :mode
                  AND week_start = (
                      SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
                  )
            """),
            {"sym": sym, "mode": mode},
        )
        db.execute(
            text("""
                INSERT INTO trade_log (symbol, action, qty, price, trigger, mode)
                VALUES (:s, 'BUY', :q, :p, 'POST_CLOSE', :m)
            """),
            {"s": sym, "q": qty, "p": entry, "m": mode},
        )
        db.commit()
        logger.info("Post-close fallback: opened %s [%s]", sym, mode)

    except Exception as exc:
        logger.error("Post-close fallback buy failed for %s: %s", sym, exc)


def _run_claude_analysis(db: Session, closed_sym: str, mode: str):
    try:
        from .claude_analyst import analyze_picks, log_analysis

        picks_rows = db.execute(
            text("""
                SELECT symbol, score, signal, entry_price, stop_price, target1, status, rationale
                FROM weekly_plan
                WHERE week_start = (
                    SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode
                )
                  AND mode = :mode
                ORDER BY rank ASC
            """),
            {"mode": mode},
        ).fetchall()

        picks = [dict(r._mapping) for r in picks_rows]

        entry_row = db.execute(
            text("""
                SELECT price FROM trade_log
                WHERE symbol = :s AND action = 'BUY' AND mode = :mode
                ORDER BY created_at DESC LIMIT 1
            """),
            {"s": closed_sym, "mode": mode},
        ).fetchone()

        closed_ctx = {
            "symbol":      closed_sym,
            "entry_price": float(entry_row[0]) if entry_row else None,
            "reason":      "position closed (stop hit or target reached)",
        }

        analysis = analyze_picks(db, picks, closed_position=closed_ctx)
        log_analysis(db, "post_close", closed_sym, analysis, mode)
        logger.info("Post-close analysis saved for %s [%s].", closed_sym, mode)

    except Exception as exc:
        logger.warning("Post-close analysis failed for %s: %s", closed_sym, exc)