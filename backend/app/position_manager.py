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
    user_id: int | None = None,
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
            user_id=user_id,
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


def _count_positions_by_type(db: Session, mode: str, symbols: set) -> tuple[int, int, int]:
    """
    Return (mv_count, pb_count, rs_count) of currently held positions by screener type.
    Looks up the screener_type from the most recent EXECUTED weekly_plan row.
    'both' and 'minervini' → mv; 'pullback' → pb; 'rs_momentum' → rs.
    """
    if not symbols:
        return 0, 0, 0
    rows = db.execute(
        text("""
            SELECT COALESCE(screener_type, 'minervini') AS stype, COUNT(*) AS cnt
            FROM (
                SELECT DISTINCT ON (symbol) symbol, screener_type
                FROM weekly_plan
                WHERE symbol IN :syms AND mode = :mode AND status = 'EXECUTED'
                ORDER BY symbol, week_start DESC
            ) sub
            GROUP BY stype
        """),
        {"syms": tuple(symbols), "mode": mode},
    ).fetchall()
    mv, pb, rs = 0, 0, 0
    for stype, cnt in rows:
        if stype in ("minervini", "both"):
            mv += cnt
        elif stype == "rs_momentum":
            rs += cnt
        else:
            pb += cnt
    return mv, pb, rs


def _get_symbol_screener_type(db: Session, symbol: str, mode: str) -> str:
    """Return the screener_type of the most recent plan row for this symbol."""
    row = db.execute(
        text("""
            SELECT COALESCE(screener_type, 'minervini')
            FROM weekly_plan
            WHERE symbol = :sym AND mode = :mode
            ORDER BY week_start DESC LIMIT 1
        """),
        {"sym": symbol, "mode": mode},
    ).fetchone()
    return row[0] if row else "minervini"


def run_monday_open(db: Session):
    """
    Called every Monday at 9:35 ET. Fills available position slots from the
    current week's PENDING picks using dedicated per-strategy slot allocation.

    mv_max_slots — max Minervini (breakout) positions at any time (default 3)
    pb_max_slots — max Pullback positions at any time (default 2)
    Both are still capped by the overall max_positions setting.
    """
    mode      = get_setting(db, "trading_mode", "paper")
    auto_exec = get_setting(db, "auto_execute", "true").lower() == "true"
    if not auto_exec:
        logger.info("Monday open: auto_execute off — skipping.")
        return

    max_pos = _effective_max_positions(db, mode)
    mv_max  = int(get_setting(db, "mv_max_slots", "3") or "3")
    pb_max  = int(get_setting(db, "pb_max_slots", "2") or "2")

    try:
        positions = alp.get_positions(mode)
    except Exception as exc:
        logger.error("Monday open: could not fetch positions: %s", exc)
        return

    total_held = len(positions)
    if total_held >= max_pos:
        logger.info("Monday open: portfolio full (%d/%d). No buys.", total_held, max_pos)
        return

    held_symbols   = {p.symbol for p in positions}
    mv_held, pb_held = _count_positions_by_type(db, mode, held_symbols)

    mv_slots = min(max(0, mv_max - mv_held), max_pos - total_held)
    pb_slots = min(max(0, pb_max - pb_held), max_pos - total_held - mv_slots)

    logger.info(
        "Monday open [%s]: held=%d (mv=%d pb=%d) | slots: mv=%d pb=%d | max_pos=%d",
        mode, total_held, mv_held, pb_held, mv_slots, pb_slots, max_pos,
    )

    # Fetch Minervini picks (screener_type = 'minervini' or 'both')
    mv_rows = db.execute(
        text("""
            SELECT symbol, entry_price, stop_price, target1,
                   COALESCE(screener_type, 'minervini') AS screener_type
            FROM weekly_plan
            WHERE week_start = (SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode)
              AND mode = :mode AND status = 'PENDING'
              AND COALESCE(screener_type, 'minervini') IN ('minervini', 'both')
            ORDER BY rank ASC
            LIMIT :slots
        """),
        {"slots": mv_slots, "mode": mode},
    ).fetchall() if mv_slots > 0 else []

    # Fetch Pullback picks (screener_type = 'pullback')
    pb_rows = db.execute(
        text("""
            SELECT symbol, entry_price, stop_price, target1,
                   COALESCE(screener_type, 'minervini') AS screener_type
            FROM weekly_plan
            WHERE week_start = (SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode)
              AND mode = :mode AND status = 'PENDING'
              AND COALESCE(screener_type, 'minervini') = 'pullback'
            ORDER BY rank ASC
            LIMIT :slots
        """),
        {"slots": pb_slots, "mode": mode},
    ).fetchall() if pb_slots > 0 else []

    rows = list(mv_rows) + list(pb_rows)

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


def check_post_close(db: Session, mode: str | None = None):
    """
    Called in each monitor cycle. Detects newly closed positions then:
      1. Infers why the position closed
      2. Runs post-close Claude analysis
      3. Runs slot-refill analysis to decide whether to open a replacement
      4. If approved, runs pre-trade gate before executing
    """
    if mode is None:
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
        closed_stype = _get_symbol_screener_type(db, sym, mode)
        logger.info(
            "Post-close [%s]: %s (%s) closed via %s (entry=$%s close=$%s)",
            mode, sym, closed_stype, close_reason,
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
                closed_stype=closed_stype,
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
    closed_stype: str,
    close_reason: str,
    entry_price: float | None,
    close_price: float | None,
    current_positions: set,
    max_pos: int,
):
    """
    Run slot-refill analysis and execute the recommended pick if approved.
    Respects per-strategy slot allocation:
      - Tries to refill same type as the closed position first (mv→mv, pb→pb)
      - Falls back to any available type if no same-type picks remain
    """
    from .claude_analyst import analyze_slot_refill, log_analysis

    mv_max = int(get_setting(db, "mv_max_slots", "3") or "3")
    pb_max = int(get_setting(db, "pb_max_slots", "2") or "2")
    mv_held, pb_held = _count_positions_by_type(db, mode, current_positions)

    try:
        acct         = alp.get_account(mode)
        portfolio    = float(acct.portfolio_value)
        cash         = float(acct.cash)
        buying_power = float(acct.buying_power)
    except Exception as exc:
        logger.error("Slot refill: account fetch failed: %s", exc)
        return

    held_tuple = tuple(current_positions) if current_positions else ("__none__",)

    # Determine which type(s) have open slots
    mv_available = mv_held < mv_max
    pb_available = pb_held < pb_max

    # Build type filter: prefer same type as what closed, fall back if needed
    if closed_stype in ("minervini", "both") and mv_available:
        type_filter = ("minervini", "both")
    elif closed_stype == "pullback" and pb_available:
        type_filter = ("pullback",)
    elif mv_available:
        type_filter = ("minervini", "both")
        logger.info("Slot refill: no pb slot available — trying mv pick instead")
    elif pb_available:
        type_filter = ("pullback",)
        logger.info("Slot refill: no mv slot available — trying pb pick instead")
    else:
        logger.info(
            "Slot refill [%s]: all strategy slots full (mv=%d/%d pb=%d/%d) — skipping",
            mode, mv_held, mv_max, pb_held, pb_max,
        )
        return

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
              AND COALESCE(screener_type, 'minervini') = ANY(:types)
            ORDER BY rank ASC
        """),
        {"mode": mode, "held": held_tuple, "types": list(type_filter)},
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


def fill_open_slots(
    db: Session,
    mode: str,
    portfolio: float,
    risk_pct: float,
    stop_pct: float,
    positions: list,
    user_id: int | None = None,
):
    """
    Called every monitor cycle. Fills open position slots from the current
    week's PENDING weekly_plan picks whenever capacity exists — not just on
    Monday morning.

    Respects mv_max_slots / pb_max_slots per-strategy limits.
    Only buys when the current SEPA signal confirms entry (BREAKOUT or PULLBACK).
    Runs the AI pre-trade gate before each order.
    """
    mv_max  = int(get_setting(db, "mv_max_slots", "3") or "3")
    pb_max  = int(get_setting(db, "pb_max_slots", "2") or "2")
    max_pos = _effective_max_positions(db, mode)

    total_held   = len(positions)
    held_symbols = {p.symbol for p in positions}

    # Sync DB → EXECUTED for any symbol that is currently held OR was bought
    # today (even if already closed). Prevents re-entering a position that was
    # stopped out and reset to PENDING by a screener re-run.
    try:
        bought_today = {
            row[0] for row in db.execute(
                text("""
                    SELECT DISTINCT symbol FROM trade_log
                    WHERE action = 'BUY'
                      AND mode = :mode
                      AND created_at >= CURRENT_DATE
                """),
                {"mode": mode},
            ).fetchall()
        }
    except Exception:
        bought_today = set()

    already_active = held_symbols | bought_today
    if already_active:
        try:
            db.execute(
                text("""
                    UPDATE weekly_plan SET status = 'EXECUTED'
                    WHERE week_start = (SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode)
                      AND mode   = :mode
                      AND symbol IN :syms
                      AND status = 'PENDING'
                """),
                {"mode": mode, "syms": tuple(already_active)},
            )
            db.commit()
        except Exception as exc:
            logger.warning("fill_open_slots: position sync failed: %s", exc)

    if total_held >= max_pos:
        return

    rs_max = int(get_setting(db, "rs_max_slots", "2") or "2")

    mv_held, pb_held, rs_held = _count_positions_by_type(db, mode, held_symbols)
    mv_slots = max(0, mv_max - mv_held)
    pb_slots = max(0, pb_max - pb_held)
    rs_slots = max(0, rs_max - rs_held)

    if mv_slots == 0 and pb_slots == 0 and rs_slots == 0:
        logger.debug("fill_open_slots [%s]: all strategy slots full — skipping.", mode)
        return

    held_tuple = tuple(held_symbols) if held_symbols else ("__none__",)

    rows = db.execute(
        text("""
            SELECT symbol, entry_price, stop_price, target1,
                   COALESCE(screener_type, 'minervini') AS screener_type
            FROM weekly_plan
            WHERE week_start = (SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode)
              AND mode = :mode
              AND status = 'PENDING'
              AND symbol NOT IN :held
            ORDER BY rank ASC
        """),
        {"mode": mode, "held": held_tuple},
    ).fetchall()

    if not rows:
        logger.debug("fill_open_slots [%s]: no PENDING picks in weekly plan.", mode)
        return

    logger.info(
        "fill_open_slots [%s]: %d PENDING picks | slots mv=%d pb=%d rs=%d | held=%d/%d",
        mode, len(rows), mv_slots, pb_slots, rs_slots, total_held, max_pos,
    )

    # Build cooldown set: symbols sold within the last 8 hours — never re-enter same day
    try:
        sold_recently = {
            row[0] for row in db.execute(
                text("""
                    SELECT DISTINCT symbol FROM trade_log
                    WHERE action = 'SELL'
                      AND mode  = :mode
                      AND created_at >= NOW() - INTERVAL '8 hours'
                """),
                {"mode": mode},
            ).fetchall()
        }
    except Exception:
        sold_recently = set()

    from .sepa_analyzer import analyze

    for row in rows:
        sym    = row[0]
        entry  = float(row[1] or 0)
        stop   = float(row[2] or 0)
        target = float(row[3] or 0)
        stype  = row[4]

        # Skip symbols that were just sold — don't re-enter same day
        if sym in sold_recently:
            logger.info("fill_open_slots: %s sold within last 8h — skipping re-entry.", sym)
            continue

        # Check slot availability for this pick's strategy type
        if stype in ("minervini", "both"):
            if mv_slots <= 0:
                continue
        elif stype == "pullback":
            if pb_slots <= 0:
                continue
        elif stype == "rs_momentum":
            if rs_slots <= 0:
                continue

        # RS momentum picks are already in Stage 2 by screener definition —
        # skip SEPA signal confirmation and buy at current price.
        if stype == "rs_momentum":
            price = entry
            signal = "RS_MOMENTUM"
        else:
            # Confirm current SEPA signal before buying stale screener picks
            result = analyze(sym, db=db)
            signal = result.get("signal", "ERROR")
            price  = result.get("price") or entry

            if signal not in ("BREAKOUT", "PULLBACK_EMA20", "PULLBACK_EMA50", "STAGE2_WATCH"):
                logger.debug("fill_open_slots: %s signal=%s — skipping.", sym, signal)
                continue

        if price <= 0:
            continue

        qty = _size_qty(portfolio, price, stop, risk_pct, stop_pct)
        # Cap at max_position_pct of portfolio
        max_position_pct = float(get_setting(db, "max_position_pct", "20.0") or "20.0")
        if price > 0:
            max_shares = int(portfolio * max_position_pct / 100 / price)
            if max_shares > 0:
                qty = min(qty, max_shares)
        if qty < 1:
            logger.info("fill_open_slots: %s qty<1 (price=$%.2f stop=$%.2f) — skipping.", sym, price, stop)
            continue

        if not _gate(db, sym, qty, price, stop, target, f"FILL_{signal}", mode, user_id=user_id):
            logger.info("fill_open_slots: %s blocked by pre-trade gate.", sym)
            continue

        try:
            order_desc = _place_entry(db, sym, qty, price, stop, target, f"FILL_{signal}", mode, stype)
            logger.info(
                "fill_open_slots [%s]: opened %s qty=%.0f signal=%s — %s",
                mode, sym, qty, signal, order_desc,
            )

            db.execute(
                text("""
                    UPDATE weekly_plan SET status = 'EXECUTED'
                    WHERE symbol = :sym AND mode = :mode
                      AND week_start = (SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode)
                """),
                {"sym": sym, "mode": mode},
            )
            db.execute(
                text("""
                    INSERT INTO trade_log (symbol, action, qty, price, trigger, mode)
                    VALUES (:s, 'BUY', :q, :p, :t, :m)
                """),
                {"s": sym, "q": qty, "p": price, "t": f"FILL_{signal}", "m": mode},
            )
            db.commit()

            held_symbols.add(sym)
            total_held += 1
            if stype in ("minervini", "both"):
                mv_held  += 1
                mv_slots -= 1
            elif stype == "rs_momentum":
                rs_held  += 1
                rs_slots -= 1
            else:
                pb_held  += 1
                pb_slots -= 1

            if total_held >= max_pos or (mv_slots == 0 and pb_slots == 0 and rs_slots == 0):
                break

        except Exception as exc:
            logger.error("fill_open_slots: buy failed for %s: %s", sym, exc)