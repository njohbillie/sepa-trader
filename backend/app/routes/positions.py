import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db, get_current_user, get_all_user_settings
from ..config import settings as global_settings
from .. import alpaca_client as alp
from ..sepa_analyzer import analyze
from ..utils import sf

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/positions", tags=["positions"])


def _resolve_alpaca_client(user_settings: dict, mode: str, is_admin: bool = False):
    if mode == "paper":
        key    = user_settings.get("alpaca_paper_key")
        secret = user_settings.get("alpaca_paper_secret")
        if is_admin:
            key    = key    or global_settings.alpaca_paper_key
            secret = secret or global_settings.alpaca_paper_secret
        paper = True
    else:
        key    = user_settings.get("alpaca_live_key")
        secret = user_settings.get("alpaca_live_secret")
        if is_admin:
            key    = key    or global_settings.alpaca_live_key
            secret = secret or global_settings.alpaca_live_secret
        paper = False
    if not key or not secret:
        raise HTTPException(status_code=400, detail="alpaca_credentials_missing")
    return alp.get_client_for_keys(key, secret, paper)


@router.get("")
def positions(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    user_settings = get_all_user_settings(db, current_user["id"])
    mode   = user_settings.get("trading_mode", "paper")
    client = _resolve_alpaca_client(user_settings, mode, is_admin=current_user["role"] == "admin")
    raw    = client.get_all_positions()

    if not raw:
        return []

    symbols = [p.symbol for p in raw]

    plan_rows = db.execute(
        text("""
            SELECT DISTINCT ON (symbol)
                symbol, stop_price, target1, target2, week_start
            FROM weekly_plan
            WHERE symbol = ANY(:syms)
              AND mode = :mode
              AND user_id = :uid
            ORDER BY symbol, week_start DESC
        """),
        {"syms": symbols, "mode": mode, "uid": current_user["id"]},
    ).fetchall()

    plan_map = {
        r[0]: {
            "stop_price": sf(r[1]),
            "target1":    sf(r[2]),
            "target2":    sf(r[3]),
            "plan_week":  str(r[4]) if r[4] else None,
        }
        for r in plan_rows
    }

    out = []
    for p in raw:
        signal_data = analyze(p.symbol)
        plan        = plan_map.get(p.symbol, {})
        out.append({
            "symbol":          p.symbol,
            "qty":             sf(p.qty, 0.0),
            "entry_price":     sf(p.avg_entry_price, 0.0),
            "current_price":   sf(p.current_price, 0.0),
            "market_value":    sf(p.market_value, 0.0),
            "unrealized_pl":   sf(p.unrealized_pl, 0.0),
            "unrealized_plpc": (sf(p.unrealized_plpc, 0.0) or 0.0) * 100,
            "signal":          signal_data.get("signal"),
            "score":           signal_data.get("score"),
            "ema20":           signal_data.get("ema20"),
            "ema50":           signal_data.get("ema50"),
            "week52_high":     signal_data.get("week52_high"),
            "week52_low":      signal_data.get("week52_low"),
            "stop_price":      plan.get("stop_price"),
            "target1":         plan.get("target1"),
            "target2":         plan.get("target2"),
            "plan_week":       plan.get("plan_week"),
            "mode":            mode,
        })
    return out


@router.delete("/{symbol}")
def close(symbol: str, current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    user_settings = get_all_user_settings(db, current_user["id"])
    mode   = user_settings.get("trading_mode", "paper")
    client = _resolve_alpaca_client(user_settings, mode, is_admin=current_user["role"] == "admin")
    client.close_position(symbol.upper())
    return {"status": "closed", "symbol": symbol.upper()}


def _upsert_plan_exits(db: Session, symbol: str, stop: float, target: float, mode: str, user_id: int):
    """Upsert stop/target into the current week's plan for a given mode and user."""
    existing = db.execute(
        text("""
            SELECT id FROM weekly_plan
            WHERE symbol = :sym
              AND mode = :mode
              AND user_id = :uid
              AND week_start = (
                  SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
              )
        """),
        {"sym": symbol, "mode": mode, "uid": user_id},
    ).fetchone()

    if existing:
        db.execute(
            text("""
                UPDATE weekly_plan
                SET stop_price = :stop, target1 = :target
                WHERE symbol = :sym
                  AND mode = :mode
                  AND user_id = :uid
                  AND week_start = (
                      SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
                  )
            """),
            {"stop": stop, "target": target, "sym": symbol, "mode": mode, "uid": user_id},
        )
    else:
        db.execute(
            text("""
                INSERT INTO weekly_plan
                    (week_start, symbol, rank, score, entry_price, stop_price, target1, status, mode, user_id)
                VALUES (
                    COALESCE(
                        (SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid),
                        CURRENT_DATE
                    ),
                    :sym, 99, 0, 0, :stop, :target, 'EXECUTED', :mode, :uid
                )
            """),
            {"sym": symbol, "stop": stop, "target": target, "mode": mode, "uid": user_id},
        )
    db.commit()


@router.patch("/{symbol}/exits")
def set_exit_levels(
    symbol: str,
    stop: float = Query(..., gt=0),
    target: float = Query(..., gt=0),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Save stop/target to the plan. Exit guard detects the change on
    the next monitor cycle and replaces the OCO automatically.
    """
    symbol        = symbol.upper()
    user_settings = get_all_user_settings(db, current_user["id"])
    mode          = user_settings.get("trading_mode", "paper")
    _upsert_plan_exits(db, symbol, stop, target, mode, current_user["id"])
    return {"status": "ok", "symbol": symbol, "stop": stop, "target": target, "mode": mode}


@router.post("/{symbol}/place-exits")
def place_exits_now(
    symbol: str,
    stop: float = Query(..., gt=0),
    target: float = Query(..., gt=0),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Save levels to the plan AND immediately replace any existing exit orders
    on Alpaca with a fresh OCO at the new levels.

    Process:
      1. Persist stop/target to weekly_plan
      2. Cancel ALL open sell orders for the symbol (including existing OCOs)
      3. Poll until Alpaca confirms orders are fully cancelled
      4. Place fresh OCO
    """
    symbol        = symbol.upper()
    user_settings = get_all_user_settings(db, current_user["id"])
    mode          = user_settings.get("trading_mode", "paper")
    client        = _resolve_alpaca_client(user_settings, mode, is_admin=current_user["role"] == "admin")

    # Step 1 — persist to plan
    _upsert_plan_exits(db, symbol, stop, target, mode, current_user["id"])

    # Step 2 — confirm position is still open
    positions = client.get_all_positions()
    pos = next((p for p in positions if p.symbol == symbol), None)
    if not pos:
        raise HTTPException(
            status_code=404,
            detail=f"No open {mode} position found for {symbol}",
        )

    qty = float(pos.qty)

    # Step 3 — cancel all existing sell orders using the user's client
    from alpaca.trading.requests import GetOrdersRequest
    from alpaca.trading.enums import QueryOrderStatus
    import time as _time

    open_orders = client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
    cancelled = []
    for o in open_orders:
        if o.symbol == symbol and 'sell' in str(getattr(o, 'side', '') or '').lower():
            try:
                client.cancel_order_by_id(str(o.id))
                cancelled.append(str(o.id))
            except Exception as exc:
                logger.warning("Could not cancel order %s for %s: %s", o.id, symbol, exc)
    logger.info(
        "place_exits_now: cancelled %d sell order(s) for %s [%s]",
        len(cancelled), symbol, mode,
    )

    if cancelled:
        # Step 4 — poll until Alpaca confirms orders are gone
        elapsed, timeout = 0.0, 15.0
        cleared = False
        while elapsed < timeout:
            remaining = client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
            sell_open = [o for o in remaining if o.symbol == symbol and 'sell' in str(getattr(o, 'side', '') or '').lower()]
            if not sell_open:
                cleared = True
                break
            _time.sleep(0.5)
            elapsed += 0.5
        if not cleared:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Timed out waiting for existing {symbol} orders to cancel. "
                    "Wait a few seconds and try again."
                ),
            )

    # Step 5 — place fresh OCO using the user's client
    try:
        from alpaca.trading.requests import LimitOrderRequest, StopLossRequest, TakeProfitRequest
        from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
        oco_req = LimitOrderRequest(
            symbol=symbol,
            qty=round(qty, 0),
            side=OrderSide.SELL,
            time_in_force=TimeInForce.GTC,
            limit_price=round(target, 2),
            order_class=OrderClass.OCO,
            stop_loss=StopLossRequest(stop_price=round(stop, 2)),
            take_profit=TakeProfitRequest(limit_price=round(target, 2)),
        )
        client.submit_order(oco_req)
        logger.info(
            "place_exits_now: placed OCO for %s qty=%.0f stop=$%.2f target=$%.2f [%s]",
            symbol, qty, stop, target, mode,
        )
    except Exception as exc:
        logger.error("place_exits_now: OCO placement failed for %s: %s", symbol, exc)
        raise HTTPException(
            status_code=502,
            detail=f"Orders were cancelled but new OCO failed to place: {str(exc)[:200]}",
        )

    return {
        "status": "ok",
        "symbol": symbol,
        "qty":    qty,
        "stop":   stop,
        "target": target,
        "mode":   mode,
    }