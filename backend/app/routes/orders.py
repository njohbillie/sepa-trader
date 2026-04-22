from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db, get_current_user, get_user_setting, get_all_user_settings
from ..config import settings as global_settings
from .. import alpaca_client as alp
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
from ..utils import sf

router = APIRouter(prefix="/api/orders", tags=["orders"])


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


@router.get("/open")
def open_orders(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    user_settings = get_all_user_settings(db, current_user["id"])
    mode   = user_settings.get("trading_mode", "paper")
    client = _resolve_alpaca_client(user_settings, mode, is_admin=current_user["role"] == "admin")
    orders = client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
    return [
        {
            "id":           str(o.id),
            "symbol":       o.symbol,
            "side":         str(o.side),
            "qty":          sf(o.qty, 0.0),
            "status":       str(o.status),
            "type":         str(o.type),
            "order_class":  str(getattr(o, 'order_class', '') or ''),
            "submitted_at": str(o.submitted_at),
            "mode":         mode,
        }
        for o in orders
    ]


@router.get("/history")
def trade_history(
    limit: int = 50,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Internal trade log — filtered to current trading mode and user."""
    mode = get_user_setting(db, "trading_mode", "paper", current_user["id"])
    rows = db.execute(
        text("""
            SELECT symbol, action, qty, price, trigger, mode, created_at
            FROM trade_log
            WHERE mode = :mode AND user_id = :uid
            ORDER BY created_at DESC
            LIMIT :l
        """),
        {"l": limit, "mode": mode, "uid": current_user["id"]},
    ).fetchall()
    return [
        {
            "symbol":    r[0],
            "action":    r[1],
            "qty":       sf(r[2], 0.0),
            "price":     sf(r[3], 0.0),
            "trigger":   r[4],
            "mode":      r[5],
            "timestamp": str(r[6]),
        }
        for r in rows
    ]


@router.get("/alpaca-history")
def alpaca_order_history(
    limit: int = 100,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Full Alpaca order history from the active account (paper or live)."""
    user_settings = get_all_user_settings(db, current_user["id"])
    mode   = user_settings.get("trading_mode", "paper")
    client = _resolve_alpaca_client(user_settings, mode, is_admin=current_user["role"] == "admin")
    orders = client.get_orders(GetOrdersRequest(status=QueryOrderStatus.ALL, limit=limit))
    return [
        {
            "id":           str(o.id),
            "symbol":       o.symbol,
            "side":         str(o.side).replace("OrderSide.", ""),
            "qty":          sf(o.qty, 0.0),
            "filled_qty":   sf(o.filled_qty, 0.0),
            "filled_avg":   sf(o.filled_avg_price) if o.filled_avg_price else None,
            "status":       str(o.status).replace("OrderStatus.", ""),
            "type":         str(o.type).replace("OrderType.", ""),
            "order_class":  str(getattr(o, 'order_class', '') or ''),
            "submitted_at": str(o.submitted_at) if o.submitted_at else None,
            "filled_at":    str(o.filled_at)    if o.filled_at    else None,
            "mode":         mode,
        }
        for o in orders
    ]