from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..database import get_db, get_setting
from .. import alpaca_client as alp

router = APIRouter(prefix="/api/account", tags=["account"])


@router.get("")
def account(db: Session = Depends(get_db)):
    mode = get_setting(db, "trading_mode", "paper")
    acct = alp.get_account(mode)
    equity     = float(acct.equity)
    last_equity = float(acct.last_equity)
    return {
        "mode":           mode,
        "portfolio_value": float(acct.portfolio_value),
        "cash":           float(acct.cash),
        "buying_power":   float(acct.buying_power),
        "equity":         equity,
        "day_pnl":        equity - last_equity,
        "day_pnl_pct":    (equity - last_equity) / last_equity * 100 if last_equity else 0,
    }
