from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db, get_current_user, get_user_setting
from ..sepa_analyzer import analyze
from ..trader import run_monitor

router = APIRouter(prefix="/api/signals", tags=["signals"])


@router.get("/analyze/{symbol}")
def analyze_symbol(symbol: str, _: dict = Depends(get_current_user)):
    return analyze(symbol.upper())


@router.get("/history")
def signal_history(
    symbol: str = None,
    limit: int = 100,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Signal history scoped to the active trading mode and user."""
    mode   = get_user_setting(db, "trading_mode", "paper", current_user["id"])
    q      = "SELECT symbol, signal, score, price, mode, created_at FROM signal_log WHERE mode = :mode AND user_id = :uid"
    params = {"l": limit, "mode": mode, "uid": current_user["id"]}
    if symbol:
        q += " AND symbol = :sym"
        params["sym"] = symbol.upper()
    q += " ORDER BY created_at DESC LIMIT :l"
    rows = db.execute(text(q), params).fetchall()
    return [
        {
            "symbol":    r[0],
            "signal":    r[1],
            "score":     r[2],
            "price":     float(r[3]) if r[3] else None,
            "mode":      r[4],
            "timestamp": str(r[5]),
        }
        for r in rows
    ]


@router.post("/run-monitor")
async def trigger_monitor(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    result = await run_monitor(db, user_id=current_user["id"])
    return result