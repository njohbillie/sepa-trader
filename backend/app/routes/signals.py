from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db
from ..sepa_analyzer import analyze
from ..trader import run_monitor

router = APIRouter(prefix="/api/signals", tags=["signals"])


@router.get("/analyze/{symbol}")
def analyze_symbol(symbol: str):
    return analyze(symbol.upper())


@router.get("/history")
def signal_history(symbol: str = None, limit: int = 100, db: Session = Depends(get_db)):
    q = "SELECT symbol, signal, score, price, mode, created_at FROM signal_log"
    params: dict = {"l": limit}
    if symbol:
        q += " WHERE symbol = :sym"
        params["sym"] = symbol.upper()
    q += " ORDER BY created_at DESC LIMIT :l"
    rows = db.execute(text(q), params).fetchall()
    return [
        {"symbol": r[0], "signal": r[1], "score": r[2],
         "price": float(r[3]) if r[3] else None, "mode": r[4], "timestamp": str(r[5])}
        for r in rows
    ]


@router.post("/run-monitor")
async def trigger_monitor(db: Session = Depends(get_db)):
    result = await run_monitor(db)
    return result
