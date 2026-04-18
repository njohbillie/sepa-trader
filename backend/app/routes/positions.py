from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from ..database import get_db, get_setting
from .. import alpaca_client as alp
from ..sepa_analyzer import analyze

router = APIRouter(prefix="/api/positions", tags=["positions"])


@router.get("")
def positions(db: Session = Depends(get_db)):
    mode = get_setting(db, "trading_mode", "paper")
    raw  = alp.get_positions(mode)
    out  = []
    for p in raw:
        signal_data = analyze(p.symbol)
        out.append({
            "symbol":         p.symbol,
            "qty":            float(p.qty),
            "entry_price":    float(p.avg_entry_price),
            "current_price":  float(p.current_price),
            "market_value":   float(p.market_value),
            "unrealized_pl":  float(p.unrealized_pl),
            "unrealized_plpc": float(p.unrealized_plpc) * 100,
            "signal":         signal_data.get("signal"),
            "score":          signal_data.get("score"),
            "ema20":          signal_data.get("ema20"),
            "ema50":          signal_data.get("ema50"),
            "week52_high":    signal_data.get("week52_high"),
            "week52_low":     signal_data.get("week52_low"),
        })
    return out


@router.delete("/{symbol}")
def close(symbol: str, db: Session = Depends(get_db)):
    mode = get_setting(db, "trading_mode", "paper")
    alp.close_position(symbol.upper(), mode)
    return {"status": "closed", "symbol": symbol.upper()}
