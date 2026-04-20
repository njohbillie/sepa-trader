from fastapi import APIRouter, Depends
from sqlalchemy import text
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
            "symbol":          p.symbol,
            "qty":             float(p.qty),
            "entry_price":     float(p.avg_entry_price),
            "current_price":   float(p.current_price),
            "market_value":    float(p.market_value),
            "unrealized_pl":   float(p.unrealized_pl),
            "unrealized_plpc": float(p.unrealized_plpc) * 100,
            "signal":          signal_data.get("signal"),
            "score":           signal_data.get("score"),
            "ema20":           signal_data.get("ema20"),
            "ema50":           signal_data.get("ema50"),
            "week52_high":     signal_data.get("week52_high"),
            "week52_low":      signal_data.get("week52_low"),
        })
    return out


@router.delete("/{symbol}")
def close(symbol: str, db: Session = Depends(get_db)):
    mode = get_setting(db, "trading_mode", "paper")
    alp.close_position(symbol.upper(), mode)
    return {"status": "closed", "symbol": symbol.upper()}


@router.patch("/{symbol}/exits")
def set_exit_levels(
    symbol: str,
    stop: float,
    target: float,
    db: Session = Depends(get_db),
):
    """
    Set or update stop_price and target1 for a symbol in the current week's plan.
    If the symbol has no weekly_plan row yet, inserts one marked EXECUTED so the
    exit guard can pick it up on the next monitor cycle.
    """
    symbol = symbol.upper()

    # Check if row exists
    existing = db.execute(
        text("""
            SELECT id FROM weekly_plan
            WHERE symbol = :sym
              AND week_start = (SELECT MAX(week_start) FROM weekly_plan)
        """),
        {"sym": symbol},
    ).fetchone()

    if existing:
        db.execute(
            text("""
                UPDATE weekly_plan
                SET stop_price = :stop, target1 = :target
                WHERE symbol = :sym
                  AND week_start = (SELECT MAX(week_start) FROM weekly_plan)
            """),
            {"stop": stop, "target": target, "sym": symbol},
        )
    else:
        # Position exists in Alpaca but not in weekly_plan — insert a stub row
        db.execute(
            text("""
                INSERT INTO weekly_plan
                    (week_start, symbol, rank, score, entry_price, stop_price, target1, status, mode)
                VALUES (
                    (SELECT COALESCE(MAX(week_start), CURRENT_DATE) FROM weekly_plan),
                    :sym, 99, 0,
                    (SELECT avg_entry_price FROM positions WHERE symbol = :sym LIMIT 1),
                    :stop, :target, 'EXECUTED',
                    (SELECT value FROM settings WHERE key = 'trading_mode' LIMIT 1)
                )
            """),
            {"sym": symbol, "stop": stop, "target": target},
        )

    db.commit()
    return {"status": "ok", "symbol": symbol, "stop": stop, "target": target}