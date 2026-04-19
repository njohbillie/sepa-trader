from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db, SessionLocal

router = APIRouter(prefix="/api/screener", tags=["screener"])


@router.get("/weekly-plan")
def get_weekly_plan(db: Session = Depends(get_db)):
    """Return the most recent weekly plan (latest week_start)."""
    rows = db.execute(
        text("""
            SELECT week_start, symbol, rank, score, signal,
                   entry_price, stop_price, target1, target2,
                   position_size, risk_amount, rationale, status, mode, created_at
            FROM weekly_plan
            WHERE week_start = (SELECT MAX(week_start) FROM weekly_plan)
            ORDER BY rank ASC
        """)
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/history")
def get_plan_history(db: Session = Depends(get_db)):
    """Return distinct week_start dates for the plan history dropdown."""
    rows = db.execute(
        text("SELECT DISTINCT week_start, COUNT(*) as cnt FROM weekly_plan GROUP BY week_start ORDER BY week_start DESC LIMIT 12")
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/weekly-plan/{week_start}")
def get_plan_for_week(week_start: str, db: Session = Depends(get_db)):
    rows = db.execute(
        text("""
            SELECT week_start, symbol, rank, score, signal,
                   entry_price, stop_price, target1, target2,
                   position_size, risk_amount, rationale, status, mode, created_at
            FROM weekly_plan
            WHERE week_start = :w
            ORDER BY rank ASC
        """),
        {"w": week_start},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/run")
def trigger_screener(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Trigger the screener immediately (runs in background)."""
    def _run():
        db2 = SessionLocal()
        try:
            from ..screener import run_screener
            run_screener(db2)
        except Exception as exc:
            import logging
            logging.getLogger(__name__).error("Screener run failed: %s", exc)
        finally:
            db2.close()

    background_tasks.add_task(_run)
    return {"status": "screener_started", "message": "Screener running in background. Check /weekly-plan in ~2 minutes."}


@router.patch("/weekly-plan/{symbol}/status")
def update_plan_status(symbol: str, body: dict, db: Session = Depends(get_db)):
    """Mark a plan entry as EXECUTED, SKIPPED, etc."""
    status = body.get("status", "PENDING")
    if status not in ("PENDING", "EXECUTED", "PARTIAL", "SKIPPED"):
        from fastapi import HTTPException
        raise HTTPException(400, "Invalid status")
    db.execute(
        text("""
            UPDATE weekly_plan SET status = :s
            WHERE symbol = :sym
              AND week_start = (SELECT MAX(week_start) FROM weekly_plan)
        """),
        {"s": status, "sym": symbol.upper()},
    )
    db.commit()
    return {"symbol": symbol, "status": status}
