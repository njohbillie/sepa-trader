from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db, get_setting, set_setting

router = APIRouter(prefix="/api/settings", tags=["settings"])

EDITABLE_KEYS = {"trading_mode", "auto_execute", "risk_pct", "stop_loss_pct", "max_positions", "watchlist"}


@router.get("")
def get_all(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT key, value FROM settings")).fetchall()
    return {r[0]: r[1] for r in rows}


class SettingUpdate(BaseModel):
    value: str


@router.patch("/{key}")
def update(key: str, body: SettingUpdate, db: Session = Depends(get_db)):
    if key not in EDITABLE_KEYS:
        from fastapi import HTTPException
        raise HTTPException(400, f"Key '{key}' is not editable")
    if key == "trading_mode" and body.value not in ("paper", "live"):
        from fastapi import HTTPException
        raise HTTPException(400, "trading_mode must be 'paper' or 'live'")
    set_setting(db, key, body.value)
    return {"key": key, "value": body.value}
