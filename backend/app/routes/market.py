"""
Market tape check endpoints.

GET  /api/market/tape-check           — return today's cached verdict (compute if missing)
DELETE /api/market/tape-check         — clear today's cache and recompute
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..database import get_db
from ..auth import get_current_user
from ..market_analysis import get_tape_check

router = APIRouter(prefix="/market", tags=["market"])


@router.get("/tape-check")
def tape_check(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Return today's market tape verdict (cached per user per day)."""
    return get_tape_check(db, user_id=current_user["id"])


@router.delete("/tape-check")
def tape_check_refresh(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Clear today's cache and recompute the tape verdict."""
    return get_tape_check(db, user_id=current_user["id"], force_refresh=True)
