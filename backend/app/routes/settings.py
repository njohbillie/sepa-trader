from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..database import (
    get_db, get_current_user,
    get_all_user_settings, set_user_setting,
)

router = APIRouter(prefix="/api/settings", tags=["settings"])

EDITABLE_KEYS = {
    "trading_mode", "auto_execute", "monitor_enabled",
    "risk_pct", "stop_loss_pct", "max_positions", "max_position_pct",
    "watchlist", "webhook_secret",
    "screener_universe",
    "screener_price_min", "screener_price_max", "screener_top_n",
    "screener_min_score", "screener_vol_surge_pct", "screener_ema20_pct", "screener_ema50_pct",
    "screener_auto_run",
    "screener_schedule_day", "screener_schedule_time",       # legacy (single day/time)
    "screener_schedule_days", "screener_schedule_times",     # Minervini multi-day / multi-time
    "pb_screener_auto_run",
    "pb_screener_schedule_days", "pb_screener_schedule_times",  # Pullback schedule
    "tv_username", "tv_password",
    # AI provider settings
    "ai_provider", "ai_api_key", "ai_model", "ai_base_url",
    "alpaca_paper_key", "alpaca_paper_secret",
    "alpaca_live_key",  "alpaca_live_secret",
    # Pullback screener settings
    "pb_price_min", "pb_price_max",
    "pb_rsi_min", "pb_rsi_max",
    "pb_avg_vol_min", "pb_rel_vol_min",
    "pb_market_cap_min", "pb_week_change_min",
    "pb_ema50_proximity", "pb_beta_max",
    "pb_earnings_days_min", "pb_top_n",
    "pb_price_above_ema20", "pb_ema20_above_ema50",
    "pb_ema50_above_ema100", "pb_ema100_above_ema200",
    "pb_ppst_required", "pb_ppst_pivot_period", "pb_ppst_period", "pb_ppst_multiplier",
    "pb_tv_screener_name",
    "pb_block_unknown_earnings",
    "pb_exchanges",
    "pb_ema_spread_min", "pb_adx_min", "pb_52w_high_pct_max", "pb_3m_perf_min",
    "tv_chart_layout_id",
    "pb_ai_chart_review", "pb_ai_chart_min_grade",
    "mv_entry_order_type", "mv_entry_slippage_pct",
    "pb_entry_order_type", "pb_entry_slippage_pct",
}


@router.get("")
def get_all(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """Return merged settings: global defaults overlaid with user-specific overrides."""
    return get_all_user_settings(db, current_user["id"])


class SettingUpdate(BaseModel):
    value: str


@router.patch("/{key}")
def update(
    key: str,
    body: SettingUpdate,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if key not in EDITABLE_KEYS:
        raise HTTPException(400, f"Key '{key}' is not editable")
    if key == "trading_mode" and body.value not in ("paper", "live"):
        raise HTTPException(400, "trading_mode must be 'paper' or 'live'")
    set_user_setting(db, key, body.value, current_user["id"])
    return {"key": key, "value": body.value}
