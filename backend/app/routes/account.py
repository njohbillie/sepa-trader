import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db, get_current_user, get_all_user_settings
from ..config import settings as global_settings
from .. import alpaca_client as alp

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/account", tags=["account"])


def _resolve_alpaca_client(user_settings: dict, mode: str, is_admin: bool = False):
    """
    Pick Alpaca credentials for a user.
    Admins fall back to .env global keys so their existing account just works.
    Regular users must configure their own credentials.
    """
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


def _fetch_account_data(client, name: str, mode: str) -> dict | None:
    """Fetch and normalise one Alpaca account. Returns None on any error."""
    try:
        acct       = client.get_account()
        equity     = float(acct.equity)
        last_eq    = float(acct.last_equity)
        day_pnl    = equity - last_eq

        # Unrealized P&L across all open positions (may not exist on all account types)
        try:
            unrealized_pl = float(acct.unrealized_pl) if acct.unrealized_pl is not None else 0.0
        except (AttributeError, TypeError):
            unrealized_pl = 0.0

        # Non-marginable buying power (cash-only portion)
        try:
            non_marginable_bp = float(acct.non_marginable_buying_power) \
                if acct.non_marginable_buying_power is not None else float(acct.buying_power)
        except (AttributeError, TypeError):
            non_marginable_bp = float(acct.buying_power)

        return {
            "name":               name,
            "mode":               mode,
            "portfolio_value":    float(acct.portfolio_value),
            "cash":               float(acct.cash),          # signed — negative when margin used
            "buying_power":       float(acct.buying_power),  # marginable BP
            "non_marginable_bp":  non_marginable_bp,
            "equity":             equity,
            "day_pnl":            day_pnl,
            "day_pnl_pct":        (day_pnl / last_eq * 100) if last_eq else 0,
            "unrealized_pl":      unrealized_pl,
        }
    except Exception as exc:
        logger.warning("_fetch_account_data(%s, %s): %s", name, mode, exc)
        return None


@router.get("/overview")
def accounts_overview(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Return all Alpaca accounts for this user, grouped by mode (paper / live).
    Includes the main account plus any strategy accounts that have dedicated
    API keys configured (currently: Dual Momentum).
    """
    uid          = current_user["id"]
    is_admin     = current_user["role"] == "admin"
    user_settings = get_all_user_settings(db, uid)

    # Strategy configs with potentially separate keys
    strategy_rows = db.execute(
        text("""
            SELECT strategy_name,
                   alpaca_paper_key, alpaca_paper_secret,
                   alpaca_live_key,  alpaca_live_secret
            FROM strategy_config
            WHERE user_id = :uid
        """),
        {"uid": uid},
    ).fetchall()

    # Pretty-print strategy names
    STRATEGY_LABELS = {
        "dual_momentum": "Dual Momentum",
    }

    result = {"paper": [], "live": []}

    for mode in ("paper", "live"):
        # ── Main account ──────────────────────────────────────────────────────
        try:
            main_client = _resolve_alpaca_client(user_settings, mode, is_admin)
            data = _fetch_account_data(main_client, "Main", mode)
            if data:
                result[mode].append(data)
        except HTTPException:
            pass  # credentials not configured for this mode

        # ── Strategy accounts (only if they have DEDICATED keys) ──────────────
        for row in strategy_rows:
            strat_name, pk, ps, lk, ls = row
            if mode == "paper":
                key, secret = pk, ps
            else:
                key, secret = lk, ls

            # Skip if no dedicated keys or same as main account credentials
            main_key = user_settings.get(f"alpaca_{mode}_key", "")
            if not key or key == main_key:
                continue

            try:
                client = alp.get_client_for_keys(key, secret, mode == "paper")
                label  = STRATEGY_LABELS.get(strat_name, strat_name.replace("_", " ").title())
                data   = _fetch_account_data(client, label, mode)
                if data:
                    result[mode].append(data)
            except Exception as exc:
                logger.warning("accounts_overview: strategy %s [%s]: %s", strat_name, mode, exc)

    return result


@router.get("")
def account(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    user_settings = get_all_user_settings(db, current_user["id"])
    mode = user_settings.get("trading_mode", "paper")
    client = _resolve_alpaca_client(user_settings, mode, is_admin=current_user["role"] == "admin")
    acct = client.get_account()
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
