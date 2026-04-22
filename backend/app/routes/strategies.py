"""
Strategy routes — Dual Momentum, market environment, AI strategist.

Endpoints
─────────
GET  /api/strategies/market-environment           current market regime
GET  /api/strategies/dual-momentum/signal         latest saved signal
POST /api/strategies/dual-momentum/evaluate       run GEM + AI assess + save
POST /api/strategies/dual-momentum/execute        execute the latest signal on Alpaca
GET  /api/strategies/dual-momentum/history        signal history (last 24)
GET  /api/strategies/dual-momentum/position       current Alpaca position
GET  /api/strategies/dual-momentum/config         per-user strategy config
PATCH /api/strategies/dual-momentum/config        update config
"""
import json
import logging
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database import get_db, get_current_user, get_all_user_settings
from ..config import settings as global_settings
from .. import alpaca_client as alp
from ..utils import sf as _sf

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/strategies", tags=["strategies"])

STRATEGY_DM = "dual_momentum"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_strategy_config(db: Session, user_id: int, strategy_name: str) -> dict:
    row = db.execute(
        text("SELECT * FROM strategy_config WHERE user_id = :uid AND strategy_name = :name"),
        {"uid": user_id, "name": strategy_name},
    ).fetchone()
    if not row:
        return {
            "strategy_name":       strategy_name,
            "is_active":           False,
            "auto_execute":        False,
            "trading_mode":        "paper",
            "alpaca_paper_key":    "",
            "alpaca_paper_secret": "",
            "alpaca_live_key":     "",
            "alpaca_live_secret":  "",
            "settings":            {},
        }
    d = dict(row._mapping)
    if isinstance(d.get("settings"), str):
        d["settings"] = json.loads(d["settings"])
    # Mask secrets in response
    for k in ("alpaca_paper_key", "alpaca_paper_secret", "alpaca_live_key", "alpaca_live_secret"):
        v = d.get(k) or ""
        d[k] = ("•" * 8 + v[-4:]) if len(v) > 4 else ("•" * len(v))
    return d


def _resolve_strategy_alpaca_client(db: Session, user_id: int, strategy_name: str,
                                     mode: str, is_admin: bool):
    """
    Credential priority: strategy-specific keys → user default keys → .env (admin only).
    """
    row = db.execute(
        text("SELECT alpaca_paper_key, alpaca_paper_secret, alpaca_live_key, alpaca_live_secret "
             "FROM strategy_config WHERE user_id = :uid AND strategy_name = :name"),
        {"uid": user_id, "name": strategy_name},
    ).fetchone()

    user_settings = get_all_user_settings(db, user_id)

    if mode == "paper":
        key    = (row and row[0]) or user_settings.get("alpaca_paper_key", "")
        secret = (row and row[1]) or user_settings.get("alpaca_paper_secret", "")
        if is_admin:
            key    = key    or global_settings.alpaca_paper_key
            secret = secret or global_settings.alpaca_paper_secret
        paper = True
    else:
        key    = (row and row[2]) or user_settings.get("alpaca_live_key", "")
        secret = (row and row[3]) or user_settings.get("alpaca_live_secret", "")
        if is_admin:
            key    = key    or global_settings.alpaca_live_key
            secret = secret or global_settings.alpaca_live_secret
        paper = False

    if not key or not secret:
        raise HTTPException(400, "alpaca_credentials_missing")
    return alp.get_client_for_keys(key, secret, paper)


def _dm_has_dedicated_keys(db: Session, user_id: int, mode: str) -> bool:
    """
    Returns True only when strategy_config contains non-empty dedicated Alpaca keys
    for the given mode.  DM must never share the main Minervini account.
    """
    row = db.execute(
        text("SELECT alpaca_paper_key, alpaca_paper_secret, alpaca_live_key, alpaca_live_secret "
             "FROM strategy_config WHERE user_id = :uid AND strategy_name = :name"),
        {"uid": user_id, "name": STRATEGY_DM},
    ).fetchone()
    if not row:
        return False
    if mode == "paper":
        return bool(row[0] and row[1])
    return bool(row[2] and row[3])


def _save_signal(db: Session, user_id: int, strategy_name: str,
                 signal: dict, ai_decision: dict, mode: str):
    dm_sig = signal.get("recommended_symbol")
    db.execute(
        text("""
            INSERT INTO strategy_signals
                (user_id, strategy_name, recommended_symbol, action,
                 data, reasoning, ai_verdict, ai_reasoning, mode)
            VALUES
                (:uid, :name, :sym, :action,
                 CAST(:data AS jsonb), :reasoning, :ai_verdict, :ai_reasoning, :mode)
        """),
        {
            "uid":         user_id,
            "name":        strategy_name,
            "sym":         dm_sig,
            "action":      ai_decision.get("decision", "WAIT"),
            "data":        json.dumps(signal),
            "reasoning":   signal.get("reasoning", ""),
            "ai_verdict":  ai_decision.get("decision"),
            "ai_reasoning": ai_decision.get("reasoning", ""),
            "mode":        mode,
        },
    )
    db.commit()


# ── Market environment ────────────────────────────────────────────────────────

@router.get("/market-environment")
def get_market_environment(_: dict = Depends(get_current_user)):
    """Current market regime assessment (SPY trend + VIX)."""
    from ..strategies.market_env import assess
    return assess()


# ── Dual Momentum ─────────────────────────────────────────────────────────────

@router.get("/dual-momentum/signal")
def get_latest_signal(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return the most recent saved Dual Momentum signal for the user's current global mode."""
    uid           = current_user["id"]
    user_settings = get_all_user_settings(db, uid)
    mode          = user_settings.get("trading_mode", "paper")

    row = db.execute(
        text("""
            SELECT id, recommended_symbol, action, data, reasoning,
                   ai_verdict, ai_reasoning, mode, executed, created_at
            FROM strategy_signals
            WHERE user_id = :uid AND strategy_name = :name AND mode = :mode
            ORDER BY created_at DESC
            LIMIT 1
        """),
        {"uid": uid, "name": STRATEGY_DM, "mode": mode},
    ).fetchone()
    if not row:
        return None
    d = dict(row._mapping)
    if isinstance(d.get("data"), str):
        d["data"] = json.loads(d["data"])
    return d


@router.post("/dual-momentum/evaluate")
def evaluate_dual_momentum(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Run the GEM algorithm + market environment + AI strategist.
    Saves the result. If auto_execute is on, fires the trade in background.
    """
    uid          = current_user["id"]
    is_admin     = current_user["role"] == "admin"
    user_settings = get_all_user_settings(db, uid)
    # Always follow the global trading mode — DM has no separate mode
    mode         = user_settings.get("trading_mode", "paper")
    cfg          = db.execute(
        text("SELECT auto_execute FROM strategy_config "
             "WHERE user_id = :uid AND strategy_name = :name"),
        {"uid": uid, "name": STRATEGY_DM},
    ).fetchone()
    auto_execute = cfg[0] if cfg else False

    # Dual Momentum must use a dedicated Alpaca account — never the Minervini account
    if not _dm_has_dedicated_keys(db, uid, mode):
        raise HTTPException(
            400,
            "dm_dedicated_keys_required: Set dedicated Alpaca keys for Dual Momentum "
            "in Strategy Settings. Sharing the Minervini account is not allowed.",
        )

    from ..strategies.dual_momentum import evaluate as dm_evaluate
    from ..strategies.market_env    import assess    as env_assess
    from ..strategies.ai_strategist import decide    as ai_decide

    # 1 — Run GEM
    signal = dm_evaluate()

    # 2 — Market environment
    market_env = env_assess()

    # 3 — Current Alpaca position (best-effort)
    portfolio: dict = {}
    try:
        client   = _resolve_strategy_alpaca_client(db, uid, STRATEGY_DM, mode, is_admin)
        positions = client.get_all_positions()
        portfolio = {
            p.symbol: {
                "qty":           _sf(p.qty, 0.0),
                "unrealized_pl": _sf(p.unrealized_pl, 0.0),
            }
            for p in positions
        }
    except Exception as exc:
        logger.warning("evaluate: could not fetch positions: %s", exc)

    # 4 — AI decision
    ai_decision = ai_decide(
        db           = db,
        market_env   = market_env,
        strategy_signals = [{
            "strategy_name":      STRATEGY_DM,
            "recommended_symbol": signal["recommended_symbol"],
            "action":             "BUY",
            "reasoning":          signal["reasoning"],
        }],
        portfolio    = portfolio,
        user_id      = uid,
    )

    # 5 — Persist signal
    _save_signal(db, uid, STRATEGY_DM, signal, ai_decision, mode)

    # 6 — Auto-execute if configured
    if auto_execute and ai_decision["decision"] == "EXECUTE":
        background_tasks.add_task(
            _execute_signal_bg, uid, STRATEGY_DM, signal["recommended_symbol"], mode
        )

    return {
        "signal":      signal,
        "market_env":  market_env,
        "ai_decision": ai_decision,
        "auto_execute": auto_execute,
    }


@router.post("/dual-momentum/execute")
def execute_dual_momentum(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Manually execute the current Dual Momentum signal."""
    uid           = current_user["id"]
    user_settings = get_all_user_settings(db, uid)
    # Always follow the global trading mode — never the per-strategy mode
    mode          = user_settings.get("trading_mode", "paper")

    row = db.execute(
        text("""
            SELECT recommended_symbol, id
            FROM strategy_signals
            WHERE user_id = :uid AND strategy_name = :name
            ORDER BY created_at DESC LIMIT 1
        """),
        {"uid": uid, "name": STRATEGY_DM},
    ).fetchone()
    if not row:
        raise HTTPException(404, "No signal found — run evaluation first")

    symbol, sig_id = row

    # Enforce dedicated keys — no sharing with Minervini account
    if not _dm_has_dedicated_keys(db, uid, mode):
        raise HTTPException(
            400,
            "dm_dedicated_keys_required: Set dedicated Alpaca keys for Dual Momentum "
            "in Strategy Settings. Sharing the Minervini account is not allowed.",
        )

    _execute_signal(db, uid, STRATEGY_DM, symbol, mode)

    db.execute(
        text("UPDATE strategy_signals SET executed = true WHERE id = :id"),
        {"id": sig_id},
    )
    db.commit()
    return {"status": "executed", "symbol": symbol, "mode": mode}


@router.get("/dual-momentum/position")
def get_dm_position(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Current Alpaca position for the Dual Momentum strategy account."""
    uid           = current_user["id"]
    is_admin      = current_user["role"] == "admin"
    user_settings = get_all_user_settings(db, uid)
    # Always follow the global trading mode
    mode          = user_settings.get("trading_mode", "paper")

    try:
        client    = _resolve_strategy_alpaca_client(db, uid, STRATEGY_DM, mode, is_admin)
        positions = client.get_all_positions()
        return [
            {
                "symbol":          p.symbol,
                "qty":             _sf(p.qty, 0.0),
                "entry_price":     _sf(p.avg_entry_price, 0.0),
                "current_price":   _sf(p.current_price, 0.0),
                "market_value":    _sf(p.market_value, 0.0),
                "unrealized_pl":   _sf(p.unrealized_pl, 0.0),
                "unrealized_plpc": (_sf(p.unrealized_plpc, 0.0) or 0.0) * 100,
            }
            for p in positions
            if p.symbol in ("SPY", "EFA", "AGG", "BIL")
        ]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Alpaca error: {str(exc)[:200]}")


@router.get("/dual-momentum/history")
def get_dm_history(
    limit: int = 24,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return signal history filtered to the user's current global mode."""
    uid           = current_user["id"]
    user_settings = get_all_user_settings(db, uid)
    mode          = user_settings.get("trading_mode", "paper")

    rows = db.execute(
        text("""
            SELECT id, recommended_symbol, action, reasoning,
                   ai_verdict, ai_reasoning, mode, executed, created_at
            FROM strategy_signals
            WHERE user_id = :uid AND strategy_name = :name AND mode = :mode
            ORDER BY created_at DESC
            LIMIT :l
        """),
        {"uid": uid, "name": STRATEGY_DM, "l": limit, "mode": mode},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


# ── Config ────────────────────────────────────────────────────────────────────

@router.get("/dual-momentum/config")
def get_dm_config(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    return _get_strategy_config(db, current_user["id"], STRATEGY_DM)


class DmConfigUpdate(BaseModel):
    is_active:               bool  | None = None
    auto_execute:            bool  | None = None
    trading_mode:            str   | None = None
    alpaca_paper_key:        str   | None = None
    alpaca_paper_secret:     str   | None = None
    alpaca_live_key:         str   | None = None
    alpaca_live_secret:      str   | None = None
    lookback_months:         int   | None = None
    eval_day:                int   | None = None
    eval_frequency:          str   | None = None   # monthly | biweekly | weekly
    vix_threshold:           float | None = None   # circuit breaker: VIX level
    spy_drawdown_threshold:  float | None = None   # circuit breaker: % drop from 20d high


@router.patch("/dual-momentum/config")
def update_dm_config(
    body: DmConfigUpdate,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    uid = current_user["id"]
    if body.trading_mode and body.trading_mode not in ("paper", "live"):
        raise HTTPException(400, "trading_mode must be 'paper' or 'live'")

    # Upsert row
    db.execute(
        text("""
            INSERT INTO strategy_config (user_id, strategy_name) VALUES (:uid, :name)
            ON CONFLICT (user_id, strategy_name) DO NOTHING
        """),
        {"uid": uid, "name": STRATEGY_DM},
    )

    updates = []
    params  = {"uid": uid, "name": STRATEGY_DM}

    for field in ("is_active", "auto_execute", "trading_mode",
                  "alpaca_paper_key", "alpaca_paper_secret",
                  "alpaca_live_key",  "alpaca_live_secret"):
        val = getattr(body, field)
        # Skip masked placeholder values
        if val is not None and not (isinstance(val, str) and val.startswith("•")):
            updates.append(f"{field} = :{field}")
            params[field] = val

    extra_settings = {}
    if body.lookback_months is not None:
        extra_settings["lookback_months"] = body.lookback_months

    from ..database import set_user_setting as _sus
    if body.eval_day is not None:
        v = max(1, min(28, body.eval_day))
        extra_settings["eval_day"] = v
        _sus(db, "dm_eval_day", str(v), uid)
    if body.eval_frequency is not None:
        if body.eval_frequency in ("monthly", "biweekly", "weekly"):
            extra_settings["eval_frequency"] = body.eval_frequency
            _sus(db, "dm_eval_frequency", body.eval_frequency, uid)
    if body.vix_threshold is not None:
        v = max(15.0, min(80.0, body.vix_threshold))
        extra_settings["vix_threshold"] = v
        _sus(db, "dm_vix_threshold", str(v), uid)
    if body.spy_drawdown_threshold is not None:
        v = max(3.0, min(30.0, body.spy_drawdown_threshold))
        extra_settings["spy_drawdown_threshold"] = v
        _sus(db, "dm_spy_drawdown_threshold", str(v), uid)

    if extra_settings:
        updates.append("settings = settings || CAST(:extra AS jsonb)")
        params["extra"] = json.dumps(extra_settings)

    if updates:
        db.execute(
            text(f"UPDATE strategy_config SET {', '.join(updates)} "
                 f"WHERE user_id = :uid AND strategy_name = :name"),
            params,
        )
        db.commit()

    return {"status": "updated"}


# ── Execution helpers ─────────────────────────────────────────────────────────

def _execute_signal(db: Session, user_id: int, strategy_name: str,
                    symbol: str, mode: str):
    """
    Close any existing strategy position and open the new one.
    Uses full-account-value sizing (single-instrument rotation strategy).
    """
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums    import OrderSide, TimeInForce

    is_admin = db.execute(
        text("SELECT role FROM users WHERE id = :id"), {"id": user_id}
    ).scalar() == "admin"
    client = _resolve_strategy_alpaca_client(db, user_id, strategy_name, mode, is_admin)

    # Close all current positions in SPY/EFA/AGG/BIL that aren't the target
    positions = client.get_all_positions()
    for p in positions:
        if p.symbol in ("SPY", "EFA", "AGG", "BIL") and p.symbol != symbol:
            try:
                client.close_position(p.symbol)
                logger.info("execute_signal: closed %s [%s]", p.symbol, mode)
            except Exception as exc:
                logger.warning("execute_signal: could not close %s: %s", p.symbol, exc)

    # Check if already holding the target
    already_holding = any(p.symbol == symbol for p in positions)
    if already_holding:
        logger.info("execute_signal: already holding %s — no action needed", symbol)
        return

    # Size: use all available buying power (rotation strategy — 100% allocation)
    account = client.get_account()
    bp      = _sf(account.buying_power, 0.0)
    # Get current price for the target via direct Yahoo Finance API
    from ..strategies.yf_client import get_current_price
    price = get_current_price(symbol)
    qty   = int(bp * 0.98 / price)  # 98% — leave a small buffer

    if price <= 0:
        raise HTTPException(502, f"Could not fetch current price for {symbol}")
    if qty < 1:
        logger.warning("execute_signal: insufficient buying power for %s (bp=$%.0f, price=$%.2f)",
                       symbol, bp, price)
        return

    req = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
    )
    client.submit_order(req)
    logger.info("execute_signal: bought %d shares of %s [%s]", qty, symbol, mode)


def _execute_signal_bg(user_id: int, strategy_name: str, symbol: str, mode: str):
    """Background wrapper for auto-execute."""
    from ..database import SessionLocal
    db = SessionLocal()
    try:
        _execute_signal(db, user_id, strategy_name, symbol, mode)
        # Mark latest signal as executed (subquery required — PostgreSQL has no UPDATE...ORDER BY)
        db.execute(
            text("""
                UPDATE strategy_signals SET executed = true
                WHERE id = (
                    SELECT id FROM strategy_signals
                    WHERE user_id = :uid AND strategy_name = :name
                    ORDER BY created_at DESC
                    LIMIT 1
                )
            """),
            {"uid": user_id, "name": strategy_name},
        )
        db.commit()
    except Exception as exc:
        logger.error("_execute_signal_bg failed: %s", exc)
    finally:
        db.close()
