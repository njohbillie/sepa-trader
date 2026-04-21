from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
from ..database import get_db, get_setting
from ..trader import _log_signal, _log_trade, _size_position, _gate, _get_weekly_plan_exits
from .. import alpaca_client as alp
from .. import telegram_alerts as tg
import asyncio

router = APIRouter(prefix="/api/webhook", tags=["webhook"])


class TVAlert(BaseModel):
    symbol: str
    signal: str          # BREAKOUT | PULLBACK_EMA20 | PULLBACK_EMA50 | STAGE2_WATCH | NO_SETUP
    price: float
    volume: float = 0
    score: int = 0
    secret: str = ""


@router.post("/tradingview")
async def tradingview(alert: TVAlert, db: Session = Depends(get_db)):
    # Validate webhook secret
    webhook_secret = get_setting(db, "webhook_secret", "")
    if webhook_secret and alert.secret != webhook_secret:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    mode         = get_setting(db, "trading_mode", "paper")
    auto_execute = get_setting(db, "auto_execute", "true").lower() == "true"
    risk_pct     = float(get_setting(db, "risk_pct", "2.0"))
    stop_pct     = float(get_setting(db, "stop_loss_pct", "8.0"))

    symbol = alert.symbol.upper().replace("NASDAQ:", "").replace("NYSE:", "").replace("AMEX:", "")
    signal = alert.signal.upper()

    # Log the incoming signal
    _log_signal(db, symbol, signal, alert.score, alert.price, mode)

    action_taken = None

    try:
        clock = alp.get_clock(mode)
        market_open = clock.is_open
    except Exception:
        market_open = False

    if auto_execute and market_open:
        positions   = {p.symbol: p for p in alp.get_positions(mode)}
        acct        = alp.get_account(mode)
        portfolio   = float(acct.portfolio_value)
        max_pos     = int(get_setting(db, "max_positions", "10"))

        if signal == "BREAKOUT" and symbol not in positions:
            if len(positions) < max_pos:
                stop, target = _get_weekly_plan_exits(db, symbol, mode)
                qty = _size_position(portfolio, alert.price, risk_pct, stop_pct, stop_price=stop)
                if qty >= 1:
                    if not _gate(db, symbol, qty, alert.price, stop, target, "TV_BREAKOUT", mode):
                        action_taken = "BLOCKED_BY_AI"
                    else:
                        try:
                            alp.place_market_buy(symbol, qty, mode)
                            _log_trade(db, symbol, "BUY", qty, alert.price, "TV_BREAKOUT", mode)
                            action_taken = f"BUY {qty} shares"
                            asyncio.create_task(tg.alert_trade("BUY", symbol, qty, alert.price, "TV_BREAKOUT", mode))
                        except Exception as e:
                            action_taken = f"BUY_FAILED: {e}"

        elif signal == "NO_SETUP" and symbol in positions:
            try:
                qty = float(positions[symbol].qty)
                alp.close_position(symbol, mode)
                _log_trade(db, symbol, "SELL", qty, alert.price, "TV_STAGE2_LOST", mode)
                action_taken = f"SELL {qty} shares"
                asyncio.create_task(tg.alert_trade("SELL", symbol, qty, alert.price, "TV_STAGE2_LOST", mode))
            except Exception as e:
                action_taken = f"SELL_FAILED: {e}"
    else:
        action_taken = "market_closed" if not market_open else "auto_execute_off"

    # Telegram notification for all signals
    asyncio.create_task(tg.send(
        f"*TradingView Alert* [{mode.upper()}]\n\n"
        f"Symbol: `{symbol}`\nSignal: `{signal}`\nPrice: `${alert.price:.2f}`\n"
        f"Score: `{alert.score}/8`\nAction: `{action_taken or 'none'}`",
        level="OPPORTUNITY" if signal == "BREAKOUT" else "URGENT" if signal == "NO_SETUP" else "INFO"
    ))

    return {
        "status":       "received",
        "symbol":       symbol,
        "signal":       signal,
        "action_taken": action_taken,
        "mode":         mode,
    }
