import httpx
from .config import settings


async def send(message: str, level: str = "INFO") -> bool:
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return False

    emoji = {"URGENT": "🚨", "OPPORTUNITY": "🟢", "INFO": "ℹ️"}.get(level, "📊")
    text  = f"{emoji} *SEPA Monitor*\n\n{message}"

    url  = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    data = {"chat_id": settings.telegram_chat_id, "text": text, "parse_mode": "Markdown"}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json=data)
            return r.status_code == 200
    except Exception:
        return False


async def alert_stage2_lost(symbols: list[str], mode: str):
    syms = ", ".join(symbols)
    await send(
        f"*STAGE 2 LOST* [{mode.upper()}]\n\nSymbols: `{syms}`\n\nPositions closed automatically.",
        level="URGENT",
    )


async def alert_breakout(symbols: list[str], mode: str):
    syms = ", ".join(symbols)
    await send(
        f"*BREAKOUT DETECTED* [{mode.upper()}]\n\nSymbols: `{syms}`\n\nPositions sized in automatically.",
        level="OPPORTUNITY",
    )


async def alert_trade(action: str, symbol: str, qty: float, price: float, trigger: str, mode: str):
    await send(
        f"*TRADE EXECUTED* [{mode.upper()}]\n\n"
        f"Action: `{action}`\nSymbol: `{symbol}`\nQty: `{qty}`\nPrice: `${price:.2f}`\nTrigger: `{trigger}`",
        level="INFO",
    )


async def alert_monitor_summary(portfolio: float, day_pnl: float, positions: int, mode: str, interval_minutes: int = 30):
    pnl_sign = "+" if day_pnl >= 0 else ""
    if interval_minutes < 60:
        freq = f"{interval_minutes}-min Check"
    elif interval_minutes == 60:
        freq = "Hourly Check"
    else:
        freq = f"{interval_minutes // 60}h Check"
    await send(
        f"*{freq}* [{mode.upper()}]\n\n"
        f"Portfolio: `${portfolio:,.2f}`\nDay P&L: `{pnl_sign}${day_pnl:,.2f}`\nPositions: `{positions}`",
        level="INFO",
    )
