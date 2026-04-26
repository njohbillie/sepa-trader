from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopLimitOrderRequest,
    GetOrdersRequest,
    StopLossRequest,
    TakeProfitRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus, OrderClass
from .config import settings
import logging
import time

logger = logging.getLogger(__name__)

_clients: dict[str, TradingClient] = {}


def get_client(mode: str = "paper") -> TradingClient:
    """Global client using .env credentials — used by scheduler/background jobs only."""
    if mode not in _clients:
        if mode == "paper":
            _clients[mode] = TradingClient(
                api_key=(settings.alpaca_paper_key or "").strip(),
                secret_key=(settings.alpaca_paper_secret or "").strip(),
                paper=True,
            )
        else:
            _clients[mode] = TradingClient(
                api_key=(settings.alpaca_live_key or "").strip(),
                secret_key=(settings.alpaca_live_secret or "").strip(),
                paper=False,
            )
    return _clients[mode]


def get_client_for_keys(api_key: str, secret_key: str, paper: bool) -> TradingClient:
    """Create a TradingClient from explicit credentials (per-user API requests).
    Strips surrounding whitespace so copy-paste artefacts don't cause 401s.
    """
    return TradingClient(
        api_key=api_key.strip(),
        secret_key=secret_key.strip(),
        paper=paper,
    )


def configure_from_db_settings(merged: dict, mode: str, is_admin: bool = True) -> None:
    """
    Update the global cached client for `mode` using credentials from the merged
    user+global settings dict (as returned by get_all_user_settings).

    For admin users, falls back to .env credentials when DB credentials are absent
    — the same logic used by the account route's _resolve_alpaca_client.

    Call this at the start of run_monitor so the live client uses DB-stored keys
    (saved via Settings panel) rather than the .env-file keys which may be empty.
    """
    if mode == "paper":
        key    = (merged.get("alpaca_paper_key") or "").strip()
        secret = (merged.get("alpaca_paper_secret") or "").strip()
        if is_admin:
            key    = key    or (settings.alpaca_paper_key or "").strip()
            secret = secret or (settings.alpaca_paper_secret or "").strip()
        paper = True
    else:
        key    = (merged.get("alpaca_live_key") or "").strip()
        secret = (merged.get("alpaca_live_secret") or "").strip()
        if is_admin:
            key    = key    or (settings.alpaca_live_key or "").strip()
            secret = secret or (settings.alpaca_live_secret or "").strip()
        paper = False

    if not key or not secret:
        raise ValueError(f"No Alpaca credentials configured for {mode} mode")

    logger.info("configure_from_db_settings: updating %s client (credentials set)", mode)
    _clients[mode] = TradingClient(api_key=key, secret_key=secret, paper=paper)


def get_account(mode: str = "paper"):
    return get_client(mode).get_account()


def get_positions(mode: str = "paper"):
    return get_client(mode).get_all_positions()


def get_open_orders(mode: str = "paper"):
    return get_client(mode).get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))


def get_open_orders_by_symbol(mode: str = "paper") -> dict[str, list]:
    """Return all open orders keyed by symbol for quick lookup."""
    orders = get_client(mode).get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
    result: dict[str, list] = {}
    for o in orders:
        result.setdefault(o.symbol, []).append(o)
    return result


def get_all_orders(mode: str = "paper", limit: int = 100):
    return get_client(mode).get_orders(
        GetOrdersRequest(status=QueryOrderStatus.ALL, limit=limit)
    )


def find_recent_fill(mode: str, symbol: str, side: str, days: int = 30):
    """Return the most-recent FILLED order for (symbol, side) within `days`,
    or None if none found. Used to reconstruct SELL fills that were executed
    by Alpaca-side bracket OCOs (stop / take-profit) which the bot never
    submitted itself and therefore never logged.
    """
    from datetime import datetime, timedelta, timezone
    side_enum = OrderSide.SELL if side.upper() == "SELL" else OrderSide.BUY
    after = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        orders = get_client(mode).get_orders(
            GetOrdersRequest(
                status=QueryOrderStatus.CLOSED,
                symbols=[symbol],
                side=side_enum,
                after=after,
                limit=100,
            )
        )
    except Exception:
        return None
    fills = [o for o in (orders or []) if getattr(o, "status", None) and str(o.status).lower().endswith("filled") and getattr(o, "filled_at", None)]
    if not fills:
        return None
    fills.sort(key=lambda o: o.filled_at, reverse=True)
    return fills[0]


def get_clock(mode: str = "paper"):
    return get_client(mode).get_clock()


def place_market_buy(symbol: str, qty: float, mode: str = "paper"):
    """Simple market buy with no exit legs. GTC so it survives past market close."""
    req = MarketOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.GTC,
    )
    return get_client(mode).submit_order(req)


def place_limit_buy(symbol: str, qty: float, limit_price: float, mode: str = "paper"):
    """DAY limit buy with no exit legs. Cancels automatically if not filled today."""
    req = LimitOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        limit_price=round(limit_price, 2),
    )
    return get_client(mode).submit_order(req)


def place_market_sell(symbol: str, qty: float, mode: str = "paper"):
    """Simple market sell. GTC so it survives past market close."""
    req = MarketOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
    )
    return get_client(mode).submit_order(req)


def place_bracket_buy(
    symbol: str,
    qty: float,
    stop_price: float,
    target_price: float,
    mode: str = "paper",
):
    """
    Market buy with attached stop-loss and take-profit legs (bracket/OCA).
    Entry leg: DAY (required by Alpaca for market bracket entries).
    Stop and target legs: GTC — remain active until one fills or position is closed.
    Use this for NEW entries only. For existing positions use place_oca_exit().
    """
    if qty <= 0 or stop_price <= 0 or target_price <= 0:
        raise ValueError(f"place_bracket_buy {symbol}: invalid qty/stop/target ({qty}/{stop_price}/{target_price})")
    if target_price <= stop_price:
        raise ValueError(f"place_bracket_buy {symbol}: target ${target_price:.2f} must exceed stop ${stop_price:.2f}")
    req = MarketOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        order_class=OrderClass.BRACKET,
        stop_loss=StopLossRequest(stop_price=round(stop_price, 2)),
        take_profit=TakeProfitRequest(limit_price=round(target_price, 2)),
    )
    return get_client(mode).submit_order(req)


def place_limit_bracket_buy(
    symbol: str,
    qty: float,
    entry_price: float,
    stop_price: float,
    target_price: float,
    slippage_pct: float = 0.5,
    mode: str = "paper",
):
    """
    DAY limit buy with attached stop-loss and take-profit bracket.
    Entry fills only up to entry_price × (1 + slippage_pct/100).
    If not filled by end of day, Alpaca cancels automatically.
    Use for pullback-to-MA entries where price is already near the target level.
    """
    if qty <= 0 or entry_price <= 0 or stop_price <= 0 or target_price <= 0:
        raise ValueError(
            f"place_limit_bracket_buy {symbol}: invalid qty/entry/stop/target "
            f"({qty}/{entry_price}/{stop_price}/{target_price})"
        )
    if stop_price >= entry_price:
        raise ValueError(f"place_limit_bracket_buy {symbol}: stop ${stop_price:.2f} must be below entry ${entry_price:.2f}")
    if target_price <= entry_price:
        raise ValueError(f"place_limit_bracket_buy {symbol}: target ${target_price:.2f} must exceed entry ${entry_price:.2f}")
    limit_price = round(entry_price * (1 + slippage_pct / 100), 2)
    req = LimitOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        limit_price=limit_price,
        order_class=OrderClass.BRACKET,
        stop_loss=StopLossRequest(stop_price=round(stop_price, 2)),
        take_profit=TakeProfitRequest(limit_price=round(target_price, 2)),
    )
    return get_client(mode).submit_order(req)


def place_stop_limit_buy(
    symbol: str,
    qty: float,
    stop_price: float,
    slippage_pct: float = 1.0,
    mode: str = "paper",
):
    """
    DAY stop-limit buy — for breakout entries.
    Activates only when stock trades at or above stop_price (confirms the breakout),
    then fills up to stop_price × (1 + slippage_pct/100).
    Alpaca does not support brackets on stop-limit entries; the monitor will add
    OCO exits on the next cycle after the entry fills.
    """
    limit_price = round(stop_price * (1 + slippage_pct / 100), 2)
    req = StopLimitOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
        stop_price=round(stop_price, 2),
        limit_price=limit_price,
    )
    return get_client(mode).submit_order(req)


def place_oca_exit(
    symbol: str,
    qty: float,
    stop_price: float,
    target_price: float,
    mode: str = "paper",
):
    """
    Place a single OCO (One-Cancels-Other) sell order for an existing position.
    When one leg fills, Alpaca automatically cancels the other.

    Both stop_loss and take_profit must be passed explicitly as request objects —
    Alpaca raises code 40010001 if take_profit.limit_price is missing even when
    limit_price is set on the parent LimitOrderRequest.
    """
    req = LimitOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
        limit_price=round(target_price, 2),
        order_class=OrderClass.OCO,
        stop_loss=StopLossRequest(stop_price=round(stop_price, 2)),
        take_profit=TakeProfitRequest(limit_price=round(target_price, 2)),  # ← required by Alpaca
    )
    return get_client(mode).submit_order(req)


def place_split_bracket_buy(
    symbol: str,
    qty: float,
    stop_price: float,
    t1_price: float,
    t2_price: float,
    mode: str = "paper",
):
    """
    Place two market bracket orders for a split-lot T1/T2 exit strategy.
    Lot 1: qty//2 shares — stop=stop_price, take-profit=t1_price.
    Lot 2: remaining shares — stop=stop_price, take-profit=t2_price.
    Both stop legs are identical so trailing stop logic treats them uniformly.
    Raises ValueError if qty < 2 (can't split into two 1-share lots).
    """
    if stop_price <= 0 or t1_price <= 0 or t2_price <= 0:
        raise ValueError(f"place_split_bracket_buy {symbol}: invalid stop/t1/t2 ({stop_price}/{t1_price}/{t2_price})")
    if t1_price <= stop_price or t2_price <= stop_price:
        raise ValueError(f"place_split_bracket_buy {symbol}: targets must exceed stop ${stop_price:.2f}")
    qty_int = int(round(qty))
    qty1    = qty_int // 2
    qty2    = qty_int - qty1
    if qty1 < 1 or qty2 < 1:
        raise ValueError(f"qty {qty_int} too small to split into T1/T2 lots (min 2 shares)")
    client = get_client(mode)
    o1 = client.submit_order(MarketOrderRequest(
        symbol=symbol, qty=qty1, side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY, order_class=OrderClass.BRACKET,
        stop_loss=StopLossRequest(stop_price=round(stop_price, 2)),
        take_profit=TakeProfitRequest(limit_price=round(t1_price, 2)),
    ))
    o2 = client.submit_order(MarketOrderRequest(
        symbol=symbol, qty=qty2, side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY, order_class=OrderClass.BRACKET,
        stop_loss=StopLossRequest(stop_price=round(stop_price, 2)),
        take_profit=TakeProfitRequest(limit_price=round(t2_price, 2)),
    ))
    return o1, o2


def place_split_limit_bracket_buy(
    symbol: str,
    qty: float,
    entry_price: float,
    stop_price: float,
    t1_price: float,
    t2_price: float,
    slippage_pct: float = 0.5,
    mode: str = "paper",
):
    """
    Place two DAY limit bracket orders for a split-lot T1/T2 exit strategy.
    Raises ValueError if qty < 2.
    """
    if entry_price <= 0 or stop_price <= 0 or t1_price <= 0 or t2_price <= 0:
        raise ValueError(
            f"place_split_limit_bracket_buy {symbol}: invalid entry/stop/t1/t2 "
            f"({entry_price}/{stop_price}/{t1_price}/{t2_price})"
        )
    if stop_price >= entry_price:
        raise ValueError(f"place_split_limit_bracket_buy {symbol}: stop ${stop_price:.2f} must be below entry ${entry_price:.2f}")
    if t1_price <= entry_price or t2_price <= entry_price:
        raise ValueError(f"place_split_limit_bracket_buy {symbol}: targets must exceed entry ${entry_price:.2f}")
    qty_int     = int(round(qty))
    qty1        = qty_int // 2
    qty2        = qty_int - qty1
    if qty1 < 1 or qty2 < 1:
        raise ValueError(f"qty {qty_int} too small to split into T1/T2 lots (min 2 shares)")
    limit_price = round(entry_price * (1 + slippage_pct / 100), 2)
    client      = get_client(mode)
    o1 = client.submit_order(LimitOrderRequest(
        symbol=symbol, qty=qty1, side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY, limit_price=limit_price,
        order_class=OrderClass.BRACKET,
        stop_loss=StopLossRequest(stop_price=round(stop_price, 2)),
        take_profit=TakeProfitRequest(limit_price=round(t1_price, 2)),
    ))
    o2 = client.submit_order(LimitOrderRequest(
        symbol=symbol, qty=qty2, side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY, limit_price=limit_price,
        order_class=OrderClass.BRACKET,
        stop_loss=StopLossRequest(stop_price=round(stop_price, 2)),
        take_profit=TakeProfitRequest(limit_price=round(t2_price, 2)),
    ))
    return o1, o2


def replace_split_oca_exits(
    symbol: str,
    qty1: float,
    qty2: float,
    new_stop: float,
    t1_price: float,
    t2_price: float,
    mode: str = "paper",
):
    """
    Cancel all exit orders for a split-lot position and re-place two OCOs
    with an updated stop (trailing stop update). T1 and T2 targets are preserved.
    """
    cancelled = cancel_symbol_exit_orders(symbol, mode)
    if cancelled:
        cleared = wait_for_orders_cancelled(symbol, mode, timeout=6.0, poll_interval=0.4)
        if not cleared:
            logger.warning("replace_split_oca_exits: cancellation timeout for %s", symbol)
    place_oca_exit(symbol, qty1, new_stop, t1_price, mode)
    try:
        place_oca_exit(symbol, qty2, new_stop, t2_price, mode)
    except Exception as exc:
        # Second leg failed — qty2 is now naked (qty1 has its OCO, qty2 does not).
        # Retry once before raising so a transient API blip doesn't leave the
        # position partially unhedged.
        logger.error("replace_split_oca_exits: second leg failed for %s: %s — retrying", symbol, exc)
        time.sleep(0.5)
        try:
            place_oca_exit(symbol, qty2, new_stop, t2_price, mode)
        except Exception as exc2:
            logger.error(
                "replace_split_oca_exits: second leg retry failed for %s: %s — qty2 NAKED",
                symbol, exc2,
            )
            try:
                from . import telegram_alerts as tg
                tg.alert_system_error_sync(
                    f"NAKED LEG [{mode}] {symbol} qty2={qty2} — split-OCO second leg failed twice",
                    exc2, level="URGENT",
                )
            except Exception:
                pass
            raise


def cancel_symbol_exit_orders(symbol: str, mode: str = "paper") -> list[str]:
    """
    Cancel all open sell orders for a symbol (OCO, bracket, or standalone).
    Returns list of cancelled order IDs.
    """
    client      = get_client(mode)
    open_orders = get_open_orders_by_symbol(mode)
    cancelled   = []

    for o in open_orders.get(symbol, []):
        side = str(getattr(o, 'side', '') or '').lower()
        if 'sell' in side:
            try:
                client.cancel_order_by_id(str(o.id))
                cancelled.append(str(o.id))
                logger.debug("Cancelled exit order %s for %s [%s]", o.id, symbol, mode)
            except Exception as exc:
                logger.warning("Could not cancel order %s for %s: %s", o.id, symbol, exc)

    return cancelled


def wait_for_orders_cancelled(
    symbol: str,
    mode: str = "paper",
    timeout: float = 15.0,
    poll_interval: float = 0.4,
) -> bool:
    """
    Poll until no open sell orders remain for a symbol, or timeout elapses.
    Returns True if orders are cleared, False if timeout hit.
    Used after cancel_symbol_exit_orders() to ensure Alpaca has fully
    processed the cancellation before placing a replacement OCO.
    """
    elapsed = 0.0
    while elapsed < timeout:
        open_orders   = get_open_orders_by_symbol(mode)
        symbol_orders = open_orders.get(symbol, [])
        sell_orders   = [
            o for o in symbol_orders
            if 'sell' in str(getattr(o, 'side', '') or '').lower()
        ]
        if not sell_orders:
            return True
        time.sleep(poll_interval)
        elapsed += poll_interval

    logger.warning(
        "wait_for_orders_cancelled: timeout after %.1fs — sell orders still open for %s",
        timeout, symbol,
    )
    return False


def replace_oca_exit(
    symbol: str,
    qty: float,
    new_stop: float,
    target_price: float,
    mode: str = "paper",
):
    """
    Cancel existing exit orders for a symbol and place a fresh OCO with
    updated stop/target prices. Used by the trailing stop logic and the
    exit guard when plan prices change.

    Polls until cancellation is confirmed before placing the new order —
    avoids Alpaca rejecting the replacement as an oversell.
    """
    cancelled = cancel_symbol_exit_orders(symbol, mode)
    if cancelled:
        cleared = wait_for_orders_cancelled(symbol, mode, timeout=6.0, poll_interval=0.4)
        if not cleared:
            logger.warning(
                "replace_oca_exit: proceeding despite timeout — "
                "cancellation may not be fully settled for %s", symbol,
            )

    # CRITICAL: cancel succeeded; if the replacement place fails, the position
    # is naked until the next monitor cycle. Retry the placement once before
    # giving up so transient API errors don't strand a position.
    last_exc = None
    for attempt in (1, 2):
        try:
            return place_oca_exit(symbol, qty, new_stop, target_price, mode)
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "replace_oca_exit: place attempt %d failed for %s: %s",
                attempt, symbol, exc,
            )
            time.sleep(0.5)
    # Both attempts failed — surface to caller so it can re-raise/alert. The
    # caller's existing `except` block catches and fires the NAKED POSITION
    # telegram alert.
    raise last_exc


def close_position(symbol: str, mode: str = "paper"):
    """
    Flatten a position and cancel all open orders for the symbol.
    Alpaca's close_position() handles cancelling attached bracket/OCA legs automatically.
    """
    return get_client(mode).close_position(symbol)