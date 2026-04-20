from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest,
    GetOrdersRequest,
    StopLossRequest,
    TakeProfitRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus, OrderClass
from .config import settings

_clients: dict[str, TradingClient] = {}


def get_client(mode: str = "paper") -> TradingClient:
    if mode not in _clients:
        if mode == "paper":
            _clients[mode] = TradingClient(
                api_key=settings.alpaca_paper_key,
                secret_key=settings.alpaca_paper_secret,
                paper=True,
            )
        else:
            _clients[mode] = TradingClient(
                api_key=settings.alpaca_live_key,
                secret_key=settings.alpaca_live_secret,
                paper=False,
            )
    return _clients[mode]


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
    return get_client(mode).get_orders(GetOrdersRequest(status=QueryOrderStatus.ALL, limit=limit))


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
    - Entry leg: DAY (required by Alpaca for market bracket entries)
    - Stop and target legs: GTC — remain active until one fills or position is closed
    - When one exit leg fills, Alpaca automatically cancels the other
    Use this for NEW entries only. For existing positions use place_oca_exit().
    """
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


def place_oca_exit(
    symbol: str,
    qty: float,
    stop_price: float,
    target_price: float,
    mode: str = "paper",
):
    """
    Place stop-loss and take-profit SELL orders for an EXISTING open position.
    Used by the exit guard in run_monitor() to repair positions that are missing
    exit legs (e.g. bracket legs that expired due to the DAY TIF bug).

    Both orders are GTC and independent. When one fills, close_position() or
    the monitor's check_post_close() will cancel the orphaned leg automatically.

    Alpaca does not allow bracket legs to be attached post-entry, so this is
    the correct approach for existing positions.
    """
    client = get_client(mode)
    qty    = round(qty, 0)

    stop_req = StopOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
        stop_price=round(stop_price, 2),
    )

    limit_req = LimitOrderRequest(
        symbol=symbol,
        qty=qty,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
        limit_price=round(target_price, 2),
    )

    stop_order  = client.submit_order(stop_req)
    limit_order = client.submit_order(limit_req)
    return stop_order, limit_order


def close_position(symbol: str, mode: str = "paper"):
    """
    Flatten a position and cancel all open orders for the symbol.
    Alpaca's close_position() handles cancelling attached bracket/OCA legs automatically.
    """
    return get_client(mode).close_position(symbol)