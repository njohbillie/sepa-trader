from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
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


def get_clock(mode: str = "paper"):
    return get_client(mode).get_clock()


def place_market_buy(symbol: str, qty: float, mode: str = "paper"):
    req = MarketOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
    )
    return get_client(mode).submit_order(req)


def place_market_sell(symbol: str, qty: float, mode: str = "paper"):
    req = MarketOrderRequest(
        symbol=symbol,
        qty=round(qty, 0),
        side=OrderSide.SELL,
        time_in_force=TimeInForce.DAY,
    )
    return get_client(mode).submit_order(req)


def close_position(symbol: str, mode: str = "paper"):
    return get_client(mode).close_position(symbol)
