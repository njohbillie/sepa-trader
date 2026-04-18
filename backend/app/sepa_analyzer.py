import warnings
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")


def analyze(symbol: str) -> dict:
    """
    Returns a dict with keys: signal, score, price, ema20, ema50, ema150, ema200,
    week52_high, week52_low, near20, near50, above_pivot, vol_surge.
    """
    try:
        hist = yf.download(symbol, period="1y", interval="1d", progress=False, auto_adjust=True)
        if hist is None or len(hist) < 200:
            return {"signal": "INSUFFICIENT_DATA", "score": 0, "price": None}

        close = hist["Close"].squeeze()
        volume = hist["Volume"].squeeze()

        e20  = close.ewm(span=20,  adjust=False).mean()
        e50  = close.ewm(span=50,  adjust=False).mean()
        e150 = close.ewm(span=150, adjust=False).mean()
        e200 = close.ewm(span=200, adjust=False).mean()

        c       = float(close.iloc[-1])
        v_today = float(volume.iloc[-1])
        v_avg50 = float(volume.tail(50).mean())

        w52_high = float(close.tail(252).max())
        w52_low  = float(close.tail(252).min())

        score = sum([
            c > float(e50.iloc[-1]),
            c > float(e150.iloc[-1]),
            c > float(e200.iloc[-1]),
            float(e50.iloc[-1])  > float(e150.iloc[-1]),
            float(e150.iloc[-1]) > float(e200.iloc[-1]),
            float(e200.iloc[-1]) > float(e200.iloc[-22]),
            c >= w52_high * 0.75,
            c >= w52_low  * 1.30,
        ])

        base = {
            "score":    score,
            "price":    round(c, 4),
            "ema20":    round(float(e20.iloc[-1]), 4),
            "ema50":    round(float(e50.iloc[-1]), 4),
            "ema150":   round(float(e150.iloc[-1]), 4),
            "ema200":   round(float(e200.iloc[-1]), 4),
            "week52_high": round(w52_high, 4),
            "week52_low":  round(w52_low, 4),
            "vol_today":   int(v_today),
            "vol_avg50":   int(v_avg50),
        }

        if score < 7:
            return {**base, "signal": "NO_SETUP"}

        pivot_h  = float(close.tail(21).iloc[:-1].max())
        near20   = abs(c - float(e20.iloc[-1]))  / float(e20.iloc[-1])  * 100 <= 2.0
        near50   = abs(c - float(e50.iloc[-1]))  / float(e50.iloc[-1])  * 100 <= 3.0
        vol_surge = v_today > v_avg50 * 1.4

        if c > pivot_h and vol_surge:
            signal = "BREAKOUT"
        elif near20:
            signal = "PULLBACK_EMA20"
        elif near50:
            signal = "PULLBACK_EMA50"
        else:
            signal = "STAGE2_WATCH"

        return {**base, "signal": signal, "near20": near20, "near50": near50,
                "vol_surge": vol_surge, "above_pivot": c > pivot_h}

    except Exception as exc:
        return {"signal": f"ERROR", "error": str(exc), "score": 0, "price": None}


def batch_analyze(symbols: list[str]) -> dict[str, dict]:
    return {sym: analyze(sym) for sym in symbols}
