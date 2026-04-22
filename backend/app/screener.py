"""
Sunday screener: scans a configurable universe of stocks using the Minervini
8-point SEPA criteria, selects top candidates, generates a weekly trading
plan, and saves it to the weekly_plan table.

Uses TradingView's scanner API — all symbols fetched in one batch request.
Live accounts automatically apply graduated conservative filters based on
account size — no manual settings changes needed as the account grows.
"""
import logging
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from .tv_analyzer import batch_analyze
from .database import get_setting, set_setting, get_user_setting, set_user_setting

logger = logging.getLogger(__name__)

# Default universe: top ~120 liquid US stocks across S&P 500 / NASDAQ 100.
# Users can override via the screener_universe setting (comma-separated).
DEFAULT_UNIVERSE = [
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO",
    # Semiconductors
    "AMD", "QCOM", "MU", "TXN", "KLAC", "LRCX", "AMAT", "MRVL", "ON", "MPWR",
    # Software / Cloud
    "CRM", "ADBE", "ORCL", "NOW", "INTU", "PANW", "CRWD", "SNOW", "DDOG", "ZS",
    "FTNT", "TEAM", "ANSS", "CDNS", "VEEV", "WDAY", "PCTY",
    # Financials
    "JPM", "BAC", "GS", "MS", "V", "MA", "WFC", "BX", "AXP", "SPGI",
    # Healthcare / Biotech
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "TMO", "ISRG", "REGN", "VRTX",
    "DXCM", "IDXX", "MRNA",
    # Consumer / Retail
    "COST", "WMT", "HD", "NKE", "MCD", "SBUX", "TJX", "LULU", "DECK", "ONON",
    # Energy
    "XOM", "CVX", "COP", "SLB",
    # Industrials
    "CAT", "DE", "HON", "LMT", "RTX", "GE", "UNP", "CSX",
    # Communications / Media
    "NFLX", "DIS", "CMCSA", "TMUS", "GOOGL",
    # High-growth / Breakout candidates
    "UBER", "ABNB", "MELI", "SHOP", "TTD", "ENPH", "FSLR", "CELH", "AXON",
    "SMCI", "APP", "PLTR", "HIMS",
]
# Deduplicate preserving order
DEFAULT_UNIVERSE = list(dict.fromkeys(DEFAULT_UNIVERSE))


def _next_monday() -> date:
    today      = date.today()
    days_ahead = (7 - today.weekday()) % 7 or 7
    return today + timedelta(days=days_ahead)


def _generate_rationale(symbol: str, result: dict) -> str:
    score  = result.get("score", 0)
    signal = result.get("signal", "")
    price  = result.get("price") or 0
    w52h   = result.get("week52_high") or 0
    w52l   = result.get("week52_low")  or 0

    parts = [f"Score {score}/8 — {signal}."]
    if price and w52h:
        pct_below = (w52h - price) / w52h * 100
        parts.append(f"${price:.2f}, {pct_below:.1f}% below 52w high.")
    if price and w52l:
        pct_above = (price - w52l) / w52l * 100
        parts.append(f"Up {pct_above:.1f}% from 52w low.")
    if result.get("vol_surge"):
        parts.append("Volume surge detected.")
    if result.get("above_pivot"):
        parts.append("Trading above 20-day pivot.")
    return " ".join(parts)


def run_screener(db: Session, mode: str = None, user_id: int = None) -> list[dict]:
    """
    Scan the stock universe, select top-N SEPA candidates, save to
    weekly_plan table, and update the watchlist setting.
    Returns a list of plan row dicts.

    Paper accounts use settings as configured — no overrides.
    Live accounts apply graduated limits from get_live_account_limits()
    which automatically unlock as the account grows across tier boundaries.
    """
    def _s(key, default=""):
        return get_user_setting(db, key, default, user_id)

    if mode is None:
        mode = _s("trading_mode", "paper")

    risk_pct         = float(_s("risk_pct",         "2.0"))
    stop_pct         = float(_s("stop_loss_pct",    "8.0"))
    max_position_pct = float(_s("max_position_pct", "20.0") or "20.0")

    # --- Screener filter settings ---
    price_min       = float(_s("screener_price_min",  "0") or "0")
    price_max       = float(_s("screener_price_max",  "0") or "0")
    top_n           = int(  _s("screener_top_n",      "0") or "0")
    if top_n <= 0:
        # Auto: target 80% deployment, one slot per max_position_pct.
        # e.g. 20% cap → 4 positions (80% deployed, 20% cash buffer)
        #      10% cap → 8 positions
        top_n = max(1, int(80.0 / max_position_pct))
        logger.info(
            "screener_top_n: auto → %d positions (80%% target / %.0f%% max position size)",
            top_n, max_position_pct,
        )
    min_score_floor = int(_s("screener_min_score", "0") or "0")
    vol_surge_pct   = float(_s("screener_vol_surge_pct", "40")  or "40")
    ema20_pct       = float(_s("screener_ema20_pct",     "2.0") or "2.0")
    ema50_pct       = float(_s("screener_ema50_pct",     "3.0") or "3.0")

    account_value = _get_portfolio_value(db, mode, user_id)
    if account_value <= 0:
        msg = "Screener aborted: could not fetch account value from Alpaca — no positions sized"
        _log_alert(db, "ERROR", msg)
        return []
    tier_label    = "PAPER"

    # --- Live account graduated overrides ---
    # Paper accounts skip this block entirely — no impact on paper trading.
    # Limits re-evaluated on every run, so crossing a tier boundary takes
    # effect automatically on the next screener execution.
    if mode == "live":
        from .database import get_live_account_limits
        limits     = get_live_account_limits(account_value)
        tier_label = limits.get("tier", "LIVE")

        logger.info(
            "Live account tier: %s (portfolio=$%.0f)",
            tier_label, account_value,
        )

        # Cap top_n at tier limit
        if limits.get("screener_top_n") is not None:
            configured_top_n = top_n
            top_n = min(top_n, limits["screener_top_n"])
            if top_n != configured_top_n:
                logger.info(
                    "Live [%s]: screener top_n capped at %d (settings=%d)",
                    tier_label, top_n, configured_top_n,
                )

        # Apply price floor only if user hasn't already set one
        lim_price_min = limits.get("screener_price_min") or 0
        if lim_price_min > 0 and price_min == 0:
            price_min = lim_price_min
            logger.info("Live [%s]: price_min set to $%.0f", tier_label, price_min)

        # Apply price ceiling only if user hasn't already set one
        lim_price_max = limits.get("screener_price_max") or 0
        if lim_price_max > 0 and price_max == 0:
            price_max = lim_price_max
            logger.info("Live [%s]: price_max set to $%.0f", tier_label, price_max)

        # Raise min_score_floor to tier minimum if not already higher
        floor_from_limits = limits.get("min_score_floor", 0)
        if floor_from_limits > min_score_floor:
            logger.info(
                "Live [%s]: min_score_floor raised from %d to %d",
                tier_label, min_score_floor, floor_from_limits,
            )
            min_score_floor = floor_from_limits

    universe_raw = _s("screener_universe", "")
    universe = (
        [s.strip().upper() for s in universe_raw.split(",") if s.strip()]
        if universe_raw
        else DEFAULT_UNIVERSE
    )

    logger.info(
        "Screener: scanning %d symbols via TradingView (mode=%s, tier=%s, account=$%.0f)...",
        len(universe), mode, tier_label, account_value,
    )

    # Single batch call — all symbols in one TradingView scanner request
    results_map = batch_analyze(
        universe,
        vol_surge_pct=vol_surge_pct,
        ema20_pct=ema20_pct,
        ema50_pct=ema50_pct,
    )

    # Build scored list; apply price filter
    all_scored = []
    for sym, result in results_map.items():
        if not result.get("price") or result.get("signal") in ("ERROR", "INSUFFICIENT_DATA"):
            continue
        price = float(result["price"])
        if price_min > 0 and price < price_min:
            continue
        if price_max > 0 and price > price_max:
            continue
        all_scored.append({"symbol": sym, **result})

    all_scored.sort(
        key=lambda x: (x["score"], int(bool(x.get("vol_surge"))), int(bool(x.get("above_pivot")))),
        reverse=True,
    )

    errors    = sum(1 for r in results_map.values() if r.get("signal") in ("ERROR", "INSUFFICIENT_DATA"))
    top_score = all_scored[0]["score"] if all_scored else 0

    # Adaptive threshold — if user/tier set a floor, respect it.
    # Otherwise step down until 5+ candidates found.
    if min_score_floor > 0:
        candidates = [c for c in all_scored if c["score"] >= min_score_floor]
    else:
        candidates = []
        for min_score in (6, 5, 4, 3):
            candidates = [c for c in all_scored if c["score"] >= min_score]
            if len(candidates) >= 5:
                break

    top_picks = candidates[:top_n] if candidates else []

    effective_min = min_score_floor if min_score_floor > 0 else (
        next((s for s in (6, 5, 4, 3) if len([c for c in all_scored if c["score"] >= s]) >= 5), 3)
    )

    summary_msg = (
        f"Screener ({mode}/{tier_label}): scanned {len(universe)}, "
        f"errors {errors}, "
        f"scored {len(all_scored)}, "
        f"qualifying (>={effective_min}) {len(candidates)}, "
        f"selected {len(top_picks)}. "
        f"Top score: {top_score}/8."
    )
    if price_min > 0 or price_max > 0:
        if price_max > 0:
            summary_msg += f" Price filter: ${price_min:.0f}–${price_max:.0f}."
        else:
            summary_msg += f" Price min: ${price_min:.0f}."

    # Surface sample errors if error rate is high
    if errors > len(universe) * 0.5:
        sample_errors = [
            f"{sym}: {r['error']}"
            for sym, r in list(results_map.items())[:3]
            if r.get("signal") == "ERROR" and r.get("error")
        ]
        if sample_errors:
            summary_msg += " Sample errors: " + " | ".join(sample_errors)

    logger.info(summary_msg)
    _log_alert(db, "INFO", summary_msg)

    week_start   = _next_monday()
    risk_dollars = account_value * (risk_pct / 100)
    plan_rows    = []

    for rank, c in enumerate(top_picks, 1):
        price    = float(c["price"])
        stop     = round(price * (1 - stop_pct / 100), 4)
        target1  = round(price * (1 + stop_pct * 2 / 100), 4)
        target2  = round(price * (1 + stop_pct * 3 / 100), 4)
        stop_d            = price - stop
        risk_based_shares = int(risk_dollars / stop_d) if stop_d > 0 else 0
        max_value_shares  = int((account_value * max_position_pct / 100) / price) if price > 0 else 0
        shares            = min(risk_based_shares, max_value_shares)
        if risk_based_shares > max_value_shares and max_value_shares > 0:
            logger.info(
                "Position cap applied for %s: risk-based=%d shares ($%.0f) capped to %d shares ($%.0f, %.0f%% of account)",
                c["symbol"], risk_based_shares, risk_based_shares * price,
                max_value_shares, max_value_shares * price, max_position_pct,
            )
        risk_amt = round(shares * stop_d, 2)

        plan_rows.append({
            "week_start":    week_start.isoformat(),
            "symbol":        c["symbol"],
            "rank":          rank,
            "score":         c["score"],
            "signal":        c.get("signal", "STAGE2_WATCH"),
            "entry_price":   price,
            "stop_price":    stop,
            "target1":       target1,
            "target2":       target2,
            "position_size": shares,
            "risk_amount":   risk_amt,
            "rationale":     _generate_rationale(c["symbol"], c),
            "status":        "PENDING",
            "mode":          mode,
            "screener_type": "minervini",
        })

    # Always save (even empty) so last-run info is queryable
    _save_plan(db, plan_rows, week_start.isoformat(), mode, user_id)
    if user_id:
        set_user_setting(db, "screener_last_run", summary_msg, user_id)
    else:
        set_setting(db, "screener_last_run", summary_msg)

    if plan_rows:
        top_symbols = [r["symbol"] for r in plan_rows]
        if user_id:
            set_user_setting(db, "watchlist", ",".join(top_symbols), user_id)
        else:
            set_setting(db, "watchlist", ",".join(top_symbols))

        tv_user = _s("tv_username", "")
        tv_pass = _s("tv_password", "")
        if tv_user and tv_pass:
            from .tradingview_client import update_weekly_picks
            tv_result = update_weekly_picks(tv_user, tv_pass, top_symbols)
            if tv_result["ok"]:
                logger.info("TradingView weekly_picks %s.", tv_result["action"])
            else:
                logger.warning("TradingView sync failed: %s", tv_result["error"])

    logger.info(
        "Screener complete. Week of %s. Tier: %s. Plan: %s",
        week_start, tier_label, [r["symbol"] for r in plan_rows],
    )
    return plan_rows


def _log_alert(db: Session, level: str, message: str):
    try:
        db.execute(
            text("INSERT INTO alert_log (level, message) VALUES (:l, :m)"),
            {"l": level, "m": message},
        )
        db.commit()
    except Exception:
        pass


def _get_portfolio_value(db: Session, mode: str, user_id: int = None) -> float:
    try:
        from . import alpaca_client as alp
        from .config import settings as global_settings

        if user_id:
            from .database import get_user_setting as _gus
            is_admin = db.execute(
                text("SELECT role FROM users WHERE id = :id"), {"id": user_id}
            ).scalar() == "admin"
            if mode == "paper":
                key    = _gus(db, "alpaca_paper_key",    "", user_id)
                secret = _gus(db, "alpaca_paper_secret", "", user_id)
                if is_admin:
                    key    = key    or global_settings.alpaca_paper_key
                    secret = secret or global_settings.alpaca_paper_secret
                paper = True
            else:
                key    = _gus(db, "alpaca_live_key",    "", user_id)
                secret = _gus(db, "alpaca_live_secret", "", user_id)
                if is_admin:
                    key    = key    or global_settings.alpaca_live_key
                    secret = secret or global_settings.alpaca_live_secret
                paper = False
            if key and secret:
                client = alp.get_client_for_keys(key, secret, paper)
                return float(client.get_account().portfolio_value)

        acct = alp.get_account(mode)
        return float(acct.portfolio_value)
    except Exception:
        logger.warning(
            "_get_portfolio_value: could not reach Alpaca — screener aborted to avoid mis-sized positions"
        )
        return 0.0


def _save_plan(db: Session, rows: list[dict], week_start: str, mode: str, user_id: int = None):
    db.execute(
        text("DELETE FROM weekly_plan WHERE week_start = :w AND mode = :m AND user_id IS NOT DISTINCT FROM :uid"),
        {"w": week_start, "m": mode, "uid": user_id},
    )
    for r in rows:
        row = {**r, "user_id": user_id}
        row.setdefault("screener_type", "minervini")
        db.execute(
            text("""
                INSERT INTO weekly_plan
                    (week_start, symbol, rank, score, signal, entry_price, stop_price,
                     target1, target2, position_size, risk_amount, rationale, status,
                     mode, user_id, screener_type)
                VALUES (:week_start, :symbol, :rank, :score, :signal, :entry_price, :stop_price,
                        :target1, :target2, :position_size, :risk_amount, :rationale, :status,
                        :mode, :user_id, :screener_type)
            """),
            row,
        )
    db.commit()


def run_both_screeners(
    db: Session,
    mode: str = None,
    user_id: int = None,
    _phase_cb=None,
) -> list[dict]:
    """
    Run Minervini + Pullback-to-MA screeners, merge and deduplicate results.

    Dedup rule: if a symbol appears in both screeners, keep the first occurrence
    (Minervini rank wins) and tag it screener_type='both'.
    Re-ranks the combined list 1..N.
    Saves the merged plan to weekly_plan.
    """
    from .pullback_screener import run_pullback_screener

    def _phase(msg):
        logger.info("Screener phase: %s", msg)
        if _phase_cb:
            try:
                _phase_cb(msg)
            except Exception:
                pass

    if mode is None:
        from .database import get_user_setting as _gus
        mode = _gus(db, "trading_mode", "paper", user_id)

    logger.info("Running both screeners (mode=%s, user=%s)…", mode, user_id)

    _phase("Minervini: scanning universe via TradingView…")
    min_rows = run_screener(db, mode=mode, user_id=user_id)   # saves its own plan
    _phase(f"Minervini done — {len(min_rows)} candidates. Running Pullback screener…")
    pb_rows  = run_pullback_screener(db, mode=mode, user_id=user_id)
    _phase(f"Pullback done — {len(pb_rows)} candidates. Merging results…")

    # Merge: Minervini first, then Pullback, dedup by symbol
    seen: dict[str, dict] = {}
    for r in min_rows:
        seen[r["symbol"]] = r
    for r in pb_rows:
        if r["symbol"] in seen:
            seen[r["symbol"]]["screener_type"] = "both"   # overlap
        else:
            seen[r["symbol"]] = r

    merged = list(seen.values())

    # Re-rank
    for i, row in enumerate(merged, 1):
        row["rank"] = i

    week_start = merged[0]["week_start"] if merged else _next_monday().isoformat()
    _save_plan(db, merged, week_start, mode, user_id)

    logger.info(
        "Both screeners done: %d minervini + %d pullback = %d unique (mode=%s)",
        len(min_rows), len(pb_rows), len(merged), mode,
    )
    return merged