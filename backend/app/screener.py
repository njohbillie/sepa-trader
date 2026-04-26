"""
Sunday screener: scans a configurable universe of stocks using the Minervini
8-point SEPA criteria, selects top candidates, generates a weekly trading
plan, and saves it to the weekly_plan table.

Uses TradingView's scanner API — all symbols fetched in one batch request.
Live accounts automatically apply graduated conservative filters based on
account size — no manual settings changes needed as the account grows.
"""
import logging
import statistics
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

import httpx

from .tv_analyzer import batch_analyze
from .database import get_setting, set_setting, get_user_setting, set_user_setting
from . import telegram_alerts as tg

logger = logging.getLogger(__name__)

# ── Market-wide universe scan ────────────────────────────────────────────────
#
# Replaces the hardcoded DEFAULT_UNIVERSE with a server-side TradingView scan
# constrained to SEPA Stage-2 trend candidates. Returns up to ~300 symbols
# pre-filtered by liquidity, price, exchange, sector, and trend ladder so the
# downstream batch_analyze() doesn't waste calls on names that can't possibly
# score well.

_TV_SCAN_URL = "https://scanner.tradingview.com/america/scan"
_TV_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin":  "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
}


def _fetch_market_universe(
    price_min: float,
    price_max: float,
    excluded_sectors: set[str],
    exchanges: list[str] | None = None,
    max_results: int = 300,
) -> list[str]:
    """Server-side TradingView scan for SEPA Stage-2 candidates.

    Returns a list of plain symbols (no exchange prefix). Empty list on failure
    so the caller can fall back to DEFAULT_UNIVERSE without crashing the run.

    Filters enforced server-side:
      • Price ≥ max(price_min, $5) — avoid penny-stock noise even if user lets
        price_min default to 0
      • Optional price ceiling
      • 30-day average volume ≥ 500k shares
      • Market cap ≥ $300M (small-cap floor; lets mid-caps and up through)
      • Exchange in {NYSE, NASDAQ} (or user override)
      • Stage-2 trend ladder: close > SMA50 > SMA150 > SMA200
      • Within 30% of 52-week high (Stage-2 requirement)

    Sector exclusion is applied locally on the returned rows (TV doesn't
    expose a clean negative-set operator across all sector strings).
    """
    floor_price = max(5.0, price_min or 0)
    filters: list[dict] = [
        {"left": "close", "operation": "egreater", "right": floor_price},
        {"left": "average_volume_30d_calc", "operation": "egreater", "right": 500_000},
        {"left": "market_cap_basic", "operation": "egreater", "right": 300_000_000},
        {"left": "exchange", "operation": "in_range",
         "right": exchanges or ["NYSE", "NASDAQ"]},
        # Stage-2 trend ladder (Minervini's templates 1, 2, 3, 4 in one go)
        {"left": "close",  "operation": "greater", "right": "SMA50"},
        {"left": "SMA50",  "operation": "greater", "right": "SMA150"},
        {"left": "SMA150", "operation": "greater", "right": "SMA200"},
    ]
    if price_max and price_max > 0:
        filters.append({"left": "close", "operation": "eless", "right": price_max})

    columns = ["close", "sector", "average_volume_30d_calc", "market_cap_basic"]

    try:
        resp = httpx.post(
            _TV_SCAN_URL,
            json={
                "filter":  filters,
                "columns": columns,
                "range":   [0, max_results],
                "sort":    {"sortBy": "market_cap_basic", "sortOrder": "desc"},
                "markets": ["america"],
            },
            timeout=30,
            headers=_TV_HEADERS,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.error("Market-wide TV scan failed: %s", exc)
        return []

    rows = (payload or {}).get("data") or []
    if not rows:
        logger.warning("Market-wide TV scan returned 0 rows — check filter set or TV availability")
        return []

    # Local sector exclusion. `sector` column is at index 1.
    out: list[str] = []
    skipped_sector = 0
    for row in rows:
        try:
            full_sym = row["s"]                  # e.g. "NASDAQ:AAPL"
            sector   = (row["d"][1] or "").strip()
        except (KeyError, IndexError, TypeError):
            continue
        if excluded_sectors and sector in excluded_sectors:
            skipped_sector += 1
            continue
        sym = full_sym.split(":")[-1]
        out.append(sym)

    if skipped_sector:
        logger.info(
            "Market-wide scan: %d rows from TV, %d skipped by sector filter, %d kept",
            len(rows), skipped_sector, len(out),
        )
    else:
        logger.info("Market-wide scan: %d candidates from TV", len(out))
    return out

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


def run_screener(db: Session, mode: str = None, user_id: int = None, account_value: float = None) -> list[dict]:
    """
    Scan the stock universe, select top-N SEPA candidates, save to
    weekly_plan table, and update the watchlist setting.
    Returns a list of plan row dicts.

    Paper accounts use settings as configured — no overrides.
    Live accounts apply graduated limits from get_live_account_limits()
    which automatically unlock as the account grows across tier boundaries.

    account_value: pass explicitly when calling from run_both_screeners to
    avoid a second Alpaca API call (the value was already fetched).
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

    # Sector exclusion — Minervini-specific. Falls back to the legacy
    # `screener_excluded_sectors` key so existing settings keep working.
    from .rs_screener import _resolve_excluded as _rs_resolve
    _excluded_csv = _s("mv_excluded_sectors", "") or _s("screener_excluded_sectors", "")
    _excluded_raw = [s.strip() for s in _excluded_csv.split(",") if s.strip()]
    excluded_sectors = _rs_resolve(_excluded_raw) if _excluded_raw else set()

    if account_value is None:
        try:
            account_value = _get_portfolio_value(db, mode, user_id)
        except RuntimeError as exc:
            msg = f"Screener aborted: {exc}"
            _log_alert(db, "ERROR", msg)
            raise
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

    # Universe selection priority:
    #   1. Explicit `screener_universe` CSV override (testing / watchlist mode)
    #   2. Market-wide TV scan (default — picks up real Stage-2 leaders fresh
    #      every run instead of re-scoring the same hardcoded mega-caps)
    #   3. Hardcoded DEFAULT_UNIVERSE as last-resort fallback if the TV scan
    #      returns nothing (network blip, schema drift)
    universe_raw = _s("screener_universe", "")
    if universe_raw:
        universe = [s.strip().upper() for s in universe_raw.split(",") if s.strip()]
        universe_source = "override"
    else:
        scanned = _fetch_market_universe(
            price_min=price_min,
            price_max=price_max,
            excluded_sectors=excluded_sectors,
        )
        if scanned:
            universe = scanned
            universe_source = "market_scan"
        else:
            universe = DEFAULT_UNIVERSE
            universe_source = "fallback_default"
            logger.warning(
                "Market-wide scan returned nothing — falling back to DEFAULT_UNIVERSE (%d names)",
                len(universe),
            )

    logger.info(
        "Screener: scanning %d symbols via TradingView (source=%s, mode=%s, tier=%s, account=$%.0f)...",
        len(universe), universe_source, mode, tier_label, account_value,
    )

    # Single batch call — all symbols in one TradingView scanner request
    results_map = batch_analyze(
        universe,
        vol_surge_pct=vol_surge_pct,
        ema20_pct=ema20_pct,
        ema50_pct=ema50_pct,
    )

    # Build scored list; apply price + sector filters
    all_scored = []
    for sym, result in results_map.items():
        if not result.get("price") or result.get("signal") in ("ERROR", "INSUFFICIENT_DATA"):
            continue
        price = float(result["price"])
        if price_min > 0 and price < price_min:
            continue
        if price_max > 0 and price > price_max:
            continue
        if excluded_sectors:
            sector = (result.get("sector") or "").strip().lower()
            if not sector:
                logger.debug("Minervini screener: %s has no sector — skipping (exclusion list active).", sym)
                continue
            if sector in excluded_sectors:
                logger.debug("Minervini screener: %s excluded (sector=%s).", sym, sector)
                continue
        all_scored.append({"symbol": sym, **result})

    all_scored.sort(
        key=lambda x: (x["score"], int(bool(x.get("vol_surge"))), int(bool(x.get("above_pivot")))),
        reverse=True,
    )

    errors    = sum(1 for r in results_map.values() if r.get("signal") in ("ERROR", "INSUFFICIENT_DATA"))
    top_score = all_scored[0]["score"] if all_scored else 0

    # Data-quality guard: if the median score across the whole universe is too
    # low, TV data is likely degraded (or the market is in a deep correction
    # where no SEPA setups exist). Either way, walking the floor down to 3
    # would emit garbage picks. Abort with empty plan + alert instead.
    median_score = statistics.median(c["score"] for c in all_scored) if all_scored else 0
    DEGRADED_MEDIAN_THRESHOLD = 4
    degraded = bool(all_scored) and median_score < DEGRADED_MEDIAN_THRESHOLD

    if degraded:
        msg = (
            f"Minervini screener ({mode}/{tier_label}): aborting — median score "
            f"{median_score:.1f} below {DEGRADED_MEDIAN_THRESHOLD} across "
            f"{len(all_scored)} scored / {len(universe)} universe "
            f"(errors {errors}). Likely degraded TV data or deep correction; "
            f"emitting empty plan instead of low-quality picks."
        )
        logger.warning(msg)
        _log_alert(db, "WARN", msg)
        try:
            tg.alert_system_error_sync(
                f"minervini_screener:{mode}",
                f"Aborted: median score {median_score:.1f} (<{DEGRADED_MEDIAN_THRESHOLD}). "
                f"scored={len(all_scored)}, errors={errors}/{len(universe)}.",
                level="WARNING",
            )
        except Exception:
            pass
        candidates = []
    elif min_score_floor > 0:
        # Adaptive threshold — if user/tier set a floor, respect it.
        candidates = [c for c in all_scored if c["score"] >= min_score_floor]
    else:
        # Otherwise step down until 5+ candidates found.
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
    """Fetch account portfolio value from Alpaca.
    Raises RuntimeError with the real Alpaca error on failure so callers can
    surface the actual cause to the user instead of a generic message.
    """
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
        if not key or not secret:
            raise RuntimeError(
                f"No Alpaca {mode} credentials configured — "
                f"add alpaca_{mode}_key and alpaca_{mode}_secret in Settings → Alpaca."
            )
        try:
            client = alp.get_client_for_keys(key, secret, paper)
            return float(client.get_account().portfolio_value)
        except Exception as exc:
            logger.error(
                "_get_portfolio_value: Alpaca %s call failed (user=%s): %r",
                mode, user_id, exc, exc_info=True,
            )
            raise RuntimeError(
                f"Alpaca {mode} API error: {exc}"
            ) from exc

    try:
        acct = alp.get_account(mode)
        return float(acct.portfolio_value)
    except Exception as exc:
        logger.error(
            "_get_portfolio_value: Alpaca %s call failed (global creds): %r",
            mode, exc, exc_info=True,
        )
        raise RuntimeError(f"Alpaca {mode} API error: {exc}") from exc


def _save_plan(db: Session, rows: list[dict], week_start: str, mode: str, user_id: int = None):
    # Preserve EXECUTED status for symbols already bought — don't reset them to PENDING
    # when the screener re-runs mid-week (e.g. a manual run).
    already_executed = {
        row[0] for row in db.execute(
            text("""
                SELECT symbol FROM weekly_plan
                WHERE week_start = :w AND mode = :m
                  AND user_id IS NOT DISTINCT FROM :uid
                  AND status = 'EXECUTED'
            """),
            {"w": week_start, "m": mode, "uid": user_id},
        ).fetchall()
    }

    # Also treat anything bought today (in trade_log) as executed
    try:
        bought_today = {r[0] for r in db.execute(
            text("""SELECT DISTINCT symbol FROM trade_log
                    WHERE action = 'BUY' AND mode = :mode
                    AND created_at >= CURRENT_DATE"""),
            {"mode": mode},
        ).fetchall()}
        already_executed |= bought_today
    except Exception:
        pass

    db.execute(
        text("DELETE FROM weekly_plan WHERE week_start = :w AND mode = :m AND user_id IS NOT DISTINCT FROM :uid"),
        {"w": week_start, "m": mode, "uid": user_id},
    )
    for r in rows:
        row = {**r, "user_id": user_id}
        row.setdefault("screener_type", "minervini")
        # Keep EXECUTED status for symbols already bought this week
        if row.get("symbol") in already_executed:
            row["status"] = "EXECUTED"
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
    Run all three screeners (Minervini, Pullback-to-MA, RS Momentum), merge and
    deduplicate results.

    Priority on overlap: Minervini > Pullback > RS Momentum.
    Overlapping symbols are tagged screener_type='both'.
    Re-ranks the combined list 1..N.
    Saves the merged plan to weekly_plan.

    RS screener can be disabled via the rs_screener_enabled setting.
    """
    from .pullback_screener import run_pullback_screener
    from .rs_screener import run_rs_screener, fetch_rs_universe, get_rs_settings

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

    logger.info("Running all screeners (mode=%s, user=%s)…", mode, user_id)

    from .database import get_user_setting as _gus2

    try:
        av = _get_portfolio_value(db, mode, user_id)
    except RuntimeError as exc:
        raise RuntimeError(
            f"Cannot reach Alpaca ({mode} mode) — {exc}. All screeners aborted."
        ) from exc
    if av == 0:
        logger.warning(
            "Account value is $0 for mode=%s — live account may be unfunded. "
            "All picks will show 0 shares; position sizing will be correct once funded.",
            mode,
        )
    logger.info("Account value for screener run: $%.0f (mode=%s)", av, mode)

    _phase("Minervini: scanning universe via TradingView…")
    min_rows = run_screener(db, mode=mode, user_id=user_id, account_value=av)

    _phase(f"Minervini done — {len(min_rows)} candidates. Running Pullback screener…")
    pb_rows  = run_pullback_screener(db, mode=mode, user_id=user_id, account_value=av)

    rs_enabled = _gus2(db, "rs_screener_enabled", "true", user_id).lower() == "true"
    score_map: dict[str, float] = {}
    rs_tv_data: dict[str, dict] = {}
    rs_rows: list[dict] = []
    if rs_enabled:
        _phase(f"Pullback done — {len(pb_rows)} candidates. Fetching global RS universe…")
        try:
            rs_cfg                  = get_rs_settings(db, user_id)
            score_map, rs_tv_data   = fetch_rs_universe(rs_cfg)
        except Exception as exc:
            logger.error("RS universe fetch failed (non-fatal): %s", exc)

        _phase(f"RS universe fetched ({len(score_map)} symbols). Running RS Momentum screener…")
        try:
            rs_rows = run_rs_screener(
                db, mode=mode, user_id=user_id, account_value=av,
                score_map=score_map or None,
                tv_data=rs_tv_data or None,
            )
        except Exception as exc:
            logger.error("RS screener failed (non-fatal): %s", exc)
    else:
        _phase(f"Pullback done — {len(pb_rows)} candidates. RS screener disabled.")

    _phase(f"RS done — {len(rs_rows)} candidates. Merging and re-ranking by RS score…")

    # Merge: Minervini wins on overlap, then Pullback, then RS
    seen: dict[str, dict] = {}
    for r in min_rows:
        seen[r["symbol"]] = r
    for r in pb_rows:
        if r["symbol"] in seen:
            seen[r["symbol"]]["screener_type"] = "both"
        else:
            seen[r["symbol"]] = r
    for r in rs_rows:
        if r["symbol"] not in seen:
            seen[r["symbol"]] = r

    # Re-rank all picks globally by RS score so highest-momentum stocks buy first
    merged = sorted(
        seen.values(),
        key=lambda r: score_map.get(r["symbol"], -999.0),
        reverse=True,
    )

    total_picks = len(merged)
    for i, row in enumerate(merged, 1):
        row["rank"] = i
        if row.get("screener_type") == "rs_momentum":
            # Score = percentile within this plan (rank 1 of N = 99th, last = ~0th)
            row["score"] = int((1 - (i - 1) / total_picks) * 99) if total_picks > 0 else 50

    week_start = merged[0]["week_start"] if merged else _next_monday().isoformat()
    _save_plan(db, merged, week_start, mode, user_id)

    logger.info(
        "All screeners done: %d minervini + %d pullback + %d rs = %d unique (mode=%s)",
        len(min_rows), len(pb_rows), len(rs_rows), len(merged), mode,
    )
    return merged