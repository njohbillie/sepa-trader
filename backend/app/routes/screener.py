import traceback
import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db, SessionLocal, get_setting, set_setting, get_current_user, get_user_setting, set_user_setting, get_all_user_settings

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/screener", tags=["screener"])


@router.get("/weekly-plan")
def get_weekly_plan(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """Return the current week's plan for the active trading mode and user."""
    mode = get_user_setting(db, "trading_mode", "paper", current_user["id"])
    uid  = current_user["id"]
    import json as _json
    rows = db.execute(
        text("""
            SELECT week_start, symbol, rank, score, signal,
                   entry_price, stop_price, target1, target2,
                   position_size, risk_amount, rationale, status, mode, created_at,
                   COALESCE(screener_type, 'minervini') AS screener_type,
                   ai_analysis
            FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
            )
              AND mode = :mode AND user_id = :uid
            ORDER BY rank ASC
        """),
        {"mode": mode, "uid": uid},
    ).fetchall()
    result = []
    for r in rows:
        row = dict(r._mapping)
        # Deserialise ai_analysis if stored as a string
        if isinstance(row.get("ai_analysis"), str):
            try:
                row["ai_analysis"] = _json.loads(row["ai_analysis"])
            except Exception:
                row["ai_analysis"] = None
        result.append(row)
    return result


@router.get("/status")
def get_screener_status(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    uid    = current_user["id"]
    status = get_user_setting(db, "screener_status",    "idle", uid)
    started_at_str = get_user_setting(db, "screener_started_at", "", uid)

    # Stale-run guard: if status stuck at "running" for > 5 minutes, auto-reset
    elapsed_s = None
    if status == "running" and started_at_str:
        try:
            started_ts = float(started_at_str)
            elapsed_s  = int(time.time() - started_ts)
            if elapsed_s > 300:
                set_user_setting(db, "screener_status", "error", uid)
                set_user_setting(db, "screener_error",
                    f"Timed out after {elapsed_s}s — check server logs for details.", uid)
                db.commit()
                status = "error"
        except (ValueError, TypeError):
            pass

    return {
        "status":           status,
        "phase":            get_user_setting(db, "screener_phase",    "",     uid),
        "error":            get_user_setting(db, "screener_error",    "",     uid),
        "last_run_summary": get_user_setting(db, "screener_last_run", "",     uid),
        "count":            int(get_user_setting(db, "screener_count", "0",   uid) or "0"),
        "started_at":       started_at_str,
        "elapsed_s":        elapsed_s,
    }


@router.get("/dd")
def get_weekly_dd(refresh: bool = False, current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Return DD for the current week's plan (mode-scoped, user-scoped).
    Cached in dd_cache for 7 days — DD data is mode-agnostic.
    """
    import json as _json

    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)
    rows = db.execute(
        text("""
            SELECT symbol FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
            )
              AND mode = :mode AND user_id = :uid
            ORDER BY rank ASC
        """),
        {"mode": mode, "uid": uid},
    ).fetchall()
    symbols = [r[0] for r in rows]
    if not symbols:
        return []

    cache_map: dict = {}
    if not refresh:
        cached = db.execute(
            text("""
                SELECT symbol, data FROM dd_cache
                WHERE symbol = ANY(:syms)
                  AND fetched_at > NOW() - INTERVAL '7 days'
            """),
            {"syms": symbols},
        ).fetchall()
        cache_map = {r[0]: _json.loads(r[1]) for r in cached}

        if len(cache_map) == len(symbols):
            return [cache_map[s] for s in symbols]

    missing = [s for s in symbols if s not in cache_map]
    from ..dd_fetcher import fetch_dd_batch
    from ..claude_analyst import generate_analyst_summary
    fresh = fetch_dd_batch(missing)

    # Enrich with AI analyst summary (best-effort, non-blocking)
    for item in fresh:
        if not item.get("error"):
            try:
                item["analyst_summary"] = generate_analyst_summary(
                    db, item["symbol"], item, user_id=uid
                )
            except Exception:
                item["analyst_summary"] = ""

    for item in fresh:
        if not item.get("error"):
            db.execute(
                text("""
                    INSERT INTO dd_cache (symbol, data)
                    VALUES (:sym, :data)
                    ON CONFLICT (symbol) DO UPDATE
                      SET data = EXCLUDED.data, fetched_at = NOW()
                """),
                {"sym": item["symbol"], "data": _json.dumps(item)},
            )
    db.commit()

    fresh_map = {f["symbol"]: f for f in fresh}
    return [cache_map.get(s) or fresh_map.get(s, {"symbol": s, "error": "not found"})
            for s in symbols]


@router.get("/history")
def get_plan_history(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """Plan history scoped to current trading mode and user."""
    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)
    rows = db.execute(
        text("""
            SELECT DISTINCT week_start, COUNT(*) as cnt
            FROM weekly_plan
            WHERE mode = :mode AND user_id = :uid
            GROUP BY week_start
            ORDER BY week_start DESC
            LIMIT 12
        """),
        {"mode": mode, "uid": uid},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@router.get("/weekly-plan/{week_start}")
def get_plan_for_week(
    week_start: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)
    rows = db.execute(
        text("""
            SELECT week_start, symbol, rank, score, signal,
                   entry_price, stop_price, target1, target2,
                   position_size, risk_amount, rationale, status, mode, created_at
            FROM weekly_plan
            WHERE week_start = :w
              AND mode = :mode AND user_id = :uid
            ORDER BY rank ASC
        """),
        {"w": week_start, "mode": mode, "uid": uid},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@router.post("/run")
def trigger_screener(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Start BOTH screeners (Minervini + Pullback) in background."""
    uid  = current_user["id"]
    mode = get_all_user_settings(db, uid).get("trading_mode", "paper")
    log.info("Screener /run triggered: uid=%s mode=%s (from merged settings)", uid, mode)
    set_user_setting(db, "screener_status",     "running",            uid)
    set_user_setting(db, "screener_error",      "",                   uid)
    set_user_setting(db, "screener_phase",      "Starting…",          uid)
    set_user_setting(db, "screener_started_at", str(time.time()),     uid)
    db.commit()

    def _run():
        db2 = SessionLocal()
        try:
            def _phase(msg):
                set_user_setting(db2, "screener_phase", msg, uid)
                db2.commit()

            from ..screener import run_both_screeners
            _phase("Fetching TradingView data…")
            results = run_both_screeners(db2, mode=mode, user_id=uid, _phase_cb=_phase)
            set_user_setting(db2, "screener_status", "done",                         uid)
            set_user_setting(db2, "screener_phase",  f"Done — {len(results)} stocks selected", uid)
            set_user_setting(db2, "screener_count",  str(len(results)),               uid)
            db2.commit()
        except Exception as exc:
            err_msg = f"{exc}\n{traceback.format_exc()}"
            log.error("Combined screener failed:\n%s", err_msg)
            db3 = SessionLocal()
            try:
                set_user_setting(db3, "screener_status", "error",                uid)
                set_user_setting(db3, "screener_phase",  "Failed",               uid)
                set_user_setting(db3, "screener_error",  str(exc)[:500],         uid)
                db3.commit()
            finally:
                db3.close()
        finally:
            db2.close()

    background_tasks.add_task(_run)
    return {"status": "running", "mode": mode}


@router.post("/run-minervini")
def trigger_minervini_screener(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Run only the Minervini (SEPA) screener in background."""
    uid  = current_user["id"]
    mode = get_all_user_settings(db, uid).get("trading_mode", "paper")
    log.info("Screener /run-minervini triggered: uid=%s mode=%s (from merged settings)", uid, mode)
    set_user_setting(db, "screener_status",     "running",        uid)
    set_user_setting(db, "screener_error",      "",               uid)
    set_user_setting(db, "screener_phase",      "Starting…",      uid)
    set_user_setting(db, "screener_started_at", str(time.time()), uid)
    db.commit()

    def _run():
        db2 = SessionLocal()
        try:
            def _phase(msg):
                set_user_setting(db2, "screener_phase", msg, uid)
                db2.commit()

            from ..screener import run_screener
            _phase("Minervini: fetching TradingView data…")
            results = run_screener(db2, mode=mode, user_id=uid)
            set_user_setting(db2, "screener_status", "done",                         uid)
            set_user_setting(db2, "screener_phase",  f"Done — {len(results)} stocks selected", uid)
            set_user_setting(db2, "screener_count",  str(len(results)),               uid)
            db2.commit()
        except Exception as exc:
            err_msg = f"{exc}\n{traceback.format_exc()}"
            log.error("Minervini screener failed:\n%s", err_msg)
            db3 = SessionLocal()
            try:
                set_user_setting(db3, "screener_status", "error",        uid)
                set_user_setting(db3, "screener_phase",  "Failed",       uid)
                set_user_setting(db3, "screener_error",  str(exc)[:500], uid)
                db3.commit()
            finally:
                db3.close()
        finally:
            db2.close()

    background_tasks.add_task(_run)
    return {"status": "running", "screener": "minervini", "mode": mode}


@router.post("/run-pullback")
def trigger_pullback_screener(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Run only the Pullback-to-MA screener in background."""
    uid  = current_user["id"]
    mode = get_all_user_settings(db, uid).get("trading_mode", "paper")
    log.info("Screener /run-pullback triggered: uid=%s mode=%s (from merged settings)", uid, mode)
    set_user_setting(db, "screener_status",     "running",        uid)
    set_user_setting(db, "screener_error",      "",               uid)
    set_user_setting(db, "screener_phase",      "Starting…",      uid)
    set_user_setting(db, "screener_started_at", str(time.time()), uid)
    db.commit()

    def _run():
        db2 = SessionLocal()
        try:
            def _phase(msg):
                set_user_setting(db2, "screener_phase", msg, uid)
                db2.commit()

            from ..pullback_screener import run_pullback_screener
            from ..screener import _save_plan, _next_monday

            _phase("Pullback: fetching TradingView data…")
            pb_rows = run_pullback_screener(db2, mode=mode, user_id=uid)
            _phase(f"Pullback done — {len(pb_rows)} candidates. Merging with existing plan…")

            # Determine week_start from PB results or fall back to next Monday
            week_start = pb_rows[0]["week_start"] if pb_rows else _next_monday().isoformat()

            # Load existing non-pullback rows for this week so we keep Minervini entries
            existing = db2.execute(
                text("""
                    SELECT week_start, symbol, rank, score, signal,
                           entry_price, stop_price, target1, target2,
                           position_size, risk_amount, rationale, status,
                           mode, screener_type
                    FROM weekly_plan
                    WHERE week_start = :w AND mode = :m
                      AND user_id IS NOT DISTINCT FROM :uid
                      AND COALESCE(screener_type, 'minervini') != 'pullback'
                """),
                {"w": week_start, "m": mode, "uid": uid},
            ).fetchall()

            # Merge: existing Minervini rows first, then PB rows (dedup by symbol)
            seen: dict = {}
            for r in existing:
                seen[r.symbol] = dict(r._mapping)
            for r in pb_rows:
                sym = r["symbol"]
                if sym in seen:
                    seen[sym]["screener_type"] = "both"   # overlap → tag as both
                else:
                    seen[sym] = r

            merged = list(seen.values())
            for i, row in enumerate(merged, 1):
                row["rank"] = i

            _save_plan(db2, merged, week_start, mode, uid)

            set_user_setting(db2, "screener_status", "done",                                uid)
            set_user_setting(db2, "screener_phase",  f"Done — {len(merged)} stocks selected", uid)
            set_user_setting(db2, "screener_count",  str(len(merged)),                       uid)
            db2.commit()
        except Exception as exc:
            err_msg = f"{exc}\n{traceback.format_exc()}"
            log.error("Pullback screener failed:\n%s", err_msg)
            db3 = SessionLocal()
            try:
                set_user_setting(db3, "screener_status", "error",        uid)
                set_user_setting(db3, "screener_phase",  "Failed",       uid)
                set_user_setting(db3, "screener_error",  str(exc)[:500], uid)
                db3.commit()
            finally:
                db3.close()
        finally:
            db2.close()

    background_tasks.add_task(_run)
    return {"status": "running", "screener": "pullback", "mode": mode}


@router.get("/pullback-settings")
def get_pullback_settings(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return current pullback screener settings for this user."""
    from ..pullback_screener import get_pb_settings
    return get_pb_settings(db, current_user["id"])


@router.get("/tv-screeners")
def list_tv_screeners(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Authenticate with TradingView using the user's stored credentials
    and return their saved screeners as [{"id": ..., "name": ...}].
    Returns an empty list (not an error) when no credentials are configured.
    """
    from fastapi import HTTPException
    from ..tradingview_client import list_saved_screeners

    uid      = current_user["id"]
    tv_user  = get_user_setting(db, "tv_username", "", uid)
    tv_pass  = get_user_setting(db, "tv_password", "", uid)

    if not tv_user or not tv_pass:
        return {
            "screeners": [],
            "count": 0,
            "message": "TradingView credentials not configured — add them in Settings → Integrations.",
        }

    screeners = list_saved_screeners(tv_user, tv_pass)
    return {"screeners": screeners, "count": len(screeners)}


@router.get("/watchlist-export")
def watchlist_export(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Return the current weekly plan as a plain-text watchlist file that
    TradingView can import directly.

    Import in TradingView:
      Watchlists panel → ⋮ (three dots) → Import list from file → select the .txt
    """
    from fastapi import HTTPException
    from fastapi.responses import Response

    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)

    rows = db.execute(
        text("""
            SELECT symbol FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
            )
              AND mode = :mode AND user_id = :uid
            ORDER BY rank ASC
        """),
        {"mode": mode, "uid": uid},
    ).fetchall()

    if not rows:
        raise HTTPException(404, "No weekly plan found — run the screener first.")

    from ..tradingview_client import to_tv_symbol
    symbols  = [r[0] for r in rows]
    tv_lines = "\n".join(to_tv_symbol(s) for s in symbols)

    return Response(
        content=tv_lines,
        media_type="text/plain",
        headers={"Content-Disposition": 'attachment; filename="weekly_picks.txt"'},
    )


@router.post("/sync-tradingview")
def sync_tradingview(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Legacy endpoint — kept so existing frontend calls don't 404.
    Returns the symbol list and import instructions.
    TV's watchlist REST API was removed in 2024; use /watchlist-export instead.
    """
    from fastapi import HTTPException

    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)

    rows = db.execute(
        text("""
            SELECT symbol FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
            )
              AND mode = :mode AND user_id = :uid
            ORDER BY rank ASC
        """),
        {"mode": mode, "uid": uid},
    ).fetchall()

    if not rows:
        raise HTTPException(404, "No weekly plan found — run the screener first.")

    from ..tradingview_client import to_tv_symbol
    symbols    = [r[0] for r in rows]
    tv_symbols = [to_tv_symbol(s) for s in symbols]

    return {
        "status":     "ready",
        "symbols":    tv_symbols,
        "mode":       mode,
        "message":    (
            f"{len(symbols)} symbols ready. "
            "Click 'Download .txt' to save the watchlist file, then import it in "
            "TradingView: Watchlists → ⋮ → Import list from file."
        ),
    }


@router.get("/analysis")
def get_analyses(limit: int = 20, current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """Return recent Claude AI analyses for the active mode and user."""
    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)
    from ..claude_analyst import get_latest_analyses
    return get_latest_analyses(db, limit=limit, mode=mode, user_id=uid)


@router.post("/analysis/run")
def trigger_analysis(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Run structured per-stock AI analysis + plain-text log for the current week's plan.
    Stores per-stock decision in weekly_plan.ai_analysis.
    """
    import json as _json
    from ..claude_analyst import analyze_picks, analyze_picks_structured, log_analysis
    from ..market_analysis import get_tape_check
    from fastapi import HTTPException

    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)

    picks_rows = db.execute(
        text("""
            SELECT symbol, score, signal, entry_price, stop_price, target1, target2,
                   status, rationale, COALESCE(screener_type, 'minervini') AS screener_type
            FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
            )
              AND mode = :mode AND user_id = :uid
            ORDER BY rank ASC
        """),
        {"mode": mode, "uid": uid},
    ).fetchall()
    if not picks_rows:
        raise HTTPException(404, "No weekly plan found for current mode.")

    picks = [dict(r._mapping) for r in picks_rows]

    # Fetch today's tape context (cached — no extra LLM call)
    try:
        tape_ctx = get_tape_check(db, user_id=uid)
    except Exception:
        tape_ctx = None

    try:
        # Single AI call — structured result is the source of truth.
        # The plain-text log is derived from it so card and log always agree.
        structured = analyze_picks_structured(db, picks, tape_context=tape_ctx, user_id=uid, mode=mode)

        # Build log text from the same structured result (no second AI call)
        log_lines = []
        for i, item in enumerate(structured, 1):
            decision     = item.get("decision",      "?")
            rationale    = item.get("rationale",     "")
            entry_zone   = item.get("entry_zone",    "")
            exit_strat   = item.get("exit_strategy", "")
            guardrails   = item.get("guardrails",    "")
            line = f"{i}. **{item['symbol']}** — {decision}: {rationale}"
            if entry_zone:
                line += f"\n   Entry: {entry_zone}"
            if exit_strat:
                line += f"\n   Exit: {exit_strat}"
            if guardrails:
                line += f"\n   Guardrails: {guardrails}"
            log_lines.append(line)
        text_analysis = "\n".join(log_lines) if log_lines else "(no pending picks)"
        log_analysis(db, "manual", None, text_analysis, mode, user_id=uid)

        # Persist per-stock JSON back into weekly_plan.ai_analysis
        week_start = db.execute(
            text("SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid"),
            {"mode": mode, "uid": uid},
        ).scalar()

        for item in structured:
            db.execute(
                text("""
                    UPDATE weekly_plan
                    SET ai_analysis = CAST(:ai AS jsonb)
                    WHERE symbol = :sym
                      AND mode  = :mode
                      AND user_id = :uid
                      AND week_start = :ws
                """),
                {
                    "ai":   _json.dumps(item),
                    "sym":  item["symbol"],
                    "mode": mode,
                    "uid":  uid,
                    "ws":   week_start,
                },
            )
        db.commit()

        return {"analysis": text_analysis, "structured": structured}

    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.patch("/weekly-plan/{symbol}/status")
def update_plan_status(
    symbol: str,
    body: dict,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    status = body.get("status", "PENDING")
    if status not in ("PENDING", "EXECUTED", "PARTIAL", "SKIPPED"):
        from fastapi import HTTPException
        raise HTTPException(400, "Invalid status")
    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)
    db.execute(
        text("""
            UPDATE weekly_plan SET status = :s
            WHERE symbol = :sym
              AND mode = :mode
              AND user_id = :uid
              AND week_start = (
                  SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
              )
        """),
        {"s": status, "sym": symbol.upper(), "mode": mode, "uid": uid},
    )
    db.commit()
    return {"symbol": symbol, "status": status, "mode": mode}


@router.get("/news")
def get_weekly_news(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Fetch Alpaca news headlines for every symbol in the current week's plan.
    Returns {symbol: [{headline, source, url, published_at}, ...]} — up to 5 per symbol.
    """
    from ..claude_analyst import _fetch_alpaca_news
    import httpx

    uid  = current_user["id"]
    mode = get_user_setting(db, "trading_mode", "paper", uid)

    rows = db.execute(
        text("""
            SELECT symbol FROM weekly_plan
            WHERE week_start = (
                SELECT MAX(week_start) FROM weekly_plan WHERE mode = :mode AND user_id = :uid
            )
              AND mode = :mode AND user_id = :uid
            ORDER BY rank ASC
        """),
        {"mode": mode, "uid": uid},
    ).fetchall()
    symbols = [r[0] for r in rows]
    if not symbols:
        return {}

    # Re-use the same fetcher but grab richer article data here
    from ..config import settings as _cfg
    from ..database import get_user_setting as _gus
    from sqlalchemy import text as _text
    from datetime import datetime, timezone, timedelta

    try:
        is_admin = db.execute(
            _text("SELECT role FROM users WHERE id = :id"), {"id": uid}
        ).scalar() == "admin"

        if mode == "live":
            key    = _gus(db, "alpaca_live_key",    "", uid) or (_cfg.alpaca_live_key    if is_admin else "")
            secret = _gus(db, "alpaca_live_secret", "", uid) or (_cfg.alpaca_live_secret if is_admin else "")
        else:
            key    = _gus(db, "alpaca_paper_key",    "", uid) or (_cfg.alpaca_paper_key    if is_admin else "")
            secret = _gus(db, "alpaca_paper_secret", "", uid) or (_cfg.alpaca_paper_secret if is_admin else "")

        if not key or not secret:
            return {s: [] for s in symbols}

        start = (datetime.now(timezone.utc) - timedelta(hours=72)).strftime("%Y-%m-%dT%H:%M:%SZ")
        resp  = httpx.get(
            "https://data.alpaca.markets/v1beta1/news",
            params={
                "symbols":          ",".join(symbols),
                "limit":            min(5 * len(symbols), 50),
                "start":            start,
                "sort":             "desc",
                "include_content":  "false",
            },
            headers={"APCA-API-KEY-ID": key.strip(), "APCA-API-SECRET-KEY": secret.strip()},
            timeout=10,
        )
        resp.raise_for_status()

        news_map: dict = {s: [] for s in symbols}
        for article in resp.json().get("news", []):
            for sym in article.get("symbols", []):
                if sym in news_map and len(news_map[sym]) < 5:
                    news_map[sym].append({
                        "headline":     article.get("headline", ""),
                        "source":       article.get("source", ""),
                        "url":          article.get("url", ""),
                        "published_at": article.get("created_at", ""),
                    })
        return news_map

    except Exception as exc:
        log.warning("get_weekly_news: failed (%s)", exc)
        return {s: [] for s in symbols}