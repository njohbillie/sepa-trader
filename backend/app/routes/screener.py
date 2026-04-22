import traceback
import logging

from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..database import get_db, SessionLocal, get_setting, set_setting, get_current_user, get_user_setting, set_user_setting

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
    uid = current_user["id"]
    return {
        "status":           get_user_setting(db, "screener_status",   "idle", uid),
        "error":            get_user_setting(db, "screener_error",    "",     uid),
        "last_run_summary": get_user_setting(db, "screener_last_run", "",     uid),
        "count":            int(get_user_setting(db, "screener_count", "0",   uid) or "0"),
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
    mode = get_user_setting(db, "trading_mode", "paper", uid)
    set_user_setting(db, "screener_status", "running", uid)
    set_user_setting(db, "screener_error",  "",        uid)

    def _run():
        db2 = SessionLocal()
        try:
            from ..screener import run_both_screeners
            results = run_both_screeners(db2, user_id=uid)
            set_user_setting(db2, "screener_status", "done",           uid)
            set_user_setting(db2, "screener_count",  str(len(results)), uid)
        except Exception as exc:
            err_msg = f"{exc}\n{traceback.format_exc()}"
            log.error("Combined screener failed:\n%s", err_msg)
            db3 = SessionLocal()
            try:
                set_user_setting(db3, "screener_status", "error",        uid)
                set_user_setting(db3, "screener_error",  str(exc)[:500], uid)
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
    mode = get_user_setting(db, "trading_mode", "paper", uid)
    set_user_setting(db, "screener_status", "running", uid)
    set_user_setting(db, "screener_error",  "",        uid)

    def _run():
        db2 = SessionLocal()
        try:
            from ..screener import run_screener
            results = run_screener(db2, user_id=uid)
            set_user_setting(db2, "screener_status", "done",           uid)
            set_user_setting(db2, "screener_count",  str(len(results)), uid)
        except Exception as exc:
            db3 = SessionLocal()
            try:
                set_user_setting(db3, "screener_status", "error",        uid)
                set_user_setting(db3, "screener_error",  str(exc)[:500], uid)
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
    mode = get_user_setting(db, "trading_mode", "paper", uid)
    set_user_setting(db, "screener_status", "running", uid)
    set_user_setting(db, "screener_error",  "",        uid)

    def _run():
        db2 = SessionLocal()
        try:
            from ..pullback_screener import run_pullback_screener
            results = run_pullback_screener(db2, user_id=uid)
            set_user_setting(db2, "screener_status", "done",           uid)
            set_user_setting(db2, "screener_count",  str(len(results)), uid)
        except Exception as exc:
            db3 = SessionLocal()
            try:
                set_user_setting(db3, "screener_status", "error",        uid)
                set_user_setting(db3, "screener_error",  str(exc)[:500], uid)
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


@router.post("/sync-tradingview")
def sync_tradingview(
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    uid     = current_user["id"]
    tv_user = get_user_setting(db, "tv_username", "", uid)
    tv_pass = get_user_setting(db, "tv_password", "", uid)
    if not tv_user or not tv_pass:
        from fastapi import HTTPException
        raise HTTPException(400, "TradingView credentials not configured in Settings.")

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
        from fastapi import HTTPException
        raise HTTPException(404, "No weekly plan found for current mode. Run the screener first.")

    def _sync():
        from ..tradingview_client import update_weekly_picks
        result = update_weekly_picks(tv_user, tv_pass, symbols)
        if result["ok"]:
            log.info("TV sync: weekly_picks %s (%d symbols).", result["action"], result["count"])
        else:
            log.error("TV sync failed: %s", result["error"])

    background_tasks.add_task(_sync)
    return {"status": "sync_started", "symbols": symbols, "mode": mode,
            "message": f"Syncing {len(symbols)} symbols to TradingView weekly_picks."}


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
        # 1 — Plain-text analysis for the log (existing behaviour)
        text_analysis = analyze_picks(db, picks, user_id=uid)
        log_analysis(db, "manual", None, text_analysis, mode, user_id=uid)

        # 2 — Structured per-stock analysis
        structured = analyze_picks_structured(db, picks, tape_context=tape_ctx, user_id=uid)

        # 3 — Persist per-stock JSON back into weekly_plan.ai_analysis
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