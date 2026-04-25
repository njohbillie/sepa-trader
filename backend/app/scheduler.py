import asyncio
import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from sqlalchemy import text
from .database import SessionLocal, get_setting, set_setting
from .trader import run_monitor

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()

_ET = pytz.timezone("America/New_York")


async def _run_monitor_for_mode(admin_uid: int | None, mode: str) -> None:
    """
    Runs the full monitor cycle (trailing stops → exit guard → slot fill) for
    one specific mode.  Uses an independent DB session so paper and live can
    run concurrently without sharing a connection.
    """
    db = SessionLocal()
    try:
        result = await run_monitor(db, user_id=admin_uid, mode=mode)
        if isinstance(result, dict) and result.get("status") == "error":
            logger.error("Watchdog [%s]: monitor error — %s", mode, result.get("error"))
        else:
            logger.info(
                "Watchdog [%s]: monitor done — portfolio=$%.0f positions=%d",
                mode,
                result.get("portfolio", 0),
                len(result.get("results", [])),
            )
        from .position_manager import check_post_close
        check_post_close(db, mode=mode)
    except Exception as exc:
        logger.error("Watchdog [%s]: monitor failed — %s", mode, exc, exc_info=True)
    finally:
        db.close()


async def _monitor_watchdog():
    """
    Runs every minute.  Fires the monitor for BOTH paper AND live modes when:
      1. monitor_enabled == "true"
      2. It's a weekday between 9:00–16:00 ET
      3. At least monitor_interval_minutes have elapsed since the last run

    Paper and live monitors run concurrently so a live account NEVER misses a
    trade because the operator is viewing/testing the paper account at the time.

    Each mode's auto_execute is controlled independently:
      • paper_auto_execute (default true)
      • live_auto_execute  (default false — must be explicitly enabled)
    """
    db = SessionLocal()
    admin_uid = None
    try:
        admin_row = db.execute(
            text("SELECT id FROM users WHERE role = 'admin' ORDER BY id LIMIT 1")
        ).fetchone()
        admin_uid = admin_row[0] if admin_row else None

        from .database import get_all_user_settings as _gaus
        merged = _gaus(db, admin_uid) if admin_uid else {}

        if merged.get("monitor_enabled", "true") != "true":
            logger.debug("Watchdog: monitor_enabled=false — skipping.")
            return

        now_et = datetime.now(_ET)

        if now_et.weekday() > 4:
            logger.debug("Watchdog: weekend — skipping.")
            return
        if not (9 <= now_et.hour < 16):
            logger.debug("Watchdog: outside market hours (%s ET) — skipping.", now_et.strftime("%H:%M"))
            return

        interval = int(merged.get("monitor_interval_minutes", "30") or "30")

        last_run_str = get_setting(db, "monitor_last_run", "")
        elapsed_min  = 9999.0
        if last_run_str:
            try:
                last_run = datetime.fromisoformat(last_run_str)
                if last_run.tzinfo is None:
                    last_run = _ET.localize(last_run)
                elapsed_min = (now_et - last_run).total_seconds() / 60
                if elapsed_min < interval:
                    logger.debug(
                        "Watchdog: elapsed=%.1fm < interval=%dm — skipping (both modes).",
                        elapsed_min, interval,
                    )
                    return
            except (ValueError, TypeError):
                pass

        logger.info(
            "Watchdog: FIRING monitor for both modes [interval=%dm elapsed=%.1fm]",
            interval, elapsed_min,
        )

        # Mark run NOW so concurrent ticks don't double-fire
        set_setting(db, "monitor_last_run", now_et.isoformat())
        db.commit()

    finally:
        db.close()

    # Run paper and live monitors in parallel — independent DB sessions
    await asyncio.gather(
        _run_monitor_for_mode(admin_uid, "paper"),
        _run_monitor_for_mode(admin_uid, "live"),
        return_exceptions=True,
    )


async def _monday_open_job():
    """
    Fires once on Monday at 9:35 ET.
    Fills position slots from the weekly plan for BOTH paper and live modes.
    Each mode reads its own auto_execute flag so live is skip-safe by default.
    """
    for mode in ("paper", "live"):
        db = SessionLocal()
        try:
            from .position_manager import run_monday_open
            run_monday_open(db, mode=mode)
        except Exception as exc:
            logger.error("Monday open [%s] failed: %s", mode, exc)
        finally:
            db.close()


async def _screener_watchdog():
    """
    Runs every minute. Fires the screener when all conditions are met:
      1. screener_auto_run == "true"
      2. current ET weekday is in screener_schedule_days (comma-separated 0–6)
         Falls back to legacy screener_schedule_day (single int) if not set.
      3. current ET HH:MM is in screener_schedule_times (comma-separated)
         Falls back to legacy screener_schedule_time if not set.
    Tracks last run per slot (YYYY-MM-DD HH:MM) to prevent double-firing.
    """
    db = SessionLocal()
    try:
        if get_setting(db, "screener_auto_run", "true") != "true":
            return

        now_et = datetime.now(_ET)

        # ── Days: multi (new) → single legacy fallback ────────────────────────
        days_raw = get_setting(db, "screener_schedule_days", "").strip()
        if days_raw:
            try:
                schedule_days = {int(d.strip()) for d in days_raw.split(",") if d.strip().isdigit()}
            except ValueError:
                schedule_days = set()
        else:
            try:
                schedule_days = {int(get_setting(db, "screener_schedule_day", "6"))}
            except ValueError:
                schedule_days = {6}

        if now_et.weekday() not in schedule_days:
            return

        # ── Times: multi (new) → single legacy fallback ───────────────────────
        times_raw = get_setting(db, "screener_schedule_times", "").strip()
        if times_raw:
            schedule_times = {t.strip() for t in times_raw.split(",") if t.strip()}
        else:
            schedule_times = {get_setting(db, "screener_schedule_time", "20:00")}

        current_hm = now_et.strftime("%H:%M")
        if current_hm not in schedule_times:
            return

        run_key = now_et.strftime("%Y-%m-%d %H:%M")
        if get_setting(db, "screener_last_auto_run", "") == run_key:
            return

        set_setting(db, "screener_last_auto_run", run_key)
        set_setting(db, "screener_status", "running")
        set_setting(db, "screener_error",  "")
        db.commit()

    finally:
        db.close()

    db2 = SessionLocal()
    try:
        from .screener import run_screener
        from sqlalchemy import text as _text
        admin_row = db2.execute(
            _text("SELECT id FROM users WHERE role = 'admin' ORDER BY id LIMIT 1")
        ).fetchone()
        admin_id = admin_row[0] if admin_row else None

        logger.info("Scheduled screener starting (%s ET)...", run_key)
        plan = run_screener(db2, user_id=admin_id)
        set_setting(db2, "screener_status", "done")
        set_setting(db2, "screener_count",  str(len(plan)))
        logger.info("Scheduled screener done. %d stocks selected.", len(plan))
    except Exception as exc:
        logger.error("Scheduled screener failed: %s", exc)
        db3 = SessionLocal()
        try:
            set_setting(db3, "screener_status", "error")
            set_setting(db3, "screener_error",  str(exc)[:500])
        finally:
            db3.close()
    finally:
        db2.close()


async def _pb_screener_watchdog():
    """
    Runs every minute. Fires the Pullback screener when all conditions are met:
      1. pb_screener_auto_run == "true"
      2. current ET weekday is in pb_screener_schedule_days (comma-separated 0–6)
      3. current ET HH:MM is in pb_screener_schedule_times (comma-separated)
    Tracks last run per slot (YYYY-MM-DD HH:MM) to prevent double-firing.
    """
    db = SessionLocal()
    try:
        if get_setting(db, "pb_screener_auto_run", "true") != "true":
            return

        now_et = datetime.now(_ET)

        days_raw = get_setting(db, "pb_screener_schedule_days", "").strip()
        if days_raw:
            try:
                schedule_days = {int(d.strip()) for d in days_raw.split(",") if d.strip().isdigit()}
            except ValueError:
                schedule_days = set()
        else:
            return  # no schedule configured — skip

        if now_et.weekday() not in schedule_days:
            return

        times_raw = get_setting(db, "pb_screener_schedule_times", "").strip()
        if not times_raw:
            return  # no times configured — skip

        schedule_times = {t.strip() for t in times_raw.split(",") if t.strip()}
        current_hm = now_et.strftime("%H:%M")
        if current_hm not in schedule_times:
            return

        run_key = now_et.strftime("%Y-%m-%d %H:%M")
        if get_setting(db, "pb_screener_last_auto_run", "") == run_key:
            return

        set_setting(db, "pb_screener_last_auto_run", run_key)
        set_setting(db, "screener_status", "running")
        set_setting(db, "screener_error",  "")
        db.commit()

    finally:
        db.close()

    db2 = SessionLocal()
    try:
        from .pullback_screener import run_pullback_screener
        from sqlalchemy import text as _text
        admin_row = db2.execute(
            _text("SELECT id FROM users WHERE role = 'admin' ORDER BY id LIMIT 1")
        ).fetchone()
        admin_id = admin_row[0] if admin_row else None

        logger.info("Scheduled pullback screener starting (%s ET)…", run_key)
        plan = run_pullback_screener(db2, user_id=admin_id)
        set_setting(db2, "screener_status", "done")
        set_setting(db2, "screener_count",  str(len(plan)))
        logger.info("Scheduled pullback screener done. %d stocks selected.", len(plan))
    except Exception as exc:
        logger.error("Scheduled pullback screener failed: %s", exc)
        db3 = SessionLocal()
        try:
            set_setting(db3, "screener_status", "error")
            set_setting(db3, "screener_error",  str(exc)[:500])
        finally:
            db3.close()
    finally:
        db2.close()


async def _run_screener_for_mode(uid: int, mode: str) -> int:
    """
    Runs all three screeners (Minervini + Pullback + RS) for one mode and
    saves picks to weekly_plan.  Returns the number of picks saved.
    Uses an independent DB session.
    """
    from .screener import run_both_screeners
    from .database import set_user_setting as _sus, get_all_user_settings as _gaus

    db = SessionLocal()
    try:
        results = run_both_screeners(db, mode=mode, user_id=uid)
        _sus(db, f"screener_last_run_{mode}", datetime.now(_ET).isoformat(), uid)
        logger.info(
            "Market-close screener [%s]: %d stocks selected.", mode, len(results)
        )
        return len(results)
    except Exception as exc:
        logger.error("Market-close screener [%s] failed: %s", mode, exc)
        return 0
    finally:
        db.close()


async def _market_close_screener():
    """
    Fires Mon–Fri at 4:05 PM ET.

    Runs all three screeners for BOTH paper AND live modes so picks are ready
    for whichever mode the monitor fires next.  Live screener failure is
    non-fatal — paper picks are always saved even if live credentials are missing.
    """
    db = SessionLocal()
    uid = None
    try:
        from sqlalchemy import text as _text
        admin_row = db.execute(
            _text("SELECT id FROM users WHERE role = 'admin' ORDER BY id LIMIT 1")
        ).fetchone()
        if not admin_row:
            return
        uid = admin_row[0]

        from .database import set_user_setting as _sus
        _sus(db, "screener_status", "running", uid)
        _sus(db, "screener_error",  "",         uid)
        db.commit()
        logger.info("Market-close screener starting (paper + live)…")
    finally:
        db.close()

    # Run paper and live screeners concurrently
    results = await asyncio.gather(
        _run_screener_for_mode(uid, "paper"),
        _run_screener_for_mode(uid, "live"),
        return_exceptions=True,
    )

    paper_count = results[0] if isinstance(results[0], int) else 0
    live_count  = results[1] if isinstance(results[1], int) else 0

    db2 = SessionLocal()
    try:
        from .database import set_user_setting as _sus
        total = paper_count + live_count
        _sus(db2, "screener_status", "done",      uid)
        _sus(db2, "screener_count",  str(total),   uid)
        logger.info(
            "Market-close screener done. paper=%d live=%d picks.",
            paper_count, live_count,
        )
    except Exception as exc:
        logger.error("Market-close screener status update failed: %s", exc)
    finally:
        db2.close()


def _check_circuit_breaker(vix_threshold: float, spy_drawdown_threshold: float) -> tuple[bool, str]:
    """
    Returns (triggered, reason) if VIX or SPY drawdown breach their thresholds.
    Uses the same yf_client as the strategy — no extra dependencies.
    """
    try:
        from .strategies.yf_client import fetch_history
        import pandas as pd

        # ── VIX check ────────────────────────────────────────────────────────
        vix_df = fetch_history("^VIX", period_days=5)
        if not vix_df.empty:
            vix_now = float(vix_df["Close"].iloc[-1])
            if vix_now >= vix_threshold:
                return True, f"VIX {vix_now:.1f} ≥ threshold {vix_threshold:.0f}"

        # ── SPY drawdown from 20-day high ────────────────────────────────────
        spy_df = fetch_history("SPY", period_days=30)
        if not spy_df.empty and len(spy_df) >= 5:
            high_20d   = float(spy_df["Close"].tail(20).max())
            spy_now    = float(spy_df["Close"].iloc[-1])
            drawdown   = (high_20d - spy_now) / high_20d * 100
            if drawdown >= spy_drawdown_threshold:
                return True, f"SPY {drawdown:.1f}% below 20-day high (threshold {spy_drawdown_threshold:.0f}%)"

    except Exception as exc:
        logger.warning("circuit breaker check failed: %s", exc)

    return False, ""


async def _dm_watchdog():
    """
    Runs daily at 4:30 PM ET on weekdays.
    For each user with DM is_active=true, fires an evaluation when either:

    A) Scheduled frequency elapsed:
       - monthly  : on/after eval_day each month  (default)
       - biweekly : every 14 days
       - weekly   : every 7 days

    B) Circuit breaker triggered (any day, regardless of frequency):
       - VIX closes at or above vix_threshold  (default 30)
       - SPY drops dm_spy_drawdown% below its 20-day high  (default 10%)
       Circuit breaker fires at most once per calendar day to avoid spam.
    """
    now_et   = datetime.now(_ET)
    today    = now_et.strftime("%Y-%m-%d")

    db = SessionLocal()
    try:
        from sqlalchemy import text as _text
        from .database import get_user_setting, set_user_setting

        rows = db.execute(_text("""
            SELECT sc.user_id, sc.auto_execute,
                   COALESCE(us_day.value,  '1')        AS eval_day,
                   COALESCE(us_freq.value, 'monthly')  AS frequency,
                   COALESCE(us_vix.value,  '30')       AS vix_threshold,
                   COALESCE(us_dd.value,   '10')       AS spy_drawdown_threshold
            FROM strategy_config sc
            LEFT JOIN user_settings us_day  ON us_day.user_id  = sc.user_id AND us_day.key  = 'dm_eval_day'
            LEFT JOIN user_settings us_freq ON us_freq.user_id = sc.user_id AND us_freq.key = 'dm_eval_frequency'
            LEFT JOIN user_settings us_vix  ON us_vix.user_id  = sc.user_id AND us_vix.key  = 'dm_vix_threshold'
            LEFT JOIN user_settings us_dd   ON us_dd.user_id   = sc.user_id AND us_dd.key   = 'dm_spy_drawdown_threshold'
            WHERE sc.strategy_name = 'dual_momentum'
              AND sc.is_active = true
        """)).fetchall()

        if not rows:
            return

        for user_id, auto_execute, eval_day_str, frequency, vix_thr_str, dd_thr_str in rows:
            try:
                eval_day        = max(1,   min(28,  int(eval_day_str   or "1")))
                vix_threshold   = max(15,  min(80,  float(vix_thr_str  or "30")))
                spy_drawdown    = max(3,   min(30,  float(dd_thr_str   or "10")))
            except ValueError:
                eval_day, vix_threshold, spy_drawdown = 1, 30.0, 10.0

            last_eval_date = get_user_setting(db, "dm_last_eval_date", "", user_id)

            # ── A: Scheduled frequency ────────────────────────────────────────
            scheduled = False
            if frequency == "weekly":
                if last_eval_date:
                    try:
                        from datetime import date
                        days_ago = (now_et.date() - date.fromisoformat(last_eval_date)).days
                        scheduled = days_ago >= 7
                    except ValueError:
                        scheduled = True
                else:
                    scheduled = True
            elif frequency == "biweekly":
                if last_eval_date:
                    try:
                        from datetime import date
                        days_ago = (now_et.date() - date.fromisoformat(last_eval_date)).days
                        scheduled = days_ago >= 14
                    except ValueError:
                        scheduled = True
                else:
                    scheduled = True
            else:  # monthly
                last_month = get_user_setting(db, "dm_last_eval_month", "", user_id)
                this_month = now_et.strftime("%Y-%m")
                scheduled  = (last_month != this_month) and (now_et.day >= eval_day)

            # ── B: Circuit breaker ────────────────────────────────────────────
            cb_triggered, cb_reason = False, ""
            if not scheduled and last_eval_date != today:
                cb_triggered, cb_reason = _check_circuit_breaker(vix_threshold, spy_drawdown)

            if not scheduled and not cb_triggered:
                continue

            reason = cb_reason if cb_triggered else f"{frequency} schedule"

            # Always follow the user's global trading mode
            mode = get_user_setting(db, "trading_mode", "paper", user_id)

            # Bug #6: check dedicated keys BEFORE running evaluation
            from .routes.strategies import _dm_has_dedicated_keys
            if not _dm_has_dedicated_keys(db, user_id, mode):
                logger.warning(
                    "DM watchdog: skipping eval for user %d — no dedicated Alpaca keys for mode=%s",
                    user_id, mode,
                )
                continue

            if cb_triggered:
                logger.warning("DM circuit breaker triggered for user %d: %s", user_id, cb_reason)
                # Bug #3: notify via Telegram on circuit breaker
                try:
                    from . import telegram_alerts as tg
                    await tg.send(
                        f"*DM Circuit Breaker*\n{cb_reason}\n"
                        f"Triggering out-of-schedule evaluation for user {user_id}.",
                        level="URGENT",
                    )
                except Exception as tg_exc:
                    logger.warning("DM circuit breaker Telegram notify failed: %s", tg_exc)
            else:
                logger.info("DM eval triggered for user %d: %s", user_id, reason)

            # Mark run dates before executing to prevent duplicates
            set_user_setting(db, "dm_last_eval_date",  today,                    user_id)
            set_user_setting(db, "dm_last_eval_month", now_et.strftime("%Y-%m"), user_id)
            db.commit()

            try:
                from .strategies.dual_momentum import evaluate as dm_evaluate
                from .strategies.market_env    import assess    as env_assess
                from .strategies.ai_strategist import decide    as ai_decide
                from .routes.strategies        import _save_signal, _execute_signal_bg, STRATEGY_DM

                signal      = dm_evaluate()
                market_env  = env_assess()

                # Fetch portfolio for AI context (best-effort — empty dict if unavailable)
                portfolio: dict = {}
                try:
                    from .routes.strategies import _resolve_strategy_alpaca_client
                    is_admin_row = db.execute(
                        text("SELECT role FROM users WHERE id = :id"), {"id": user_id}
                    ).scalar()
                    client = _resolve_strategy_alpaca_client(
                        db, user_id, STRATEGY_DM, mode, is_admin_row == "admin"
                    )
                    from .utils import sf as _sf
                    for p in client.get_all_positions():
                        portfolio[p.symbol] = {
                            "qty":           _sf(p.qty, 0.0),
                            "unrealized_pl": _sf(p.unrealized_pl, 0.0),
                        }
                except Exception as pf_exc:
                    logger.warning("DM watchdog: portfolio fetch failed: %s", pf_exc)

                ai_decision = ai_decide(
                    db=db,
                    market_env=market_env,
                    strategy_signals=[{
                        "strategy_name":      STRATEGY_DM,
                        "recommended_symbol": signal["recommended_symbol"],
                        "action":             "BUY",
                        "reasoning":          signal["reasoning"],
                    }],
                    portfolio=portfolio,
                    user_id=user_id,
                )

                signal["trigger"] = reason
                _save_signal(db, user_id, STRATEGY_DM, signal, ai_decision, mode)

                logger.info("DM eval complete: user %d → %s [%s] (trigger: %s)",
                            user_id, ai_decision.get("decision"),
                            signal.get("recommended_symbol"), reason)

                if auto_execute and ai_decision.get("decision") == "EXECUTE":
                    _execute_signal_bg(user_id, STRATEGY_DM, signal["recommended_symbol"], mode)

            except Exception as exc:
                logger.error("DM eval failed for user %d: %s", user_id, exc)

    finally:
        db.close()


def start_scheduler():
    # Watchdog fires every minute — interval is controlled by monitor_interval_minutes setting
    scheduler.add_job(
        _monitor_watchdog,
        CronTrigger(minute="*"),
        id="sepa_monitor",
        replace_existing=True,
    )

    # Monday 9:35 AM ET — fill position slots from the weekly plan
    scheduler.add_job(
        _monday_open_job,
        CronTrigger(day_of_week="mon", hour=9, minute=35, timezone="America/New_York"),
        id="monday_open",
        replace_existing=True,
    )

    # Watchdog checks every minute whether it's time to run the Minervini screener
    # (uses screener_schedule_days/times settings)
    scheduler.add_job(
        _screener_watchdog,
        CronTrigger(minute="*"),
        id="screener_watchdog",
        replace_existing=True,
    )

    # Watchdog checks every minute whether it's time to run the Pullback screener
    # (uses pb_screener_schedule_days/times settings)
    scheduler.add_job(
        _pb_screener_watchdog,
        CronTrigger(minute="*"),
        id="pb_screener_watchdog",
        replace_existing=True,
    )

    # Market close: run both screeners (Minervini + Pullback) at 4:05 PM ET Mon–Fri
    scheduler.add_job(
        _market_close_screener,
        CronTrigger(
            day_of_week="mon-fri",
            hour=16,
            minute=5,
            timezone="America/New_York",
        ),
        id="market_close_screener",
        replace_existing=True,
    )

    # DM watchdog — fires daily at 4:30 PM ET on weekdays.
    # Handles scheduled frequency (monthly/biweekly/weekly) + VIX/drawdown circuit breakers.
    scheduler.add_job(
        _dm_watchdog,
        CronTrigger(day_of_week="mon-fri", hour=16, minute=30, timezone="America/New_York"),
        id="dm_watchdog",
        replace_existing=True,
    )

    scheduler.start()


def stop_scheduler():
    scheduler.shutdown(wait=False)