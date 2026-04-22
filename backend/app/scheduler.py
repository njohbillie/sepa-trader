import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .database import SessionLocal, get_setting, set_setting
from .trader import run_monitor

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()

_ET = pytz.timezone("America/New_York")


async def _monitor_job():
    db = SessionLocal()
    try:
        if get_setting(db, "monitor_enabled", "true") != "true":
            return
        await run_monitor(db)
        from .position_manager import check_post_close
        check_post_close(db)
    finally:
        db.close()


async def _monday_open_job():
    """Fires once on Monday at 9:35 ET to fill position slots from the weekly plan."""
    db = SessionLocal()
    try:
        from .position_manager import run_monday_open
        run_monday_open(db)
    finally:
        db.close()


async def _screener_watchdog():
    """
    Runs every minute. Fires the screener when all three conditions are met:
      1. screener_auto_run == "true"
      2. current ET weekday matches screener_schedule_day (0=Mon … 6=Sun)
      3. current ET HH:MM matches screener_schedule_time
    Tracks last run date to prevent double-firing within the same minute.
    """
    db = SessionLocal()
    try:
        if get_setting(db, "screener_auto_run", "true") != "true":
            return

        day_setting  = int(get_setting(db, "screener_schedule_day",  "6"))
        time_setting = get_setting(db, "screener_schedule_time", "20:00")

        now_et = datetime.now(_ET)
        if now_et.weekday() != day_setting:
            return

        try:
            h, m = map(int, time_setting.split(":"))
        except ValueError:
            return
        if now_et.hour != h or now_et.minute != m:
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
            if cb_triggered:
                logger.warning("DM circuit breaker triggered for user %d: %s", user_id, cb_reason)
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

                cfg = db.execute(_text(
                    "SELECT trading_mode FROM strategy_config "
                    "WHERE user_id=:uid AND strategy_name=:name"
                ), {"uid": user_id, "name": STRATEGY_DM}).fetchone()
                mode = cfg[0] if cfg else "paper"

                signal      = dm_evaluate()
                market_env  = env_assess()
                ai_decision = ai_decide(
                    db=db,
                    market_env=market_env,
                    strategy_signals=[{
                        "strategy_name":      STRATEGY_DM,
                        "recommended_symbol": signal["recommended_symbol"],
                        "action":             "BUY",
                        "reasoning":          signal["reasoning"],
                    }],
                    portfolio={},
                    user_id=user_id,
                )

                # Tag the signal with what triggered it
                signal["trigger"] = reason
                _save_signal(db, user_id, STRATEGY_DM, signal, ai_decision, mode)

                logger.info("DM eval complete: user %d → %s [%s] (trigger: %s)",
                            user_id, ai_decision.get("decision"),
                            signal.get("recommended_symbol"), reason)

                if auto_execute and ai_decision.get("decision") == "EXECUTE":
                    from .routes.strategies import _dm_has_dedicated_keys
                    if _dm_has_dedicated_keys(db, user_id, mode):
                        _execute_signal_bg(user_id, STRATEGY_DM, signal["recommended_symbol"], mode)
                    else:
                        logger.warning(
                            "DM auto-execute blocked for user %d: no dedicated Alpaca keys set for mode=%s",
                            user_id, mode,
                        )

            except Exception as exc:
                logger.error("DM eval failed for user %d: %s", user_id, exc)

    finally:
        db.close()


def start_scheduler():
    # Every 30 minutes Mon-Fri during market hours (9:00–15:30 ET)
    # Fires at :00 and :30 of each hour — covers 9:30 open through 15:30
    scheduler.add_job(
        _monitor_job,
        CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/30",
            timezone="America/New_York",
        ),
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

    # Watchdog checks every minute whether it's time to run the screener
    scheduler.add_job(
        _screener_watchdog,
        CronTrigger(minute="*"),
        id="screener_watchdog",
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