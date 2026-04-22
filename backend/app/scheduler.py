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


async def _dm_monthly_watchdog():
    """
    Runs daily at 4:30 PM ET on weekdays.
    Fires the Dual Momentum evaluation for every user with is_active=true
    once per month — on whichever weekday falls on or after the configured
    dm_eval_day (1–28, default 1). Stores last-run year-month per user to
    prevent double-firing.
    """
    now_et = datetime.now(_ET)

    db = SessionLocal()
    try:
        from sqlalchemy import text as _text
        from .database import get_user_setting, set_user_setting

        # Find all users with DM active
        rows = db.execute(_text("""
            SELECT sc.user_id, sc.auto_execute,
                   COALESCE(us.value, '1') AS eval_day
            FROM strategy_config sc
            LEFT JOIN user_settings us
                ON us.user_id = sc.user_id AND us.key = 'dm_eval_day'
            WHERE sc.strategy_name = 'dual_momentum'
              AND sc.is_active = true
        """)).fetchall()

        if not rows:
            return

        for user_id, auto_execute, eval_day_str in rows:
            try:
                eval_day = max(1, min(28, int(eval_day_str or "1")))
            except ValueError:
                eval_day = 1

            # Only fire on or after eval_day in the month (first weekday ≥ eval_day)
            if now_et.day < eval_day:
                continue

            # Already ran this month?
            run_key = now_et.strftime("%Y-%m")
            last_run = get_user_setting(db, "dm_last_eval_month", "", user_id)
            if last_run == run_key:
                continue

            # Mark as run before executing to prevent parallel duplicates
            set_user_setting(db, "dm_last_eval_month", run_key, user_id)
            db.commit()

            logger.info("DM monthly eval: firing for user %d (%s)", user_id, run_key)

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

                signal     = dm_evaluate()
                market_env = env_assess()
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
                _save_signal(db, user_id, STRATEGY_DM, signal, ai_decision, mode)
                logger.info("DM monthly eval: user %d → %s [%s]",
                            user_id, ai_decision.get("decision"), signal.get("recommended_symbol"))

                if auto_execute and ai_decision.get("decision") == "EXECUTE":
                    _execute_signal_bg(user_id, STRATEGY_DM, signal["recommended_symbol"], mode)

            except Exception as exc:
                logger.error("DM monthly eval failed for user %d: %s", user_id, exc)

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

    # DM monthly evaluation — fires daily at 4:30 PM ET on weekdays,
    # executes for each active user once per month on/after their eval_day setting
    scheduler.add_job(
        _dm_monthly_watchdog,
        CronTrigger(day_of_week="mon-fri", hour=16, minute=30, timezone="America/New_York"),
        id="dm_monthly",
        replace_existing=True,
    )

    scheduler.start()


def stop_scheduler():
    scheduler.shutdown(wait=False)