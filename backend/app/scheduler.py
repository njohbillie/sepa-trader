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
        logger.info("Scheduled screener starting (%s ET)...", run_key)
        plan = run_screener(db2)
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

    scheduler.start()


def stop_scheduler():
    scheduler.shutdown(wait=False)