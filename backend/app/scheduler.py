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

        day_setting  = int(get_setting(db, "screener_schedule_day",  "6"))   # 0=Mon…6=Sun
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

        # Prevent double-run within the same minute
        run_key = now_et.strftime("%Y-%m-%d %H:%M")
        if get_setting(db, "screener_last_auto_run", "") == run_key:
            return
        set_setting(db, "screener_last_auto_run", run_key)
        set_setting(db, "screener_status", "running")
        set_setting(db, "screener_error",  "")
        db.commit()

    finally:
        db.close()

    # Run screener in a fresh session (can take 5–30s)
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
    # Hourly monitor Mon-Fri during market hours (9:30–16:00 ET)
    scheduler.add_job(
        _monitor_job,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="30,0", timezone="America/New_York"),
        id="sepa_monitor",
        replace_existing=True,
    )
    # Watchdog checks every minute whether it's time to run the screener.
    # Schedule day/time are read from DB settings so changes take effect immediately.
    scheduler.add_job(
        _screener_watchdog,
        CronTrigger(minute="*"),
        id="screener_watchdog",
        replace_existing=True,
    )
    scheduler.start()


def stop_scheduler():
    scheduler.shutdown(wait=False)
