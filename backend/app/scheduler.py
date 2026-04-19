import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .database import SessionLocal
from .trader import run_monitor

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()


async def _monitor_job():
    db = SessionLocal()
    try:
        await run_monitor(db)
    finally:
        db.close()


async def _screener_job():
    db = SessionLocal()
    try:
        from .screener import run_screener
        logger.info("Sunday screener starting...")
        plan = run_screener(db)
        logger.info("Sunday screener done. %d stocks selected.", len(plan))
    except Exception as exc:
        logger.error("Sunday screener failed: %s", exc)
    finally:
        db.close()


def start_scheduler():
    # Hourly monitor Mon-Fri during market hours (9:30–16:00 ET)
    scheduler.add_job(
        _monitor_job,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="30,0", timezone="America/New_York"),
        id="sepa_monitor",
        replace_existing=True,
    )
    # Sunday screener at 8:00 PM ET (after markets have settled)
    scheduler.add_job(
        _screener_job,
        CronTrigger(day_of_week="sun", hour=20, minute=0, timezone="America/New_York"),
        id="sunday_screener",
        replace_existing=True,
    )
    scheduler.start()


def stop_scheduler():
    scheduler.shutdown(wait=False)
