from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .database import SessionLocal
from .trader import run_monitor

scheduler = AsyncIOScheduler()


async def _monitor_job():
    db = SessionLocal()
    try:
        await run_monitor(db)
    finally:
        db.close()


def start_scheduler():
    # Run every hour Mon-Fri during market hours (9:30–16:00 ET)
    scheduler.add_job(
        _monitor_job,
        CronTrigger(day_of_week="mon-fri", hour="9-15", minute="30,0", timezone="America/New_York"),
        id="sepa_monitor",
        replace_existing=True,
    )
    scheduler.start()


def stop_scheduler():
    scheduler.shutdown(wait=False)
