from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from .database import SessionLocal
from .scheduler import start_scheduler, stop_scheduler
from .routes import account, positions, orders, signals, settings, webhook, screener


def _run_migrations():
    db = SessionLocal()
    try:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS weekly_plan (
                id            SERIAL PRIMARY KEY,
                week_start    DATE NOT NULL,
                symbol        VARCHAR(10) NOT NULL,
                rank          INTEGER NOT NULL,
                score         INTEGER NOT NULL,
                signal        VARCHAR(30),
                entry_price   NUMERIC(12,4),
                stop_price    NUMERIC(12,4),
                target1       NUMERIC(12,4),
                target2       NUMERIC(12,4),
                position_size INTEGER,
                risk_amount   NUMERIC(12,2),
                rationale     TEXT,
                status        VARCHAR(20) DEFAULT 'PENDING',
                mode          VARCHAR(10) NOT NULL DEFAULT 'paper',
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        # Seed any new settings keys (idempotent — DO NOTHING on conflict)
        db.execute(text("""
            INSERT INTO settings (key, value) VALUES
                ('screener_auto_run',      'true'),
                ('screener_schedule_day',  '6'),
                ('screener_schedule_time', '20:00'),
                ('screener_price_min',     '0'),
                ('screener_price_max',     '0'),
                ('screener_top_n',         '10'),
                ('screener_min_score',     '0'),
                ('screener_vol_surge_pct', '40'),
                ('screener_ema20_pct',     '2.0'),
                ('screener_ema50_pct',     '3.0')
            ON CONFLICT (key) DO NOTHING
        """))
        db.commit()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _run_migrations()
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(title="SEPA Trader", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(account.router)
app.include_router(positions.router)
app.include_router(orders.router)
app.include_router(signals.router)
app.include_router(settings.router)
app.include_router(webhook.router)
app.include_router(screener.router)


@app.get("/health")
def health():
    return {"status": "ok"}
