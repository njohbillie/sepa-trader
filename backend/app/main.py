import logging
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from .config import settings
from .database import SessionLocal
from .scheduler import start_scheduler, stop_scheduler
from .routes import account, positions, orders, signals, settings as settings_route, webhook, screener, market as market_route

logger = logging.getLogger(__name__)


def _run_migrations():
    db = SessionLocal()
    try:
        # ── Trading tables ────────────────────────────────────────────────────
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
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS ai_analysis_log (
                id         SERIAL PRIMARY KEY,
                trigger    VARCHAR(30) NOT NULL,
                symbol     VARCHAR(10),
                analysis   TEXT NOT NULL,
                mode       VARCHAR(10) NOT NULL DEFAULT 'paper',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS dd_cache (
                symbol     VARCHAR(10) PRIMARY KEY,
                data       TEXT NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))

        # ── Auth tables ───────────────────────────────────────────────────────
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id            SERIAL PRIMARY KEY,
                email         VARCHAR(255) UNIQUE NOT NULL,
                username      VARCHAR(100) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role          VARCHAR(20) NOT NULL DEFAULT 'user',
                is_active     BOOLEAN NOT NULL DEFAULT true,
                totp_secret   VARCHAR(64),
                totp_enabled  BOOLEAN NOT NULL DEFAULT false,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_login    TIMESTAMPTZ
            )
        """))
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS user_settings (
                id      SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                key     VARCHAR(100) NOT NULL,
                value   TEXT NOT NULL,
                UNIQUE (user_id, key)
            )
        """))

        # ── Strategy tables ───────────────────────────────────────────────────
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS strategy_config (
                id                  SERIAL PRIMARY KEY,
                user_id             INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                strategy_name       VARCHAR(50) NOT NULL,
                is_active           BOOLEAN NOT NULL DEFAULT false,
                auto_execute        BOOLEAN NOT NULL DEFAULT false,
                trading_mode        VARCHAR(10) NOT NULL DEFAULT 'paper',
                alpaca_paper_key    TEXT,
                alpaca_paper_secret TEXT,
                alpaca_live_key     TEXT,
                alpaca_live_secret  TEXT,
                settings            JSONB NOT NULL DEFAULT '{}',
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, strategy_name)
            )
        """))
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS strategy_signals (
                id                  SERIAL PRIMARY KEY,
                user_id             INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                strategy_name       VARCHAR(50) NOT NULL,
                recommended_symbol  VARCHAR(20),
                current_symbol      VARCHAR(20),
                action              VARCHAR(20),
                data                JSONB,
                reasoning           TEXT,
                ai_verdict          VARCHAR(20),
                ai_reasoning        TEXT,
                mode                VARCHAR(10) NOT NULL DEFAULT 'paper',
                executed            BOOLEAN NOT NULL DEFAULT false,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))

        # ── Add user_id FK to trading tables (idempotent) ────────────────────
        for table in ("weekly_plan", "trade_log", "signal_log", "ai_analysis_log"):
            db.execute(text(f"""
                ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id)
            """))

        # ── Reset screener_top_n to auto (0) if still at old hardcoded default ─
        db.execute(text("""
            UPDATE settings SET value = '0'
            WHERE key = 'screener_top_n' AND value = '10'
        """))

        # ── Add 2FA columns to users table (idempotent) ──────────────────────
        db.execute(text("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS totp_secret  VARCHAR(64),
            ADD COLUMN IF NOT EXISTS totp_enabled BOOLEAN NOT NULL DEFAULT false
        """))

        # ── Add screener_type to weekly_plan (phase-2) ────────────────────────
        db.execute(text("""
            ALTER TABLE weekly_plan
            ADD COLUMN IF NOT EXISTS screener_type VARCHAR(20) DEFAULT 'minervini'
        """))

        # ── Market tape cache (phase-3) ───────────────────────────────────────
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS market_tape_cache (
                id           SERIAL PRIMARY KEY,
                user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                cache_date   DATE NOT NULL,
                signals      JSONB NOT NULL DEFAULT '{}',
                verdict      VARCHAR(20) NOT NULL DEFAULT 'caution',
                summary      TEXT NOT NULL DEFAULT '',
                key_risk     TEXT NOT NULL DEFAULT '',
                refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (user_id, cache_date)
            )
        """))

        # ── Seed global settings (defaults) ──────────────────────────────────
        db.execute(text("""
            INSERT INTO settings (key, value) VALUES
                ('screener_auto_run',          'true'),
                ('screener_schedule_day',       '6'),
                ('screener_schedule_time',      '20:00'),
                ('screener_price_min',          '0'),
                ('screener_price_max',          '0'),
                ('screener_top_n',              '0'),
                ('screener_min_score',          '0'),
                ('screener_vol_surge_pct',      '40'),
                ('screener_ema20_pct',          '2.0'),
                ('screener_ema50_pct',          '3.0'),
                ('ai_provider',                 'anthropic'),
                ('ai_model',                    ''),
                ('positions_snapshot_paper',    ''),
                ('positions_snapshot_live',     ''),
                ('pb_price_min',                '10'),
                ('pb_price_max',                '200'),
                ('pb_ema_alignment',            'true'),
                ('pb_price_above_ema20',        'true'),
                ('pb_rsi_min',                  '40'),
                ('pb_rsi_max',                  '60'),
                ('pb_avg_vol_min',              '1000000'),
                ('pb_rel_vol_min',              '0.75'),
                ('pb_market_cap_min',           '500000000'),
                ('pb_week_change_min',          '-3'),
                ('pb_ema50_proximity',          '8'),
                ('pb_beta_max',                 '2.5'),
                ('pb_earnings_days_min',        '15'),
                ('pb_ppst_required',            'true'),
                ('pb_top_n',                    '5')
            ON CONFLICT (key) DO NOTHING
        """))

        db.commit()

        # ── Bootstrap admin user ──────────────────────────────────────────────
        _bootstrap_admin(db)

    finally:
        db.close()


def _bootstrap_admin(db):
    """
    Create the initial admin user if none exists. Assigns all existing data to them.
    If ADMIN_PASSWORD is explicitly set in .env, always updates the password on startup —
    useful for password resets without DB access.
    """
    from .auth import hash_password

    admin_email    = getattr(settings, "admin_email",    "admin@sepa.local")
    admin_password = getattr(settings, "admin_password", "")

    existing = db.execute(
        text("SELECT id FROM users WHERE role = 'admin' ORDER BY id LIMIT 1")
    ).fetchone()

    if existing:
        admin_id = existing[0]
        # If a password is explicitly set in .env, apply it (password reset mechanism)
        if admin_password:
            db.execute(
                text("UPDATE users SET password_hash = :pw WHERE id = :id"),
                {"pw": hash_password(admin_password), "id": admin_id},
            )
            db.commit()
            logger.info("Admin password updated from ADMIN_PASSWORD env var.")
    else:
        if not admin_password:
            admin_password = secrets.token_urlsafe(12)
            logger.warning("=" * 60)
            logger.warning("ADMIN ACCOUNT CREATED")
            logger.warning("  Email:    %s", admin_email)
            logger.warning("  Password: %s", admin_password)
            logger.warning("Change this immediately in the Admin panel.")
            logger.warning("=" * 60)

        row = db.execute(
            text("""
                INSERT INTO users (email, username, password_hash, role)
                VALUES (:email, 'admin', :pw, 'admin')
                ON CONFLICT (email) DO UPDATE SET role = 'admin', password_hash = :pw
                RETURNING id
            """),
            {"email": admin_email, "pw": hash_password(admin_password)},
        ).fetchone()
        db.commit()
        admin_id = row[0]

    # Assign any existing data rows (pre-auth migration) to the admin
    for table in ("weekly_plan", "trade_log", "signal_log", "ai_analysis_log"):
        db.execute(
            text(f"UPDATE {table} SET user_id = :uid WHERE user_id IS NULL"),
            {"uid": admin_id},
        )
    db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _run_migrations()
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(title="BAMETTA", lifespan=lifespan)

_origins = getattr(settings, "allowed_origins", "").split(",") if getattr(settings, "allowed_origins", "") else [
    "http://localhost",
    "http://localhost:5173",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from .routes import auth as auth_route, admin as admin_route, strategies as strategies_route

app.include_router(auth_route.router)
app.include_router(admin_route.router)
app.include_router(account.router)
app.include_router(positions.router)
app.include_router(orders.router)
app.include_router(signals.router)
app.include_router(settings_route.router)
app.include_router(webhook.router)
app.include_router(screener.router)
app.include_router(strategies_route.router)
app.include_router(market_route.router)


@app.get("/health")
def health():
    return {"status": "ok"}
