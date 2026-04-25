from fastapi import Cookie, Depends, HTTPException
from jose import JWTError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session

from .config import settings
from .crypto import encrypt as _enc, decrypt as _dec

engine       = create_engine(settings.database_url.replace("postgresql://", "postgresql+psycopg2://"))
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base         = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Global settings (system-level, used by scheduler/background jobs) ─────────

# Sensitive global keys — encrypted at rest, decrypted transparently on read.
_GLOBAL_PRIVATE_KEYS: frozenset[str] = frozenset({
    "webhook_secret",
})


def get_setting(db, key: str, default: str = "") -> str:
    row = db.execute(text("SELECT value FROM settings WHERE key = :k"), {"k": key}).fetchone()
    if not row:
        return default
    val = row[0]
    if key in _GLOBAL_PRIVATE_KEYS and val:
        val = _dec(val)
    return val


def set_setting(db, key: str, value: str):
    stored = _enc(value) if (key in _GLOBAL_PRIVATE_KEYS and value) else value
    db.execute(
        text("INSERT INTO settings (key, value) VALUES (:k, :v) ON CONFLICT (key) DO UPDATE SET value = :v"),
        {"k": key, "v": stored},
    )
    db.commit()


# ── Per-user settings (override global defaults for API-driven operations) ────

# Keys that are strictly private to each user.
# They are NEVER sourced from the global settings table, so a new user always
# sees empty values regardless of what admin has configured globally.
_PRIVATE_KEYS: frozenset[str] = frozenset({
    # AI provider credentials
    "ai_api_key",
    "ai_base_url",
    # Legacy key — kept so old data isn't leaked if still in settings table
    "claude_api_key",
    # Integrations
    "tv_username",
    "tv_password",
    "webhook_secret",
    # Alpaca credentials
    "alpaca_paper_key",
    "alpaca_paper_secret",
    "alpaca_live_key",
    "alpaca_live_secret",
})


def get_user_setting(db, key: str, default: str = "", user_id: int = None) -> str:
    """Return user-specific setting.
    Private keys never fall back to global — they must be set explicitly per user.
    Sensitive values are decrypted transparently before being returned.
    """
    if user_id:
        row = db.execute(
            text("SELECT value FROM user_settings WHERE key = :k AND user_id = :uid"),
            {"k": key, "uid": user_id},
        ).fetchone()
        if row:
            val = row[0]
            if key in _PRIVATE_KEYS and val:
                val = _dec(val)
            return val
    # Private keys have no global fallback — return the supplied default
    if key in _PRIVATE_KEYS:
        return default
    return get_setting(db, key, default)


def set_user_setting(db, key: str, value: str, user_id: int):
    # Strip whitespace from credential keys so copy-paste artefacts don't cause 401s
    if key in _PRIVATE_KEYS and isinstance(value, str):
        value = value.strip()
    # Encrypt sensitive keys at rest
    stored = _enc(value) if (key in _PRIVATE_KEYS and value) else value
    db.execute(
        text("""
            INSERT INTO user_settings (user_id, key, value) VALUES (:uid, :k, :v)
            ON CONFLICT (user_id, key) DO UPDATE SET value = :v
        """),
        {"uid": user_id, "k": key, "v": stored},
    )
    db.commit()


def get_all_user_settings(db, user_id: int) -> dict:
    """Return merged dict: global defaults overlaid with user overrides.
    Private/sensitive keys are excluded from the global layer — they must
    be configured explicitly per user and are blank for new accounts.
    Sensitive values are decrypted transparently before being returned.
    """
    global_rows = db.execute(text("SELECT key, value FROM settings")).fetchall()
    merged: dict[str, str] = {}
    for k, v in global_rows:
        if k in _PRIVATE_KEYS:
            continue  # never bleed from global into any user's view
        if k in _GLOBAL_PRIVATE_KEYS and v:
            v = _dec(v)
        merged[k] = v

    user_rows = db.execute(
        text("SELECT key, value FROM user_settings WHERE user_id = :uid"),
        {"uid": user_id},
    ).fetchall()
    for k, v in user_rows:
        if k in _PRIVATE_KEYS and v:
            v = _dec(v)
        merged[k] = v

    return merged


# Public re-export so other modules (e.g. routes/settings.py masking) can
# discover which keys must never be returned in plain text.
PRIVATE_KEYS: frozenset[str] = _PRIVATE_KEYS


# ── Auth dependencies ─────────────────────────────────────────────────────────

def get_current_user(
    access_token: str | None = Cookie(None),
    db: Session = Depends(get_db),
) -> dict:
    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        from .auth import decode_token
        payload = decode_token(access_token)
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token type")
        user_id = int(payload["sub"])
    except (JWTError, KeyError, ValueError):
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    row = db.execute(
        text("SELECT id, email, username, role, is_active, totp_enabled FROM users WHERE id = :id"),
        {"id": user_id},
    ).fetchone()

    if not row or not row[4]:
        raise HTTPException(status_code=401, detail="User not found or inactive")

    return {"id": row[0], "email": row[1], "username": row[2], "role": row[3], "totp_enabled": bool(row[5])}


def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


# ── Account tier helpers ──────────────────────────────────────────────────────

def get_live_account_limits(portfolio_value: float) -> dict:
    """
    Graduated position sizing constraints for live accounts.
    Called automatically on every screener run and monitor cycle.
    Paper accounts never call this — they always use settings as-is.
    """
    if portfolio_value < 10_000:
        return {
            "tier":               "MICRO (<$10K)",
            "max_positions":      3,
            "screener_top_n":     3,
            "screener_price_min": 15.0,
            "screener_price_max": 200.0,
            "min_score_floor":    5,
            "max_position_pct":   35.0,
            "min_cash_pct":       5.0,
            "max_risk_pct":       2.5,
        }

    if portfolio_value < 25_000:
        return {
            "tier":               "SMALL ($10K–$25K)",
            "max_positions":      5,
            "screener_top_n":     5,
            "screener_price_min": 10.0,
            "screener_price_max": 500.0,
            "min_score_floor":    4,
            "max_position_pct":   25.0,
            "min_cash_pct":       8.0,
            "max_risk_pct":       2.0,
        }

    if portfolio_value < 50_000:
        return {
            "tier":               "STANDARD ($25K–$50K)",
            "max_positions":      7,
            "screener_top_n":     7,
            "screener_price_min": 0.0,
            "screener_price_max": 0.0,
            "min_score_floor":    4,
            "max_position_pct":   20.0,
            "min_cash_pct":       10.0,
            "max_risk_pct":       2.0,
        }

    return {
        "tier":               "FULL ($50K+)",
        "max_positions":      None,
        "screener_top_n":     None,
        "screener_price_min": None,
        "screener_price_max": None,
        "min_score_floor":    0,
        "max_position_pct":   20.0,
        "min_cash_pct":       10.0,
        "max_risk_pct":       2.0,
    }
