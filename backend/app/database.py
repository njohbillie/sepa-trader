from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from .config import settings

engine = create_engine(settings.database_url.replace("postgresql://", "postgresql+psycopg2://"))
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_setting(db, key: str, default: str = "") -> str:
    row = db.execute(text("SELECT value FROM settings WHERE key = :k"), {"k": key}).fetchone()
    return row[0] if row else default


def set_setting(db, key: str, value: str):
    db.execute(
        text("INSERT INTO settings (key, value) VALUES (:k, :v) ON CONFLICT (key) DO UPDATE SET value = :v"),
        {"k": key, "v": value},
    )
    db.commit()


def get_live_account_limits(portfolio_value: float) -> dict:
    """
    Graduated position sizing constraints for live accounts.
    Called automatically on every screener run and monitor cycle.
    Paper accounts never call this — they always use settings as-is.

    Tiers:
      < $10K  — micro    (3 positions, tight price/score filters)
      $10-25K — small    (5 positions, moderate filters)
      $25-50K — standard (7 positions, light filters)
      $50K+   — full     (uses max_positions setting, no overrides)

    As the account grows and crosses a tier boundary, limits unlock
    automatically on the next screener run or monitor cycle — no
    manual settings changes required.
    """
    if portfolio_value < 10_000:
        return {
            "tier":               "MICRO (<$10K)",
            "max_positions":      3,
            "screener_top_n":     3,
            "screener_price_min": 15.0,
            "screener_price_max": 200.0,
            "min_score_floor":    5,
            "max_position_pct":   35.0,   # each position ~25-33% on a $5K account
            "min_cash_pct":       5.0,    # keep 5% cash buffer
            "max_risk_pct":       2.5,    # slight tolerance for small account math
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
            "screener_price_min": 0.0,   # no price floor needed
            "screener_price_max": 0.0,   # no price ceiling needed
            "min_score_floor":    4,
            "max_position_pct":   20.0,
            "min_cash_pct":       10.0,
            "max_risk_pct":       2.0,
        }

    # $50K+ — full account, use settings values without structural override
    return {
        "tier":               "FULL ($50K+)",
        "max_positions":      None,   # None = use settings value unchanged
        "screener_top_n":     None,
        "screener_price_min": None,
        "screener_price_max": None,
        "min_score_floor":    0,      # 0 = adaptive (no forced floor)
        "max_position_pct":   20.0,
        "min_cash_pct":       10.0,
        "max_risk_pct":       2.0,
    }