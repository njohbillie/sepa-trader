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
