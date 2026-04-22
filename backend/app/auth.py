"""
JWT utilities, password hashing, and TOTP for two-factor authentication.
"""
from datetime import datetime, timedelta, timezone

import bcrypt as _bcrypt
import pyotp
from jose import jwt, JWTError

from .config import settings

_ALGO                        = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES  = 60   # 1 hour — short enough to be safe, long enough to be usable
REFRESH_TOKEN_EXPIRE_DAYS    = 30  # 30 days sliding window
TWO_FA_TOKEN_EXPIRE_MINUTES  = 5


# ── Passwords ─────────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode("utf-8"), _bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return _bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


# ── Tokens ────────────────────────────────────────────────────────────────────

def _make_token(data: dict, expires_delta: timedelta) -> str:
    payload = {**data, "exp": datetime.now(timezone.utc) + expires_delta}
    return jwt.encode(payload, settings.secret_key, algorithm=_ALGO)


def create_access_token(user_id: int, role: str) -> str:
    return _make_token(
        {"sub": str(user_id), "role": role, "type": "access"},
        timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )


def create_refresh_token(user_id: int) -> str:
    return _make_token(
        {"sub": str(user_id), "type": "refresh"},
        timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
    )


def create_2fa_token(user_id: int) -> str:
    """Short-lived token issued after password check when 2FA is still pending."""
    return _make_token(
        {"sub": str(user_id), "type": "2fa_pending"},
        timedelta(minutes=TWO_FA_TOKEN_EXPIRE_MINUTES),
    )


def decode_token(token: str) -> dict:
    """Decode and validate a JWT. Raises JWTError on any failure."""
    return jwt.decode(token, settings.secret_key, algorithms=[_ALGO])


# ── TOTP ──────────────────────────────────────────────────────────────────────

def generate_totp_secret() -> str:
    return pyotp.random_base32()


def get_totp_uri(secret: str, email: str) -> str:
    return pyotp.TOTP(secret).provisioning_uri(
        name=email,
        issuer_name="BAMETTA",
    )


def verify_totp(secret: str, code: str) -> bool:
    return pyotp.TOTP(secret).verify(code, valid_window=1)
