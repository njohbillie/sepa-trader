"""
Symmetric encryption for sensitive credentials stored in the database.
Uses Fernet (AES-128-CBC + HMAC-SHA256) derived from SECRET_KEY.
Backward-compatible: plain-text values that fail decryption are returned as-is.
"""
import base64
import hashlib
from cryptography.fernet import Fernet, InvalidToken
from .config import settings

_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        raw = settings.secret_key.encode("utf-8")
        key = base64.urlsafe_b64encode(hashlib.sha256(raw).digest())
        _fernet = Fernet(key)
    return _fernet


def encrypt(value: str) -> str:
    """Encrypt a plain-text string. Returns Fernet token string."""
    if not value:
        return value
    return _get_fernet().encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt(value: str) -> str:
    """
    Decrypt a Fernet token.  If the value is plain-text (pre-encryption rows)
    it is returned as-is so existing data keeps working transparently.
    """
    if not value:
        return value
    try:
        return _get_fernet().decrypt(value.encode("utf-8")).decode("utf-8")
    except (InvalidToken, Exception):
        return value
